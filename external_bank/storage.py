"""
Redis-backed storage for the simulated external bank.

Two key namespaces:
  extbank:        — uploaded data waiting to be synced (gzip + JSON blobs)
  extbank_failed: — records that failed during sync   (Redis Lists)

Memory-efficiency measures:
  - gzip compression for upload data (~50-60% size reduction)
  - Redis Lists for failed records (atomic RPUSH, no read-modify-write)
  - Separate counter keys for O(1) row counts
  - TTL on all keys to prevent memory leaks
  - SCAN instead of KEYS for non-blocking pattern matching
"""
import gzip
import json
import os

import redis

_PREFIX = "extbank:"
_FAILED_PREFIX = "extbank_failed:"
_TTL_UPLOAD = 60 * 60 * 24   # 24 hours
_TTL_FAILED = 60 * 60 * 72   # 72 hours

_redis = redis.Redis(
    host=os.environ.get('REDIS_HOST', 'localhost'),
    port=int(os.environ.get('REDIS_PORT', '6379')),
    db=1,
    decode_responses=False,
)


def _key(tenant_id, loan_type, file_type):
    return f"{_PREFIX}{tenant_id}:{loan_type}:{file_type}"


def _failed_key(tenant_id, loan_type, file_type):
    return f"{_FAILED_PREFIX}{tenant_id}:{loan_type}:{file_type}"


def _count_key(tenant_id, loan_type, file_type):
    return f"{_PREFIX}{tenant_id}:{loan_type}:{file_type}:count"


# ── Compression helpers ─────────────────────────────────────

def _compress(data):
    return gzip.compress(json.dumps(data).encode('utf-8'))


def _decompress(raw):
    if raw is None:
        return None
    return json.loads(gzip.decompress(raw).decode('utf-8'))


# ── Upload data (extbank:) ──────────────────────────────────

def store_data(tenant_id, loan_type, file_type, records):
    """Store (replace) uploaded records with gzip compression."""
    key = _key(tenant_id, loan_type, file_type)
    cnt_key = _count_key(tenant_id, loan_type, file_type)
    pipe = _redis.pipeline()
    pipe.set(key, _compress(records), ex=_TTL_UPLOAD)
    pipe.set(cnt_key, len(records), ex=_TTL_UPLOAD)
    pipe.execute()


def get_data(tenant_id, loan_type, file_type):
    """Retrieve uploaded records."""
    data = _decompress(_redis.get(_key(tenant_id, loan_type, file_type)))
    return data if data is not None else []


def get_row_count(tenant_id, loan_type, file_type):
    """O(1) row count — no deserialization needed."""
    val = _redis.get(_count_key(tenant_id, loan_type, file_type))
    return int(val) if val else 0


def clear_data(tenant_id=None, loan_type=None, file_type=None):
    """Clear uploaded data (uses SCAN, non-blocking)."""
    if tenant_id and loan_type and file_type:
        _redis.delete(
            _key(tenant_id, loan_type, file_type),
            _count_key(tenant_id, loan_type, file_type),
        )
    elif tenant_id:
        _scan_delete(f"{_PREFIX}{tenant_id}:*")
    else:
        _scan_delete(f"{_PREFIX}*")


# ── Failed records (extbank_failed:) ────────────────────────

def store_failed(tenant_id, loan_type, file_type, records):
    """Append failed records atomically via Redis List (RPUSH)."""
    if not records:
        return
    key = _failed_key(tenant_id, loan_type, file_type)
    pipe = _redis.pipeline()
    for record in records:
        pipe.rpush(key, json.dumps(record).encode('utf-8'))
    pipe.expire(key, _TTL_FAILED)
    pipe.execute()


def get_failed(tenant_id, loan_type, file_type):
    """Retrieve all failed records from Redis List."""
    raw_list = _redis.lrange(
        _failed_key(tenant_id, loan_type, file_type), 0, -1,
    )
    return [json.loads(item.decode('utf-8')) for item in raw_list]


def get_failed_row_count(tenant_id, loan_type, file_type):
    """O(1) count via LLEN — no deserialization needed."""
    return _redis.llen(_failed_key(tenant_id, loan_type, file_type))


def clear_failed(tenant_id=None, loan_type=None, file_type=None):
    """Clear failed records (uses SCAN, non-blocking)."""
    if tenant_id and loan_type and file_type:
        _redis.delete(_failed_key(tenant_id, loan_type, file_type))
    elif tenant_id:
        _scan_delete(f"{_FAILED_PREFIX}{tenant_id}:*")
    else:
        _scan_delete(f"{_FAILED_PREFIX}*")


# ── Utilities ───────────────────────────────────────────────

def list_keys():
    """List all stored data keys (uses SCAN, non-blocking)."""
    keys = []
    for k in _redis.scan_iter(match=f"{_PREFIX}*", count=100):
        decoded = k.decode('utf-8')
        if decoded.endswith(':count'):
            continue
        keys.append(decoded.replace(_PREFIX, ''))
    return keys


def _scan_delete(pattern):
    """Delete keys matching pattern using SCAN (non-blocking)."""
    for k in _redis.scan_iter(match=pattern, count=100):
        _redis.delete(k)
