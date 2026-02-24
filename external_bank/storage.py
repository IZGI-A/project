"""
Redis-backed storage for the simulated external bank.

Two key namespaces:
  extbank:        — uploaded data waiting to be synced (gzip + JSON blobs)
  extbank_failed: — records that failed during sync   (Redis Lists)

Memory-efficiency measures:
  - CHUNKED storage: data is split into chunks of CHUNK_SIZE rows
  - gzip compression for each chunk (~50-60% size reduction)
  - Redis Lists for failed records (atomic RPUSH, no read-modify-write)
  - Separate counter keys for O(1) row counts
  - TTL on all keys to prevent memory leaks
  - SCAN instead of KEYS for non-blocking pattern matching
"""
import gzip
import json
import logging
import os

import redis

logger = logging.getLogger(__name__)

_PREFIX = "extbank:"
_FAILED_PREFIX = "extbank_failed:"
_TTL_UPLOAD = 60 * 60 * 24   # 24 hours
_TTL_FAILED = 60 * 60 * 72   # 72 hours
CHUNK_SIZE = 50_000           # rows per chunk

_redis = redis.Redis(
    host=os.environ.get('REDIS_HOST', 'localhost'),
    port=int(os.environ.get('REDIS_PORT', '6379')),
    db=1,
    decode_responses=False,
)


def _key(tenant_id, loan_type, file_type):
    return f"{_PREFIX}{tenant_id}:{loan_type}:{file_type}"


def _chunk_key(tenant_id, loan_type, file_type, chunk_idx):
    return f"{_PREFIX}{tenant_id}:{loan_type}:{file_type}:chunk:{chunk_idx}"


def _chunk_count_key(tenant_id, loan_type, file_type):
    return f"{_PREFIX}{tenant_id}:{loan_type}:{file_type}:chunks"


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


# ── Upload data (extbank:) — chunked ─────────────────────────

def store_data(tenant_id, loan_type, file_type, records):
    """Store (replace) uploaded records in chunks with gzip compression."""
    # Clear any previous chunks first
    _clear_chunks(tenant_id, loan_type, file_type)

    total_rows = 0
    chunk_idx = 0
    pipe = _redis.pipeline()

    for i in range(0, len(records), CHUNK_SIZE):
        chunk = records[i:i + CHUNK_SIZE]
        key = _chunk_key(tenant_id, loan_type, file_type, chunk_idx)
        pipe.set(key, _compress(chunk), ex=_TTL_UPLOAD)
        total_rows += len(chunk)
        chunk_idx += 1

    # Store metadata
    cnt_key = _count_key(tenant_id, loan_type, file_type)
    cc_key = _chunk_count_key(tenant_id, loan_type, file_type)
    pipe.set(cnt_key, total_rows, ex=_TTL_UPLOAD)
    pipe.set(cc_key, chunk_idx, ex=_TTL_UPLOAD)
    pipe.execute()

    logger.info(
        "Stored %d rows in %d chunks for %s/%s/%s",
        total_rows, chunk_idx, tenant_id, loan_type, file_type,
    )


def store_data_streaming(tenant_id, loan_type, file_type, row_iterator):
    """
    Store uploaded records from an iterator in chunks.
    Memory efficient — only holds one chunk in memory at a time.
    """
    _clear_chunks(tenant_id, loan_type, file_type)

    total_rows = 0
    chunk_idx = 0
    chunk = []

    for row in row_iterator:
        chunk.append(row)
        if len(chunk) >= CHUNK_SIZE:
            key = _chunk_key(tenant_id, loan_type, file_type, chunk_idx)
            _redis.set(key, _compress(chunk), ex=_TTL_UPLOAD)
            total_rows += len(chunk)
            chunk_idx += 1
            chunk = []

    # Write remaining rows
    if chunk:
        key = _chunk_key(tenant_id, loan_type, file_type, chunk_idx)
        _redis.set(key, _compress(chunk), ex=_TTL_UPLOAD)
        total_rows += len(chunk)
        chunk_idx += 1

    # Store metadata
    pipe = _redis.pipeline()
    pipe.set(_count_key(tenant_id, loan_type, file_type), total_rows, ex=_TTL_UPLOAD)
    pipe.set(_chunk_count_key(tenant_id, loan_type, file_type), chunk_idx, ex=_TTL_UPLOAD)
    pipe.execute()

    logger.info(
        "Streamed %d rows in %d chunks for %s/%s/%s",
        total_rows, chunk_idx, tenant_id, loan_type, file_type,
    )
    return total_rows


def get_data(tenant_id, loan_type, file_type):
    """Retrieve all uploaded records (concatenates all chunks)."""
    num_chunks = _get_num_chunks(tenant_id, loan_type, file_type)
    if num_chunks == 0:
        return []

    all_records = []
    for i in range(num_chunks):
        key = _chunk_key(tenant_id, loan_type, file_type, i)
        raw = _redis.get(key)
        if raw:
            all_records.extend(_decompress(raw))

    return all_records


def get_data_iter(tenant_id, loan_type, file_type):
    """
    Generator that yields chunks of records one at a time.
    Memory efficient — only one chunk in memory at a time.
    """
    num_chunks = _get_num_chunks(tenant_id, loan_type, file_type)
    for i in range(num_chunks):
        key = _chunk_key(tenant_id, loan_type, file_type, i)
        raw = _redis.get(key)
        if raw:
            yield _decompress(raw)


def get_row_count(tenant_id, loan_type, file_type):
    """O(1) row count — no deserialization needed."""
    val = _redis.get(_count_key(tenant_id, loan_type, file_type))
    return int(val) if val else 0


def clear_data(tenant_id=None, loan_type=None, file_type=None):
    """Clear uploaded data (uses SCAN, non-blocking)."""
    if tenant_id and loan_type and file_type:
        _clear_chunks(tenant_id, loan_type, file_type)
        _redis.delete(
            _count_key(tenant_id, loan_type, file_type),
            _chunk_count_key(tenant_id, loan_type, file_type),
        )
    elif tenant_id:
        _scan_delete(f"{_PREFIX}{tenant_id}:*")
    else:
        _scan_delete(f"{_PREFIX}*")


def _clear_chunks(tenant_id, loan_type, file_type):
    """Clear all chunk keys for a given dataset."""
    num_chunks = _get_num_chunks(tenant_id, loan_type, file_type)
    if num_chunks > 0:
        pipe = _redis.pipeline()
        for i in range(num_chunks):
            pipe.delete(_chunk_key(tenant_id, loan_type, file_type, i))
        pipe.execute()


def _get_num_chunks(tenant_id, loan_type, file_type):
    """Get the number of chunks stored for a dataset."""
    val = _redis.get(_chunk_count_key(tenant_id, loan_type, file_type))
    return int(val) if val else 0


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
        if decoded.endswith(':count') or ':chunk:' in decoded or decoded.endswith(':chunks'):
            continue
        keys.append(decoded.replace(_PREFIX, ''))
    return keys


def _scan_delete(pattern):
    """Delete keys matching pattern using SCAN (non-blocking)."""
    for k in _redis.scan_iter(match=pattern, count=100):
        _redis.delete(k)
