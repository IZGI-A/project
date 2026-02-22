"""
Redis-backed storage for the simulated external bank.

Two key namespaces:
  extbank:        - uploaded data waiting to be synced
  extbank_failed: - records that failed during sync
"""
import json
import os

import redis

_PREFIX = "extbank:"
_FAILED_PREFIX = "extbank_failed:"

_redis = redis.Redis(
    host=os.environ.get('REDIS_HOST', 'localhost'),
    port=int(os.environ.get('REDIS_PORT', '6379')),
    db=1,
    decode_responses=True,
)


def _key(tenant_id, loan_type, file_type):
    return f"{_PREFIX}{tenant_id}:{loan_type}:{file_type}"


def _failed_key(tenant_id, loan_type, file_type):
    return f"{_FAILED_PREFIX}{tenant_id}:{loan_type}:{file_type}"


# ── Upload data (extbank:) ──────────────────────────────────

def store_data(tenant_id, loan_type, file_type, records):
    """Store (replace) uploaded records."""
    _redis.set(_key(tenant_id, loan_type, file_type), json.dumps(records))


def get_data(tenant_id, loan_type, file_type):
    """Retrieve uploaded records."""
    raw = _redis.get(_key(tenant_id, loan_type, file_type))
    if raw is None:
        return []
    return json.loads(raw)


def get_row_count(tenant_id, loan_type, file_type):
    """Get the number of uploaded records."""
    raw = _redis.get(_key(tenant_id, loan_type, file_type))
    if raw is None:
        return 0
    return len(json.loads(raw))


def clear_data(tenant_id=None, loan_type=None, file_type=None):
    """Clear uploaded data."""
    if tenant_id and loan_type and file_type:
        _redis.delete(_key(tenant_id, loan_type, file_type))
    elif tenant_id:
        for k in _redis.keys(f"{_PREFIX}{tenant_id}:*"):
            _redis.delete(k)
    else:
        for k in _redis.keys(f"{_PREFIX}*"):
            _redis.delete(k)


# ── Failed records (extbank_failed:) ────────────────────────

def store_failed(tenant_id, loan_type, file_type, records):
    """Append records to existing failed records."""
    existing = get_failed(tenant_id, loan_type, file_type)
    existing.extend(records)
    _redis.set(_failed_key(tenant_id, loan_type, file_type), json.dumps(existing))


def get_failed(tenant_id, loan_type, file_type):
    """Retrieve failed records."""
    raw = _redis.get(_failed_key(tenant_id, loan_type, file_type))
    if raw is None:
        return []
    return json.loads(raw)


def get_failed_row_count(tenant_id, loan_type, file_type):
    """Get the number of failed records."""
    raw = _redis.get(_failed_key(tenant_id, loan_type, file_type))
    if raw is None:
        return 0
    return len(json.loads(raw))


def clear_failed(tenant_id=None, loan_type=None, file_type=None):
    """Clear failed records."""
    if tenant_id and loan_type and file_type:
        _redis.delete(_failed_key(tenant_id, loan_type, file_type))
    elif tenant_id:
        for k in _redis.keys(f"{_FAILED_PREFIX}{tenant_id}:*"):
            _redis.delete(k)
    else:
        for k in _redis.keys(f"{_FAILED_PREFIX}*"):
            _redis.delete(k)


def list_keys():
    """List all stored data keys."""
    keys = _redis.keys(f"{_PREFIX}*")
    return [k.replace(_PREFIX, '') for k in keys]
