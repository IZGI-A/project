"""
Redis-backed storage for the simulated external bank.
Data is keyed by (tenant_id, loan_type, file_type).
Uses Redis so all gunicorn workers share the same data.
"""
import json
import os

import redis

_PREFIX = "extbank:"

_redis = redis.Redis(
    host=os.environ.get('REDIS_HOST', 'localhost'),
    port=int(os.environ.get('REDIS_PORT', '6379')),
    db=1,
    decode_responses=True,
)


def _key(tenant_id, loan_type, file_type):
    return f"{_PREFIX}{tenant_id}:{loan_type}:{file_type}"


def store_data(tenant_id, loan_type, file_type, records):
    """Store (replace) records for a given tenant/loan_type/file_type."""
    _redis.set(_key(tenant_id, loan_type, file_type), json.dumps(records))


def get_data(tenant_id, loan_type, file_type):
    """Retrieve records for a given tenant/loan_type/file_type."""
    raw = _redis.get(_key(tenant_id, loan_type, file_type))
    if raw is None:
        return []
    return json.loads(raw)


def get_row_count(tenant_id, loan_type, file_type):
    """Get the number of stored records."""
    raw = _redis.get(_key(tenant_id, loan_type, file_type))
    if raw is None:
        return 0
    return len(json.loads(raw))


def clear_data(tenant_id=None, loan_type=None, file_type=None):
    """Clear specific or all stored data."""
    if tenant_id and loan_type and file_type:
        _redis.delete(_key(tenant_id, loan_type, file_type))
    elif tenant_id:
        pattern = f"{_PREFIX}{tenant_id}:*"
        keys = _redis.keys(pattern)
        if keys:
            _redis.delete(*keys)
    else:
        pattern = f"{_PREFIX}*"
        keys = _redis.keys(pattern)
        if keys:
            _redis.delete(*keys)


def list_keys():
    """List all stored data keys."""
    keys = _redis.keys(f"{_PREFIX}*")
    return [k.replace(_PREFIX, '') for k in keys]
