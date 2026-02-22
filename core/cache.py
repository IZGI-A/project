"""
Tenant-aware Redis caching utilities.

All cache operations are safe to call even if Redis is unavailable —
failures are logged and the caller falls through to the database.

Cache keys follow the pattern:
    {tenant_id}:{resource}:{discriminator}

The Django CACHES KEY_PREFIX ('findata') and version number are
automatically prepended by django-redis.
"""
import logging
from typing import Any, Callable

from django.core.cache import cache

logger = logging.getLogger(__name__)

# ── TTL constants (seconds) ───────────────────────────────────

TTL_TENANT_AUTH = 300            # 5 min
TTL_SYNC_CONFIG = 120            # 2 min
TTL_SYNC_LOGS = 60               # 1 min
TTL_CH_COUNT = 300               # 5 min
TTL_CH_SCHEMA = 3600             # 1 hour
TTL_PROFILE = 600                # 10 min
TTL_VALIDATION_ERRORS = 1800     # 30 min
TTL_EXISTING_LOANS = 300         # 5 min


# ── Key builders ──────────────────────────────────────────────

def _key(tenant_id: str, resource: str, *parts) -> str:
    segments = [tenant_id, resource] + [str(p) for p in parts if p is not None]
    return ':'.join(segments)


def tenant_auth_key(api_key_prefix: str) -> str:
    return _key('_global', 'tenant_auth', api_key_prefix)


def sync_configs_key(tenant_id: str) -> str:
    return _key(tenant_id, 'sync_configs')


def sync_logs_key(tenant_id: str, limit: int) -> str:
    return _key(tenant_id, 'sync_logs', 'recent', limit)


def ch_count_key(tenant_id: str, table: str, loan_type: str) -> str:
    return _key(tenant_id, 'ch_count', table, loan_type)


def ch_schema_key(tenant_id: str, table: str) -> str:
    return _key(tenant_id, 'ch_schema', table)


def profile_key(tenant_id: str, loan_type: str, data_type: str) -> str:
    return _key(tenant_id, 'profile', loan_type, data_type)


def validation_errors_key(tenant_id: str, sync_log_id) -> str:
    return _key(tenant_id, 'val_errors', sync_log_id)


def existing_loans_key(tenant_id: str, loan_type: str) -> str:
    return _key(tenant_id, 'existing_loans', loan_type)


# ── Safe get / set / delete wrappers ─────────────────────────

def cache_get(key: str) -> Any:
    try:
        return cache.get(key)
    except Exception:
        logger.warning("Cache GET failed for key=%s", key, exc_info=True)
        return None


def cache_set(key: str, value: Any, ttl: int) -> None:
    try:
        cache.set(key, value, timeout=ttl)
    except Exception:
        logger.warning("Cache SET failed for key=%s", key, exc_info=True)


def cache_delete(key: str) -> None:
    try:
        cache.delete(key)
    except Exception:
        logger.warning("Cache DELETE failed for key=%s", key, exc_info=True)


def cache_delete_many(keys: list) -> None:
    try:
        cache.delete_many(keys)
    except Exception:
        logger.warning("Cache DELETE_MANY failed for %d keys", len(keys), exc_info=True)


def cache_get_or_set(key: str, default_func: Callable, ttl: int) -> Any:
    """
    Return cached value, or call default_func(), cache it, and return it.
    If Redis is down, just calls default_func() directly.
    """
    try:
        value = cache.get(key)
        if value is not None:
            return value
    except Exception:
        logger.warning("Cache GET failed in get_or_set for key=%s", key, exc_info=True)

    value = default_func()

    if value is not None:
        try:
            cache.set(key, value, timeout=ttl)
        except Exception:
            logger.warning("Cache SET failed in get_or_set for key=%s", key, exc_info=True)

    return value


# ── Invalidation ──────────────────────────────────────────────

def invalidate_after_sync(tenant_id: str, loan_type: str) -> None:
    """
    Invalidate all caches that become stale after a sync completes.
    Called from SyncEngine after sync finishes (success or failure).
    """
    keys_to_delete = [
        sync_configs_key(tenant_id),
        sync_logs_key(tenant_id, 10),
        sync_logs_key(tenant_id, 20),
        ch_count_key(tenant_id, 'fact_credit', loan_type),
        ch_count_key(tenant_id, 'fact_payment', loan_type),
        profile_key(tenant_id, loan_type, 'credit'),
        profile_key(tenant_id, loan_type, 'payment'),
        existing_loans_key(tenant_id, loan_type),
    ]
    logger.info(
        "Invalidating %d cache keys after sync: tenant=%s loan_type=%s",
        len(keys_to_delete), tenant_id, loan_type,
    )
    cache_delete_many(keys_to_delete)


def invalidate_tenant_auth(api_key_prefix: str) -> None:
    cache_delete(tenant_auth_key(api_key_prefix))
