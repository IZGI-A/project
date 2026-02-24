"""
Celery tasks for automated data synchronization.

check_and_sync runs every 60 seconds (via Beat), checks Redis for new
upload data, and dispatches run_sync for each tenant/loan_type that has
pending data.

A Redis-based distributed lock prevents concurrent syncs for the same
tenant/loan_type pair, which would corrupt staging tables.
"""
import logging

import redis as _redis
from celery import shared_task
from django.conf import settings

logger = logging.getLogger(__name__)

SYNC_LOCK_TTL = 600  # 10 minutes max lock duration


def _get_sync_lock_key(tenant_id: str, loan_type: str) -> str:
    return f"sync_lock:{tenant_id}:{loan_type}"


def _get_redis():
    return _redis.Redis(
        host=getattr(settings, 'REDIS_HOST', 'redis'),
        port=6379, db=0,
    )


@shared_task(name='adapter.tasks.check_and_sync')
def check_and_sync():
    """
    Poll Redis for new upload data across all active tenants.
    If data is found, dispatch a sync task for that tenant/loan_type.
    """
    from adapter.models import Tenant, SyncConfiguration
    from external_bank import storage
    from config.db_router import set_current_tenant_schema, clear_current_tenant_schema

    tenants = Tenant.objects.filter(is_active=True)
    dispatched = 0

    for tenant in tenants:
        set_current_tenant_schema(tenant.pg_schema)
        try:
            configs = SyncConfiguration.objects.filter(is_enabled=True)
            for config in configs:
                has_credit = storage.get_row_count(
                    tenant.tenant_id, config.loan_type, 'credit',
                ) > 0
                has_payment = storage.get_row_count(
                    tenant.tenant_id, config.loan_type, 'payment_plan',
                ) > 0

                if has_credit or has_payment:
                    # Skip if a sync is already running for this tenant/loan_type
                    r = _get_redis()
                    lock_key = _get_sync_lock_key(tenant.tenant_id, config.loan_type)
                    if r.exists(lock_key):
                        logger.info(
                            "Sync already in progress, skipping: %s/%s",
                            tenant.tenant_id, config.loan_type,
                        )
                        continue
                    run_sync.delay(tenant.tenant_id, config.loan_type)
                    dispatched += 1
                    logger.info(
                        "New data detected, dispatching sync: %s/%s",
                        tenant.tenant_id, config.loan_type,
                    )
        finally:
            clear_current_tenant_schema()

    if dispatched:
        logger.info("check_and_sync: dispatched %d sync tasks", dispatched)


@shared_task(name='adapter.tasks.run_sync', bind=True, max_retries=2,
             default_retry_delay=60)
def run_sync(self, tenant_id, loan_type):
    """
    Execute the full sync pipeline for a single tenant/loan_type.
    The SyncEngine itself acquires a distributed lock to prevent concurrent syncs.
    """
    from adapter.models import Tenant, SyncConfiguration
    from adapter.sync.engine import SyncEngine
    from config.db_router import set_current_tenant_schema, clear_current_tenant_schema

    try:
        tenant = Tenant.objects.get(tenant_id=tenant_id, is_active=True)
        set_current_tenant_schema(tenant.pg_schema)

        config = SyncConfiguration.objects.get(
            loan_type=loan_type, is_enabled=True,
        )

        engine = SyncEngine(
            tenant_id=tenant.tenant_id,
            pg_schema=tenant.pg_schema,
            ch_database=tenant.ch_database,
            external_bank_url=config.external_bank_url,
        )
        sync_log = engine.sync(loan_type)

        logger.info(
            "Sync completed: %s/%s status=%s errors=%d",
            tenant_id, loan_type, sync_log.status, sync_log.error_count,
        )
        return {
            'sync_log_id': str(sync_log.id),
            'status': sync_log.status,
            'errors': sync_log.error_count,
        }
    except Exception as exc:
        logger.exception("Sync failed: %s/%s", tenant_id, loan_type)
        raise self.retry(exc=exc)
    finally:
        clear_current_tenant_schema()
