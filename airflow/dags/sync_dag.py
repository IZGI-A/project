"""
Airflow DAG for data synchronization.

Syncs credit and payment data from the external bank for each tenant.
Runs every 60 minutes by default.
"""
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# Add project root to path for Django imports
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, PROJECT_ROOT)

default_args = {
    'owner': 'findata',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


def setup_django():
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
    import django
    django.setup()


def sync_tenant_loan_type(tenant_id: str, loan_type: str, **kwargs):
    """Sync a specific tenant and loan type."""
    setup_django()
    from adapter.models import Tenant, SyncConfiguration
    from adapter.sync.engine import SyncEngine
    from config.db_router import set_current_tenant_schema, clear_current_tenant_schema

    try:
        tenant = Tenant.objects.get(tenant_id=tenant_id, is_active=True)
        set_current_tenant_schema(tenant.pg_schema)

        config = SyncConfiguration.objects.get(loan_type=loan_type, is_enabled=True)

        engine = SyncEngine(
            tenant_id=tenant.tenant_id,
            pg_schema=tenant.pg_schema,
            ch_database=tenant.ch_database,
            external_bank_url=config.external_bank_url,
        )
        sync_log = engine.sync(loan_type)
        return {
            'sync_log_id': str(sync_log.id),
            'status': sync_log.status,
            'errors': sync_log.error_count,
        }
    except Exception as e:
        raise RuntimeError(f"Sync failed for {tenant_id}/{loan_type}: {e}") from e
    finally:
        clear_current_tenant_schema()


TENANTS = ['BANK001', 'BANK002', 'BANK003']
LOAN_TYPES = ['RETAIL', 'COMMERCIAL']

with DAG(
    dag_id='financial_data_sync',
    default_args=default_args,
    description='Sync credit and payment data from external bank',
    schedule_interval=timedelta(minutes=60),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['findata', 'sync'],
) as dag:
    for tenant_id in TENANTS:
        for loan_type in LOAN_TYPES:
            task_id = f'sync_{tenant_id.lower()}_{loan_type.lower()}'
            PythonOperator(
                task_id=task_id,
                python_callable=sync_tenant_loan_type,
                op_kwargs={'tenant_id': tenant_id, 'loan_type': loan_type},
            )
