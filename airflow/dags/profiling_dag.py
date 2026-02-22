"""
Airflow DAG for data profiling.

Runs profiling queries on ClickHouse fact tables.
This is primarily informational; results are served via the API in real-time.
This DAG can be used for scheduled health checks.
"""
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, PROJECT_ROOT)

default_args = {
    'owner': 'findata',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}


def setup_django():
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
    import django
    django.setup()


def run_profiling(tenant_id: str, **kwargs):
    """Run profiling for a specific tenant."""
    setup_django()
    from adapter.models import Tenant
    from adapter.profiling.engine import ProfilingEngine

    tenant = Tenant.objects.get(tenant_id=tenant_id, is_active=True)
    engine = ProfilingEngine(tenant.ch_database)

    results = {}
    for loan_type in ['RETAIL', 'COMMERCIAL']:
        for data_type in ['credit', 'payment']:
            try:
                profile = engine.profile(loan_type, data_type)
                results[f'{loan_type}_{data_type}'] = {
                    'row_count': profile.get('row_count', 0),
                    'status': 'ok',
                }
            except Exception as e:
                results[f'{loan_type}_{data_type}'] = {
                    'status': 'error',
                    'error': str(e),
                }

    return results


TENANTS = ['BANK001', 'BANK002', 'BANK003']

with DAG(
    dag_id='financial_data_profiling',
    default_args=default_args,
    description='Run data profiling on ClickHouse fact tables',
    schedule_interval=timedelta(hours=6),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['findata', 'profiling'],
) as dag:
    for tenant_id in TENANTS:
        PythonOperator(
            task_id=f'profile_{tenant_id.lower()}',
            python_callable=run_profiling,
            op_kwargs={'tenant_id': tenant_id},
        )
