"""
Large file upload & sync stress test.
Tests 200MB+ CSV files through the entire ETL pipeline.

Run inside the web container:
    docker compose exec web python /app/tests/test_large_upload.py
"""

import csv
import io
import os
import sys
import time
import traceback
import gc
import resource

# Ensure /app is on path for Django
sys.path.insert(0, "/app")
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
import django
django.setup()

from adapter.models import Tenant, SyncConfiguration, SyncLog
from adapter.sync.engine import SyncEngine
from config.db_router import set_current_tenant_schema, clear_current_tenant_schema
from external_bank import storage
from django.contrib.auth.hashers import make_password
import secrets

TENANT_ID = "BANK001"
LOAN_TYPE = "RETAIL"
DATA_DIR = "/app/data-test/large-data"
CREDIT_FILE = os.path.join(DATA_DIR, "retail_credit_large.csv")
PAYMENT_FILE = os.path.join(DATA_DIR, "retail_payment_plan_large.csv")


def get_memory_mb():
    """Get current RSS memory usage in MB."""
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024


def print_memory(label=""):
    print(f"  [Memory] {label}: {get_memory_mb():.0f} MB peak RSS")


def get_api_key():
    """Regenerate API key for BANK001."""
    tenant = Tenant.objects.get(tenant_id=TENANT_ID)
    raw_key = f"sk_live_{secrets.token_hex(24)}"
    tenant.api_key_hash = make_password(raw_key)
    tenant.api_key_prefix = raw_key[:16]
    tenant.save()
    return raw_key, tenant


def count_csv_rows(filepath):
    """Count rows without loading entire file."""
    count = 0
    with open(filepath, 'r') as f:
        next(f)  # skip header
        for _ in f:
            count += 1
    return count


def streaming_upload(filepath, file_type):
    """Upload a CSV file using streaming storage (memory efficient)."""
    file_size = os.path.getsize(filepath)
    file_name = os.path.basename(filepath)

    print(f"\n{'='*60}")
    print(f"UPLOADING: {file_name}")
    print(f"  Size: {file_size / (1024*1024):.1f} MB")
    print(f"  Type: {file_type}")
    print(f"{'='*60}")

    print_memory("Before upload")
    start = time.time()

    def csv_row_iter():
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter=';')
            for row in reader:
                yield dict(row)

    total_rows = storage.store_data_streaming(
        TENANT_ID, LOAN_TYPE, file_type, csv_row_iter()
    )

    elapsed = time.time() - start
    speed = file_size / elapsed / (1024 * 1024)

    print(f"  Rows stored: {total_rows:,}")
    print(f"  Time: {elapsed:.1f}s")
    print(f"  Speed: {speed:.1f} MB/s")
    print_memory("After upload")

    # Verify row count in Redis
    redis_count = storage.get_row_count(TENANT_ID, LOAN_TYPE, file_type)
    print(f"  Redis row count: {redis_count:,}")
    assert redis_count == total_rows, f"Mismatch! stored={total_rows}, redis={redis_count}"

    gc.collect()
    return total_rows


def run_sync(tenant):
    """Run sync engine directly (no HTTP overhead)."""
    print(f"\n{'='*60}")
    print(f"RUNNING SYNC: {TENANT_ID} / {LOAN_TYPE}")
    print(f"{'='*60}")

    print_memory("Before sync")
    start = time.time()

    set_current_tenant_schema(tenant.pg_schema)
    try:
        # Ensure sync config exists
        config, _ = SyncConfiguration.objects.get_or_create(
            loan_type=LOAN_TYPE,
            defaults={
                'external_bank_url': 'http://localhost:8000/bank/api',
                'sync_interval_minutes': 60,
                'is_enabled': True,
            }
        )

        engine = SyncEngine(
            tenant_id=TENANT_ID,
            pg_schema=tenant.pg_schema,
            ch_database=tenant.ch_database,
            external_bank_url=config.external_bank_url,
        )

        sync_log = engine.sync(LOAN_TYPE, wait_for_lock=True)
        elapsed = time.time() - start

        print(f"\n  Status: {sync_log.status}")
        print(f"  Time: {elapsed:.1f}s ({elapsed/60:.1f} min)")
        print(f"  Credit rows: {sync_log.valid_credit_rows}/{sync_log.total_credit_rows}")
        print(f"  Payment rows: {sync_log.valid_payment_rows}/{sync_log.total_payment_rows}")
        print(f"  Errors: {sync_log.error_count}")
        if sync_log.error_summary:
            print(f"  Error summary: {dict(list(sync_log.error_summary.items())[:5])}")
        print_memory("After sync")

        return sync_log
    finally:
        clear_current_tenant_schema()


def verify_clickhouse(tenant):
    """Verify data in ClickHouse."""
    print(f"\n{'='*60}")
    print(f"VERIFYING CLICKHOUSE: {tenant.ch_database}")
    print(f"{'='*60}")

    from adapter.clickhouse_manager import get_clickhouse_client
    client = get_clickhouse_client(database=tenant.ch_database)

    credit_count = client.command(
        f"SELECT count() FROM fact_credit WHERE loan_type = '{LOAN_TYPE}'"
    )
    payment_count = client.command(
        f"SELECT count() FROM fact_payment WHERE loan_type = '{LOAN_TYPE}'"
    )

    print(f"  fact_credit ({LOAN_TYPE}):  {credit_count:,} rows")
    print(f"  fact_payment ({LOAN_TYPE}): {payment_count:,} rows")

    return credit_count, payment_count


def main():
    print("=" * 60)
    print("LARGE FILE STRESS TEST (200MB+)")
    print("=" * 60)

    # Verify files exist
    for f in [CREDIT_FILE, PAYMENT_FILE]:
        if not os.path.exists(f):
            print(f"ERROR: File not found: {f}")
            sys.exit(1)
        size = os.path.getsize(f)
        print(f"  {os.path.basename(f)}: {size/(1024*1024):.1f} MB")

    total_start = time.time()
    print_memory("Initial")

    # Step 1: Setup
    _, tenant = get_api_key()
    print(f"\n  Tenant: {tenant.tenant_id}, PG: {tenant.pg_schema}, CH: {tenant.ch_database}")

    # Step 2: Upload credit file (streaming)
    credit_rows = streaming_upload(CREDIT_FILE, "credit")

    # Step 3: Upload payment file (streaming)
    payment_rows = streaming_upload(PAYMENT_FILE, "payment_plan")

    # Step 4: Run sync
    sync_log = run_sync(tenant)

    # Step 5: Verify ClickHouse
    ch_credits, ch_payments = verify_clickhouse(tenant)

    total_elapsed = time.time() - total_start

    # Summary
    print(f"\n{'='*60}")
    print("TEST SUMMARY")
    print(f"{'='*60}")
    print(f"  Total time:       {total_elapsed:.1f}s ({total_elapsed/60:.1f} min)")
    print(f"  Sync status:      {sync_log.status}")
    print(f"  Upload credits:   {credit_rows:,} rows")
    print(f"  Upload payments:  {payment_rows:,} rows")
    print(f"  CH credits:       {ch_credits:,} rows")
    print(f"  CH payments:      {ch_payments:,} rows")
    print(f"  Validation errors: {sync_log.error_count}")
    print_memory("Final")

    if sync_log.status == 'COMPLETED':
        print("\n  RESULT: PASSED")
    else:
        print(f"\n  RESULT: FAILED (status: {sync_log.status})")
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\nUNHANDLED ERROR: {e}")
        traceback.print_exc()
        sys.exit(1)
