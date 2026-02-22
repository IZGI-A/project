"""
Custom Prometheus metrics for the Financial Data Integration Adapter.

Instrumentation points:
  - SyncEngine.sync()       -> sync_operations_total, sync_duration_seconds
  - SyncEngine._fail_sync() -> sync_operations_total
  - BaseValidator           -> validation_errors_total
  - StorageManager          -> clickhouse_rows_inserted_total
  - Upload views            -> data_upload_bytes_total
"""
from prometheus_client import Counter, Histogram

# Sync operation counter (incremented on COMPLETED or FAILED)
sync_operations_total = Counter(
    'sync_operations_total',
    'Total number of sync operations',
    ['tenant', 'loan_type', 'status'],
)

# Sync duration histogram (seconds)
sync_duration_seconds = Histogram(
    'sync_duration_seconds',
    'Duration of sync operations in seconds',
    ['tenant', 'loan_type'],
    buckets=[0.5, 1, 2, 5, 10, 30, 60, 120, 300],
)

# Validation error counter
validation_errors_total = Counter(
    'validation_errors_total',
    'Total number of validation errors',
    ['tenant', 'error_type'],
)

# ClickHouse rows inserted counter
clickhouse_rows_inserted_total = Counter(
    'clickhouse_rows_inserted_total',
    'Total rows inserted into ClickHouse',
    ['tenant', 'table'],
)

# Data upload bytes counter
data_upload_bytes_total = Counter(
    'data_upload_bytes_total',
    'Total bytes uploaded by tenants',
    ['tenant'],
)
