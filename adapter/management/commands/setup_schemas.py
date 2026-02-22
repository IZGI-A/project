"""
Management command to set up tenant schemas and apply migrations to each.

Creates bank001, bank002, bank003 schemas in the financial_shared database,
then creates tenant-specific tables (sync_configurations, sync_logs,
validation_errors) in each schema.
"""
from django.core.management.base import BaseCommand
from django.db import connection


TENANT_SCHEMAS = ['bank001', 'bank002', 'bank003']

# Tenant-specific tables (excluding 'tenants' which lives in public schema)
TENANT_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS sync_configurations (
    id BIGSERIAL PRIMARY KEY,
    loan_type VARCHAR(20) UNIQUE NOT NULL,
    external_bank_url VARCHAR(500) NOT NULL,
    sync_interval_minutes INTEGER NOT NULL DEFAULT 60,
    is_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    last_sync_at TIMESTAMPTZ NULL,
    last_sync_status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS sync_logs (
    id UUID PRIMARY KEY,
    loan_type VARCHAR(20) NOT NULL,
    batch_id UUID NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'STARTED',
    total_credit_rows INTEGER NOT NULL DEFAULT 0,
    total_payment_rows INTEGER NOT NULL DEFAULT 0,
    valid_credit_rows INTEGER NOT NULL DEFAULT 0,
    valid_payment_rows INTEGER NOT NULL DEFAULT 0,
    error_count INTEGER NOT NULL DEFAULT 0,
    error_summary JSONB NOT NULL DEFAULT '{}',
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ NULL
);

CREATE TABLE IF NOT EXISTS validation_errors (
    id BIGSERIAL PRIMARY KEY,
    sync_log_id UUID NOT NULL REFERENCES sync_logs(id) ON DELETE CASCADE,
    row_number INTEGER NOT NULL,
    file_type VARCHAR(20) NOT NULL,
    field_name VARCHAR(100) NOT NULL,
    error_type VARCHAR(50) NOT NULL,
    error_message TEXT NOT NULL,
    raw_value TEXT NULL
);

CREATE INDEX IF NOT EXISTS idx_validation_errors_sync_log
    ON validation_errors(sync_log_id);

CREATE INDEX IF NOT EXISTS idx_sync_logs_started_at
    ON sync_logs(started_at DESC);
"""


class Command(BaseCommand):
    help = 'Create tenant schemas and tables in PostgreSQL'

    def handle(self, *args, **options):
        with connection.cursor() as cursor:
            for schema in TENANT_SCHEMAS:
                # Create schema
                cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
                self.stdout.write(f"Schema '{schema}' ensured.")

                # Set search_path to create tables in tenant schema
                cursor.execute(f"SET search_path TO {schema}, public")

                # Create tenant-specific tables
                cursor.execute(TENANT_TABLES_SQL)
                self.stdout.write(
                    self.style.SUCCESS(f"  Tables created in schema '{schema}'")
                )

            # Reset search_path
            cursor.execute("SET search_path TO public")

        self.stdout.write(self.style.SUCCESS("All tenant schemas set up successfully."))
