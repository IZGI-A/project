"""
Sync Engine: orchestrates the full data sync pipeline.

Pipeline: FETCH -> VALIDATE -> NORMALIZE -> STORE -> LOG

Memory-efficient chunked processing:
  - Data is fetched from Redis in chunks (50k rows)
  - Each chunk is validated, normalized, and inserted into ClickHouse staging
  - Only after all chunks are processed, partition is atomically replaced
  - Memory usage is bounded by chunk size, not total dataset size

If error_rate > 50%, abort and preserve old data.
"""
import logging
import time
import uuid
from datetime import timezone, datetime

import redis as _redis
from django.conf import settings

from adapter.models import SyncLog, SyncConfiguration, ValidationError as VE
from core.cache import invalidate_after_sync
from adapter.metrics import (
    sync_operations_total,
    sync_duration_seconds,
    validation_errors_total,
)
from adapter.validators.field_validators import CreditFieldValidator, PaymentFieldValidator
from adapter.validators.cross_validators import CrossFileValidator
from adapter.validators.base import BatchValidationResult
from adapter.normalizers.date_normalizer import DateNormalizer
from adapter.normalizers.rate_normalizer import RateNormalizer
from adapter.normalizers.category_normalizer import CategoryNormalizer
from adapter.storage.manager import StorageManager
from adapter.sync.fetcher import DataFetcher

logger = logging.getLogger(__name__)

MAX_ERROR_RATE = 0.50  # Abort if more than 50% of rows have errors
ERROR_SAVE_BATCH = 1000
SYNC_LOCK_TTL = 600  # 10 minutes


class SyncEngine:
    """Orchestrates the data sync pipeline for a tenant and loan type."""

    def __init__(self, tenant_id: str, pg_schema: str, ch_database: str,
                 external_bank_url: str):
        self.tenant_id = tenant_id
        self.pg_schema = pg_schema
        self.ch_database = ch_database
        self.external_bank_url = external_bank_url
        self.batch_id = str(uuid.uuid4())

        # Components
        self.fetcher = DataFetcher(external_bank_url, tenant_id)
        self.credit_validator = CreditFieldValidator()
        self.payment_validator = PaymentFieldValidator()
        self.cross_validator = CrossFileValidator()
        self.date_normalizer = DateNormalizer()
        self.rate_normalizer = RateNormalizer()
        self.category_normalizer = CategoryNormalizer()
        self.storage_manager = StorageManager(ch_database)

    def _get_redis(self):
        return _redis.Redis(
            host=getattr(settings, 'REDIS_HOST', 'redis'),
            port=6379, db=0,
        )

    def sync(self, loan_type: str, wait_for_lock: bool = False) -> SyncLog:
        """
        Execute the full sync pipeline for a loan type (chunked, memory efficient).
        Acquires a distributed lock to prevent concurrent syncs on the same
        tenant/loan_type staging tables.

        Args:
            wait_for_lock: If True, wait up to SYNC_LOCK_TTL seconds for the
                           lock instead of failing immediately.
        """
        # Acquire distributed lock
        r = self._get_redis()
        lock_key = f"sync_lock:{self.tenant_id}:{loan_type}"
        acquired = r.set(lock_key, self.batch_id, nx=True, ex=SYNC_LOCK_TTL)

        if not acquired and wait_for_lock:
            logger.info(
                "Sync lock held for %s/%s, waiting for release...",
                self.tenant_id, loan_type,
            )
            waited = 0
            while not acquired and waited < SYNC_LOCK_TTL:
                time.sleep(2)
                waited += 2
                acquired = r.set(lock_key, self.batch_id, nx=True, ex=SYNC_LOCK_TTL)
            if acquired:
                logger.info("Lock acquired after %ds wait for %s/%s",
                            waited, self.tenant_id, loan_type)

        if not acquired:
            logger.warning(
                "Sync lock held by another process for %s/%s, aborting",
                self.tenant_id, loan_type,
            )
            sync_log = SyncLog(
                loan_type=loan_type,
                batch_id=self.batch_id,
                status='FAILED',
                error_summary={'reason': 'Concurrent sync in progress'},
                completed_at=datetime.now(timezone.utc),
            )
            sync_log.save()
            return sync_log

        sync_log = SyncLog(
            loan_type=loan_type,
            batch_id=self.batch_id,
            status='STARTED',
        )
        sync_log.save()
        start_time = time.time()

        try:
            # Get row counts (O(1), no data loading)
            total_credits = self.fetcher.fetch_row_count(loan_type, 'credit')
            total_payments = self.fetcher.fetch_row_count(loan_type, 'payment_plan')
            sync_log.total_credit_rows = total_credits
            sync_log.total_payment_rows = total_payments
            sync_log.save()

            logger.info(
                "Starting chunked sync for %s/%s: %d credits, %d payments",
                self.tenant_id, loan_type, total_credits, total_payments,
            )

            # ── Phase 1: CREDITS (validate → normalize → staging) ──
            self._update_status(sync_log, 'FETCHING')
            client = self.storage_manager._get_client()
            client.command("TRUNCATE TABLE staging_credit")

            valid_credit_count = 0
            credit_error_count = 0
            credit_error_summary = {}
            valid_loan_ids = set()  # Only store IDs, not full records
            all_credit_errors = []
            failed_credit_rows = []  # Raw rows that failed validation
            global_row_idx = 0

            self._update_status(sync_log, 'VALIDATING')

            for chunk in self.fetcher.fetch_iter(loan_type, 'credit'):
                # Validate chunk
                chunk_valid = []
                for row in chunk:
                    global_row_idx += 1
                    vr = self.credit_validator.validate_row(row, global_row_idx, loan_type)
                    if vr.is_valid:
                        chunk_valid.append(row)
                        loan_id = row.get('loan_account_number', '')
                        if loan_id:
                            valid_loan_ids.add(loan_id)
                    else:
                        credit_error_count += 1
                        if len(failed_credit_rows) < 10000:
                            failed_credit_rows.append(row)
                        for err in vr.errors:
                            err_type = err.get('error_type', 'UNKNOWN')
                            credit_error_summary[err_type] = credit_error_summary.get(err_type, 0) + 1
                            if len(all_credit_errors) < 50000:
                                all_credit_errors.append(err)

                # Normalize and insert valid records into staging
                if chunk_valid:
                    self._update_status(sync_log, 'NORMALIZING')
                    normalized = []
                    for record in chunk_valid:
                        record = self.date_normalizer.normalize_credit(record)
                        record = self.rate_normalizer.normalize_credit(record, loan_type)
                        record = self.category_normalizer.normalize_credit(record, loan_type)
                        normalized.append(record)

                    self._update_status(sync_log, 'STORING')
                    columns = self.storage_manager._credit_columns()
                    rows = [
                        self.storage_manager._prepare_credit_row(r, loan_type, self.batch_id)
                        for r in normalized
                    ]
                    client.insert('staging_credit', rows, column_names=columns)
                    valid_credit_count += len(rows)
                    del normalized, rows  # Free memory

                del chunk, chunk_valid  # Free memory

            logger.info(
                "Credits processed: %d valid, %d errors out of %d total",
                valid_credit_count, credit_error_count, total_credits,
            )

            # ── Phase 2: PAYMENTS (validate → cross-validate → normalize → staging) ──
            client.command("TRUNCATE TABLE staging_payment")

            # Also get existing ClickHouse loan IDs for cross-validation
            existing_loan_ids = self.cross_validator._get_existing_loans(
                self.ch_database, loan_type
            )
            all_valid_loans = valid_loan_ids | existing_loan_ids

            valid_payment_count = 0
            payment_error_count = 0
            payment_error_summary = {}
            all_payment_errors = []
            failed_payment_rows = []  # Raw rows that failed validation
            global_row_idx = 0

            for chunk in self.fetcher.fetch_iter(loan_type, 'payment_plan'):
                chunk_valid = []
                for row in chunk:
                    global_row_idx += 1
                    # Field validation
                    vr = self.payment_validator.validate_row(row, global_row_idx, loan_type)
                    if not vr.is_valid:
                        payment_error_count += 1
                        if len(failed_payment_rows) < 10000:
                            failed_payment_rows.append(row)
                        for err in vr.errors:
                            err_type = err.get('error_type', 'UNKNOWN')
                            payment_error_summary[err_type] = payment_error_summary.get(err_type, 0) + 1
                            if len(all_payment_errors) < 50000:
                                all_payment_errors.append(err)
                        continue

                    # Cross-validation: check loan_account_number exists
                    loan_id = row.get('loan_account_number', '')
                    if loan_id not in all_valid_loans:
                        payment_error_count += 1
                        if len(failed_payment_rows) < 10000:
                            failed_payment_rows.append(row)
                        err = {
                            'row_number': global_row_idx,
                            'field_name': 'loan_account_number',
                            'error_type': 'CROSS_REFERENCE',
                            'error_message': f'loan_account_number {loan_id} not found in credit records',
                            'raw_value': loan_id,
                        }
                        payment_error_summary['CROSS_REFERENCE'] = payment_error_summary.get('CROSS_REFERENCE', 0) + 1
                        if len(all_payment_errors) < 50000:
                            all_payment_errors.append(err)
                        continue

                    chunk_valid.append(row)

                # Normalize and insert valid payments into staging
                if chunk_valid:
                    normalized = []
                    for record in chunk_valid:
                        record = self.date_normalizer.normalize_payment(record)
                        record = self.category_normalizer.normalize_payment(record)
                        normalized.append(record)

                    columns = self.storage_manager._payment_columns()
                    rows = [
                        self.storage_manager._prepare_payment_row(r, loan_type, self.batch_id)
                        for r in normalized
                    ]
                    client.insert('staging_payment', rows, column_names=columns)
                    valid_payment_count += len(rows)
                    del normalized, rows

                del chunk, chunk_valid

            logger.info(
                "Payments processed: %d valid, %d errors out of %d total",
                valid_payment_count, payment_error_count, total_payments,
            )

            # ── Phase 3: Check error rate ──
            total_rows = total_credits + total_payments
            total_errors = credit_error_count + payment_error_count

            if total_rows > 0 and (total_rows - valid_credit_count - valid_payment_count) / total_rows > MAX_ERROR_RATE:
                # Abort — clean staging, preserve fact tables
                client.command("TRUNCATE TABLE staging_credit")
                client.command("TRUNCATE TABLE staging_payment")

                sync_log.status = 'FAILED'
                sync_log.valid_credit_rows = valid_credit_count
                sync_log.valid_payment_rows = valid_payment_count
                sync_log.error_count = total_errors
                sync_log.error_summary = {
                    'reason': 'Error rate exceeds 50%. Aborting sync, old data preserved.',
                    **credit_error_summary,
                    **payment_error_summary,
                }
                sync_log.completed_at = datetime.now(timezone.utc)
                sync_log.save()

                self._save_errors_batched(sync_log, all_credit_errors, 'credit')
                self._save_errors_batched(sync_log, all_payment_errors, 'payment_plan')
                self._store_failed_rows(loan_type, failed_credit_rows, failed_payment_rows)
                self._update_sync_config(loan_type, 'FAILED')
                self._cleanup_redis(loan_type)

                sync_operations_total.labels(
                    tenant=self.tenant_id, loan_type=loan_type, status='FAILED',
                ).inc()
                invalidate_after_sync(self.tenant_id, loan_type)
                logger.warning(
                    "Sync aborted for %s/%s: error rate %.1f%%",
                    self.tenant_id, loan_type,
                    (total_rows - valid_credit_count - valid_payment_count) / total_rows * 100,
                )
                return sync_log

            # ── Phase 4: Atomic partition replace ──
            self._update_status(sync_log, 'STORING')

            if valid_credit_count > 0:
                client.command(
                    f"ALTER TABLE fact_credit REPLACE PARTITION '{loan_type}' "
                    f"FROM staging_credit"
                )
            client.command("TRUNCATE TABLE staging_credit")

            if valid_payment_count > 0:
                client.command(
                    f"ALTER TABLE fact_payment REPLACE PARTITION '{loan_type}' "
                    f"FROM staging_payment"
                )
            client.command("TRUNCATE TABLE staging_payment")

            from adapter.metrics import clickhouse_rows_inserted_total
            clickhouse_rows_inserted_total.labels(
                tenant=self.tenant_id, table='fact_credit',
            ).inc(valid_credit_count)
            clickhouse_rows_inserted_total.labels(
                tenant=self.tenant_id, table='fact_payment',
            ).inc(valid_payment_count)

            # ── Phase 5: Success logging ──
            sync_log.status = 'COMPLETED'
            sync_log.valid_credit_rows = valid_credit_count
            sync_log.valid_payment_rows = valid_payment_count
            sync_log.error_count = total_errors
            sync_log.error_summary = {**credit_error_summary, **payment_error_summary}
            sync_log.completed_at = datetime.now(timezone.utc)
            sync_log.save()

            self._save_errors_batched(sync_log, all_credit_errors, 'credit')
            self._save_errors_batched(sync_log, all_payment_errors, 'payment_plan')
            self._store_failed_rows(loan_type, failed_credit_rows, failed_payment_rows)

            sync_operations_total.labels(
                tenant=self.tenant_id, loan_type=loan_type, status='COMPLETED',
            ).inc()
            sync_duration_seconds.labels(
                tenant=self.tenant_id, loan_type=loan_type,
            ).observe(time.time() - start_time)

            for err in all_credit_errors + all_payment_errors:
                validation_errors_total.labels(
                    tenant=self.tenant_id, error_type=err.get('error_type', 'UNKNOWN'),
                ).inc()

            self._update_sync_config(loan_type, 'COMPLETED')
            self._cleanup_redis(loan_type)
            invalidate_after_sync(self.tenant_id, loan_type)

            logger.info(
                "Sync completed for %s/%s. Credits: %d/%d, Payments: %d/%d, Errors: %d",
                self.tenant_id, loan_type,
                valid_credit_count, total_credits,
                valid_payment_count, total_payments,
                total_errors,
            )
            return sync_log

        except Exception as e:
            # Clean up staging on failure
            try:
                client = self.storage_manager._get_client()
                client.command("TRUNCATE TABLE staging_credit")
                client.command("TRUNCATE TABLE staging_payment")
            except Exception:
                pass

            sync_log.status = 'FAILED'
            sync_log.error_summary = {'exception': str(e)}
            sync_log.completed_at = datetime.now(timezone.utc)
            sync_log.save()
            self._update_sync_config(loan_type, 'FAILED')
            sync_operations_total.labels(
                tenant=self.tenant_id, loan_type=loan_type, status='FAILED',
            ).inc()
            sync_duration_seconds.labels(
                tenant=self.tenant_id, loan_type=loan_type,
            ).observe(time.time() - start_time)
            invalidate_after_sync(self.tenant_id, loan_type)
            logger.exception("Sync failed for %s/%s: %s", self.tenant_id, loan_type, e)
            return sync_log
        finally:
            # Always release the distributed lock
            try:
                r.delete(lock_key)
            except Exception:
                pass

    def _save_errors_batched(self, sync_log, errors, file_type):
        """Save validation errors in batches to prevent memory spikes."""
        if not errors:
            return
        batch = []
        for err in errors:
            batch.append(VE(
                sync_log=sync_log,
                row_number=err['row_number'],
                file_type=file_type,
                field_name=err['field_name'],
                error_type=err['error_type'],
                error_message=err['error_message'],
                raw_value=err.get('raw_value'),
            ))
            if len(batch) >= ERROR_SAVE_BATCH:
                VE.objects.bulk_create(batch, batch_size=ERROR_SAVE_BATCH)
                batch = []
        if batch:
            VE.objects.bulk_create(batch, batch_size=ERROR_SAVE_BATCH)

    def _store_failed_rows(self, loan_type, failed_credit_rows, failed_payment_rows):
        """Store failed raw rows in Redis so users can preview/download them."""
        from external_bank import storage
        if failed_credit_rows:
            storage.store_failed(self.tenant_id, loan_type, 'credit', failed_credit_rows)
            logger.info("Stored %d failed credit rows for %s/%s",
                        len(failed_credit_rows), self.tenant_id, loan_type)
        if failed_payment_rows:
            storage.store_failed(self.tenant_id, loan_type, 'payment_plan', failed_payment_rows)
            logger.info("Stored %d failed payment rows for %s/%s",
                        len(failed_payment_rows), self.tenant_id, loan_type)

    def _cleanup_redis(self, loan_type):
        """Clear upload data from Redis after sync."""
        from external_bank import storage
        storage.clear_data(self.tenant_id, loan_type, 'credit')
        storage.clear_data(self.tenant_id, loan_type, 'payment_plan')
        logger.info("Redis upload data cleared for %s/%s", self.tenant_id, loan_type)

    def _update_status(self, sync_log, status):
        sync_log.status = status
        sync_log.save(update_fields=['status'])

    def _update_sync_config(self, loan_type, status):
        try:
            config = SyncConfiguration.objects.get(loan_type=loan_type)
            config.last_sync_at = datetime.now(timezone.utc)
            config.last_sync_status = status
            config.save(update_fields=['last_sync_at', 'last_sync_status'])
        except SyncConfiguration.DoesNotExist:
            pass
