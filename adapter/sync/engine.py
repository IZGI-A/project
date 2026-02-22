"""
Sync Engine: orchestrates the full data sync pipeline.

Pipeline: FETCH -> VALIDATE -> NORMALIZE -> STORE -> LOG
If error_rate > 50%, abort and preserve old data.
"""
import logging
import uuid
from datetime import timezone, datetime

from adapter.models import SyncLog, SyncConfiguration
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

    def sync(self, loan_type: str) -> SyncLog:
        """
        Execute the full sync pipeline for a loan type.

        Returns the SyncLog with results.
        """
        sync_log = SyncLog(
            loan_type=loan_type,
            batch_id=self.batch_id,
            status='STARTED',
        )
        sync_log.save()

        try:
            # 1. FETCH
            self._update_status(sync_log, 'FETCHING')
            credit_records = self.fetcher.fetch(loan_type, 'credit')
            payment_records = self.fetcher.fetch(loan_type, 'payment_plan')

            sync_log.total_credit_rows = len(credit_records)
            sync_log.total_payment_rows = len(payment_records)
            sync_log.save()

            # 2. VALIDATE
            self._update_status(sync_log, 'VALIDATING')

            # Field validation - credits
            credit_result = self._validate_credits(credit_records, loan_type)

            # Field validation - payments
            payment_field_result = self._validate_payments(payment_records, loan_type)

            # Cross-file validation (payments against valid credits + existing)
            cross_result = self.cross_validator.validate(
                credit_result.valid_records,
                payment_field_result.valid_records,
                self.ch_database,
                loan_type,
            )

            # Check error rate
            total_rows = sync_log.total_credit_rows + sync_log.total_payment_rows
            total_errors = credit_result.error_count + payment_field_result.error_count + cross_result.error_count
            total_valid_credit = credit_result.valid_rows
            total_valid_payment = cross_result.valid_rows

            if total_rows > 0 and (total_rows - total_valid_credit - total_valid_payment) / total_rows > MAX_ERROR_RATE:
                self._fail_sync(
                    sync_log, credit_result, payment_field_result, cross_result,
                    'Error rate exceeds 50%. Aborting sync, old data preserved.',
                )
                # Still move failed records from upload to failed store
                self._cleanup_redis(
                    loan_type, credit_result, payment_field_result,
                    cross_result, credit_records, payment_records,
                )
                return sync_log

            # 3. NORMALIZE
            self._update_status(sync_log, 'NORMALIZING')
            normalized_credits = self._normalize_credits(credit_result.valid_records, loan_type)
            normalized_payments = self._normalize_payments(cross_result.valid_records)

            # 4. STORE
            self._update_status(sync_log, 'STORING')

            if normalized_credits:
                self.storage_manager.store_credits(normalized_credits, loan_type, self.batch_id)

            if normalized_payments:
                self.storage_manager.store_payments(normalized_payments, loan_type, self.batch_id)

            # 5. Clean up Redis - move failed records to failed store
            self._cleanup_redis(
                loan_type, credit_result, payment_field_result,
                cross_result, credit_records, payment_records,
            )

            # 6. LOG - success
            sync_log.status = 'COMPLETED'
            sync_log.valid_credit_rows = total_valid_credit
            sync_log.valid_payment_rows = total_valid_payment
            sync_log.error_count = total_errors
            sync_log.error_summary = self._merge_error_summaries(
                credit_result, payment_field_result, cross_result,
            )
            sync_log.completed_at = datetime.now(timezone.utc)
            sync_log.save()

            # Save validation errors to DB
            self._save_validation_errors(
                sync_log, credit_result, payment_field_result, cross_result,
            )

            # Update sync configuration
            self._update_sync_config(loan_type, 'COMPLETED')

            logger.info(
                "Sync completed for %s/%s. Credits: %d/%d, Payments: %d/%d, Errors: %d",
                self.tenant_id, loan_type,
                total_valid_credit, sync_log.total_credit_rows,
                total_valid_payment, sync_log.total_payment_rows,
                total_errors,
            )
            return sync_log

        except Exception as e:
            sync_log.status = 'FAILED'
            sync_log.error_summary = {'exception': str(e)}
            sync_log.completed_at = datetime.now(timezone.utc)
            sync_log.save()
            self._update_sync_config(loan_type, 'FAILED')
            logger.exception("Sync failed for %s/%s: %s", self.tenant_id, loan_type, e)
            return sync_log

    def _validate_credits(self, records: list, loan_type: str) -> BatchValidationResult:
        result = BatchValidationResult()
        for idx, row in enumerate(records, start=1):
            vr = self.credit_validator.validate_row(row, idx, loan_type)
            result.add_row_result(vr, row)
        return result

    def _validate_payments(self, records: list, loan_type: str) -> BatchValidationResult:
        result = BatchValidationResult()
        for idx, row in enumerate(records, start=1):
            vr = self.payment_validator.validate_row(row, idx, loan_type)
            result.add_row_result(vr, row)
        return result

    def _normalize_credits(self, records: list, loan_type: str) -> list:
        normalized = []
        for record in records:
            record = self.date_normalizer.normalize_credit(record)
            record = self.rate_normalizer.normalize_credit(record, loan_type)
            record = self.category_normalizer.normalize_credit(record, loan_type)
            normalized.append(record)
        return normalized

    def _normalize_payments(self, records: list) -> list:
        normalized = []
        for record in records:
            record = self.date_normalizer.normalize_payment(record)
            record = self.category_normalizer.normalize_payment(record)
            normalized.append(record)
        return normalized

    def _fail_sync(self, sync_log, credit_result, payment_result, cross_result, reason):
        sync_log.status = 'FAILED'
        sync_log.valid_credit_rows = credit_result.valid_rows
        sync_log.valid_payment_rows = cross_result.valid_rows
        sync_log.error_count = (
            credit_result.error_count + payment_result.error_count + cross_result.error_count
        )
        sync_log.error_summary = {
            'reason': reason,
            **self._merge_error_summaries(credit_result, payment_result, cross_result),
        }
        sync_log.completed_at = datetime.now(timezone.utc)
        sync_log.save()

        self._save_validation_errors(sync_log, credit_result, payment_result, cross_result)
        self._update_sync_config(sync_log.loan_type, 'FAILED')

        logger.warning(
            "Sync aborted for %s/%s: %s", self.tenant_id, sync_log.loan_type, reason,
        )

    def _merge_error_summaries(self, *results):
        merged = {}
        for r in results:
            for key, count in r.get_error_summary().items():
                merged[key] = merged.get(key, 0) + count
        return merged

    def _save_validation_errors(self, sync_log, credit_result, payment_result, cross_result):
        from adapter.models import ValidationError as VE
        errors_to_create = []

        for err in credit_result.errors:
            errors_to_create.append(VE(
                sync_log=sync_log,
                row_number=err['row_number'],
                file_type='credit',
                field_name=err['field_name'],
                error_type=err['error_type'],
                error_message=err['error_message'],
                raw_value=err.get('raw_value'),
            ))

        for err in payment_result.errors:
            errors_to_create.append(VE(
                sync_log=sync_log,
                row_number=err['row_number'],
                file_type='payment_plan',
                field_name=err['field_name'],
                error_type=err['error_type'],
                error_message=err['error_message'],
                raw_value=err.get('raw_value'),
            ))

        for err in cross_result.errors:
            errors_to_create.append(VE(
                sync_log=sync_log,
                row_number=err['row_number'],
                file_type='payment_plan',
                field_name=err['field_name'],
                error_type=err['error_type'],
                error_message=err['error_message'],
                raw_value=err.get('raw_value'),
            ))

        if errors_to_create:
            VE.objects.bulk_create(errors_to_create, batch_size=1000)

    def _cleanup_redis(self, loan_type, credit_result, payment_field_result,
                       cross_result, original_credits, original_payments):
        """
        After sync, always clear upload keys (extbank:).
        Move failed records to the failed store (extbank_failed:).
        """
        from external_bank import storage

        # Credits
        if original_credits:
            storage.clear_data(self.tenant_id, loan_type, 'credit')

            if credit_result.error_count > 0:
                failed_row_numbers = set(e['row_number'] for e in credit_result.errors)
                failed_credits = [
                    original_credits[i - 1] for i in failed_row_numbers
                    if i - 1 < len(original_credits)
                ]
                storage.store_failed(self.tenant_id, loan_type, 'credit', failed_credits)
                logger.info(
                    "Redis: %s/%s/credit - %d failed rows moved to failed store",
                    self.tenant_id, loan_type, len(failed_credits),
                )
            else:
                storage.clear_failed(self.tenant_id, loan_type, 'credit')
                logger.info("Redis: %s/%s/credit - all valid, cleared", self.tenant_id, loan_type)

        # Payments
        if original_payments:
            storage.clear_data(self.tenant_id, loan_type, 'payment_plan')

            failed_payment_rows = set()
            for e in payment_field_result.errors:
                failed_payment_rows.add(e['row_number'])
            for e in cross_result.errors:
                failed_payment_rows.add(e['row_number'])

            if failed_payment_rows:
                failed_payments = [
                    original_payments[i - 1] for i in failed_payment_rows
                    if i - 1 < len(original_payments)
                ]
                storage.store_failed(self.tenant_id, loan_type, 'payment_plan', failed_payments)
                logger.info(
                    "Redis: %s/%s/payment_plan - %d failed rows moved to failed store",
                    self.tenant_id, loan_type, len(failed_payments),
                )
            else:
                storage.clear_failed(self.tenant_id, loan_type, 'payment_plan')
                logger.info("Redis: %s/%s/payment_plan - all valid, cleared", self.tenant_id, loan_type)

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
