"""
ClickHouse Storage Manager - Handles atomic data replacement using REPLACE PARTITION.

Flow:
1. TRUNCATE staging tables
2. INSERT normalized data into staging tables
3. If validation passed: ALTER TABLE REPLACE PARTITION from staging to fact
4. TRUNCATE staging tables
5. If validation failed: TRUNCATE staging (fact tables untouched)
"""
import logging
from datetime import datetime
from decimal import Decimal

from adapter.clickhouse_manager import get_clickhouse_client
from adapter.metrics import clickhouse_rows_inserted_total

logger = logging.getLogger(__name__)


class StorageManager:
    """Manages atomic data loading into ClickHouse using REPLACE PARTITION."""

    def __init__(self, ch_database: str):
        self.ch_database = ch_database
        self._tenant_id = ch_database.replace('_dw', '').upper()

    def _get_client(self):
        return get_clickhouse_client(database=self.ch_database)

    INSERT_BATCH_SIZE = 50_000  # rows per ClickHouse insert batch

    def store_credits(self, records: list, loan_type: str, batch_id: str) -> int:
        """
        Store credit records atomically using batched inserts.
        1. Truncate staging_credit
        2. Insert into staging_credit in batches (memory efficient)
        3. REPLACE PARTITION from staging_credit to fact_credit
        4. Truncate staging_credit
        """
        client = self._get_client()
        try:
            client.command("TRUNCATE TABLE staging_credit")

            if not records:
                return 0

            columns = self._credit_columns()
            total_inserted = 0

            for i in range(0, len(records), self.INSERT_BATCH_SIZE):
                batch = records[i:i + self.INSERT_BATCH_SIZE]
                rows = [self._prepare_credit_row(r, loan_type, batch_id) for r in batch]
                client.insert('staging_credit', rows, column_names=columns)
                total_inserted += len(rows)

            client.command(
                f"ALTER TABLE fact_credit REPLACE PARTITION '{loan_type}' "
                f"FROM staging_credit"
            )
            client.command("TRUNCATE TABLE staging_credit")

            clickhouse_rows_inserted_total.labels(
                tenant=self._tenant_id, table='fact_credit',
            ).inc(total_inserted)
            logger.info(
                "Stored %d credit records for %s in %s",
                total_inserted, loan_type, self.ch_database,
            )
            return total_inserted
        except Exception as e:
            logger.error("Failed to store credits: %s", e)
            try:
                client.command("TRUNCATE TABLE staging_credit")
            except Exception:
                pass
            raise

    def store_payments(self, records: list, loan_type: str, batch_id: str) -> int:
        """
        Store payment records atomically using batched inserts.
        Same pattern as store_credits but for fact_payment.
        """
        client = self._get_client()
        try:
            client.command("TRUNCATE TABLE staging_payment")

            if not records:
                return 0

            columns = self._payment_columns()
            total_inserted = 0

            for i in range(0, len(records), self.INSERT_BATCH_SIZE):
                batch = records[i:i + self.INSERT_BATCH_SIZE]
                rows = [self._prepare_payment_row(r, loan_type, batch_id) for r in batch]
                client.insert('staging_payment', rows, column_names=columns)
                total_inserted += len(rows)

            client.command(
                f"ALTER TABLE fact_payment REPLACE PARTITION '{loan_type}' "
                f"FROM staging_payment"
            )
            client.command("TRUNCATE TABLE staging_payment")

            clickhouse_rows_inserted_total.labels(
                tenant=self._tenant_id, table='fact_payment',
            ).inc(total_inserted)
            logger.info(
                "Stored %d payment records for %s in %s",
                total_inserted, loan_type, self.ch_database,
            )
            return total_inserted
        except Exception as e:
            logger.error("Failed to store payments: %s", e)
            try:
                client.command("TRUNCATE TABLE staging_payment")
            except Exception:
                pass
            raise

    def _credit_columns(self):
        return [
            'batch_id', 'loan_type', 'loaded_at',
            'loan_account_number', 'customer_id', 'customer_type',
            'loan_status_code', 'days_past_due', 'final_maturity_date',
            'total_installment_count', 'outstanding_installment_count',
            'paid_installment_count', 'first_payment_date',
            'original_loan_amount', 'outstanding_principal_balance',
            'nominal_interest_rate', 'total_interest_amount',
            'kkdf_rate', 'kkdf_amount', 'bsmv_rate', 'bsmv_amount',
            'grace_period_months', 'installment_frequency',
            'loan_start_date', 'loan_closing_date',
            'internal_rating', 'external_rating',
            'loan_product_type',
            'customer_region_code', 'sector_code',
            'internal_credit_rating', 'default_probability',
            'risk_class', 'customer_segment',
            'insurance_included', 'customer_district_code',
            'customer_province_code',
        ]

    def _payment_columns(self):
        return [
            'batch_id', 'loan_type', 'loaded_at',
            'loan_account_number', 'installment_number',
            'actual_payment_date', 'scheduled_payment_date',
            'installment_amount', 'principal_component',
            'interest_component', 'kkdf_component', 'bsmv_component',
            'installment_status', 'remaining_principal',
            'remaining_interest', 'remaining_kkdf', 'remaining_bsmv',
        ]

    def _prepare_credit_row(self, record: dict, loan_type: str, batch_id: str) -> list:
        now = datetime.utcnow()
        return [
            batch_id,
            loan_type,
            now,
            str(record.get('loan_account_number', '')),
            str(record.get('customer_id', '')),
            str(record.get('customer_type', '')),
            str(record.get('loan_status_code', '')),
            self._to_uint(record.get('days_past_due', 0)),
            record.get('final_maturity_date'),       # already date or None
            self._to_uint(record.get('total_installment_count', 0)),
            self._to_uint(record.get('outstanding_installment_count', 0)),
            self._to_uint(record.get('paid_installment_count', 0)),
            record.get('first_payment_date'),
            self._to_decimal(record.get('original_loan_amount', 0)),
            self._to_decimal(record.get('outstanding_principal_balance', 0)),
            self._to_decimal(record.get('nominal_interest_rate', 0)),
            self._to_decimal(record.get('total_interest_amount', 0)),
            self._to_decimal(record.get('kkdf_rate', 0)),
            self._to_decimal(record.get('kkdf_amount', 0)),
            self._to_decimal(record.get('bsmv_rate', 0)),
            self._to_decimal(record.get('bsmv_amount', 0)),
            self._to_uint(record.get('grace_period_months', 0)),
            self._to_uint(record.get('installment_frequency', 1)),
            record.get('loan_start_date'),
            record.get('loan_closing_date'),
            self._to_nullable_uint(record.get('internal_rating')),
            self._to_nullable_uint(record.get('external_rating')),
            # Commercial-only
            self._to_nullable_uint(record.get('loan_product_type')),
            record.get('customer_region_code') or None,
            self._to_nullable_uint(record.get('sector_code')),
            self._to_nullable_uint(record.get('internal_credit_rating')),
            self._to_nullable_decimal(record.get('default_probability')),
            self._to_nullable_uint(record.get('risk_class')),
            self._to_nullable_uint(record.get('customer_segment')),
            # Retail-only
            self._to_nullable_uint(record.get('insurance_included')),
            record.get('customer_district_code') or None,
            record.get('customer_province_code') or None,
        ]

    def _prepare_payment_row(self, record: dict, loan_type: str, batch_id: str) -> list:
        now = datetime.utcnow()
        return [
            batch_id,
            loan_type,
            now,
            str(record.get('loan_account_number', '')),
            self._to_uint(record.get('installment_number', 0)),
            record.get('actual_payment_date'),
            record.get('scheduled_payment_date'),
            self._to_decimal(record.get('installment_amount', 0)),
            self._to_decimal(record.get('principal_component', 0)),
            self._to_decimal(record.get('interest_component', 0)),
            self._to_decimal(record.get('kkdf_component', 0)),
            self._to_decimal(record.get('bsmv_component', 0)),
            str(record.get('installment_status', '')),
            self._to_decimal(record.get('remaining_principal', 0)),
            self._to_decimal(record.get('remaining_interest', 0)),
            self._to_decimal(record.get('remaining_kkdf', 0)),
            self._to_decimal(record.get('remaining_bsmv', 0)),
        ]

    @staticmethod
    def _to_uint(value) -> int:
        try:
            return max(0, int(value))
        except (ValueError, TypeError):
            return 0

    @staticmethod
    def _to_decimal(value) -> Decimal:
        try:
            return Decimal(str(value))
        except Exception:
            return Decimal('0')

    @staticmethod
    def _to_nullable_uint(value):
        if value is None or value == '' or value == 'None':
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _to_nullable_decimal(value):
        if value is None or value == '' or value == 'None':
            return None
        try:
            return Decimal(str(value))
        except Exception:
            return None
