"""
Data Profiling Engine - Real-time ClickHouse queries.

No separate cache table. ClickHouse computes min/max/avg/stddev
directly from fact tables in milliseconds.
"""
import logging

from adapter.clickhouse_manager import get_clickhouse_client

logger = logging.getLogger(__name__)


class ProfilingEngine:
    """Runs profiling queries against ClickHouse fact tables."""

    # Meta fields excluded from completeness analysis
    META_FIELDS = ['batch_id', 'loan_type', 'loaded_at']

    # Loan-type specific columns (used to exclude irrelevant fields)
    RETAIL_ONLY_FIELDS = {
        'insurance_included', 'customer_district_code', 'customer_province_code',
    }
    COMMERCIAL_ONLY_FIELDS = {
        'loan_product_type', 'customer_region_code', 'sector_code',
        'internal_credit_rating', 'default_probability',
        'risk_class', 'customer_segment',
    }

    NUMERIC_FIELDS_CREDIT = [
        'days_past_due', 'total_installment_count',
        'outstanding_installment_count', 'paid_installment_count',
        'original_loan_amount', 'outstanding_principal_balance',
        'nominal_interest_rate', 'total_interest_amount',
        'kkdf_rate', 'kkdf_amount', 'bsmv_rate', 'bsmv_amount',
        'internal_rating', 'external_rating',
    ]

    CATEGORICAL_FIELDS_CREDIT_COMMON = [
        'customer_type', 'loan_status_code',
        'installment_frequency', 'grace_period_months',
    ]
    CATEGORICAL_FIELDS_CREDIT_RETAIL = [
        'insurance_included',
        'customer_district_code', 'customer_province_code',
    ]
    CATEGORICAL_FIELDS_CREDIT_COMMERCIAL = [
        'loan_product_type', 'sector_code', 'risk_class',
        'customer_segment', 'internal_credit_rating',
        'customer_region_code',
    ]

    NULLABLE_FIELDS_CREDIT_COMMON = [
        'final_maturity_date', 'first_payment_date',
        'loan_start_date', 'loan_closing_date',
        'internal_rating', 'external_rating',
    ]
    NULLABLE_FIELDS_CREDIT_RETAIL = [
        'insurance_included',
        'customer_district_code', 'customer_province_code',
    ]
    NULLABLE_FIELDS_CREDIT_COMMERCIAL = [
        'loan_product_type', 'customer_region_code',
        'sector_code', 'internal_credit_rating',
        'default_probability', 'risk_class', 'customer_segment',
    ]

    NUMERIC_FIELDS_PAYMENT = [
        'installment_number', 'installment_amount',
        'principal_component', 'interest_component',
        'kkdf_component', 'bsmv_component',
        'remaining_principal', 'remaining_interest',
        'remaining_kkdf', 'remaining_bsmv',
    ]

    CATEGORICAL_FIELDS_PAYMENT = [
        'installment_status',
    ]

    NULLABLE_FIELDS_PAYMENT = [
        'actual_payment_date', 'scheduled_payment_date',
    ]

    def __init__(self, ch_database: str):
        self.ch_database = ch_database

    def profile(self, loan_type: str, data_type: str = 'credit') -> dict:
        """
        Generate a data profile for the given loan_type and data_type.

        Returns:
            dict with numeric_stats, categorical_stats, null_ratios, row_count
        """
        table = 'fact_credit' if data_type == 'credit' else 'fact_payment'

        if data_type == 'credit':
            numeric_fields = self.NUMERIC_FIELDS_CREDIT
            categorical_fields = list(self.CATEGORICAL_FIELDS_CREDIT_COMMON)
            nullable_fields = list(self.NULLABLE_FIELDS_CREDIT_COMMON)
            if loan_type == 'RETAIL':
                categorical_fields += self.CATEGORICAL_FIELDS_CREDIT_RETAIL
                nullable_fields += self.NULLABLE_FIELDS_CREDIT_RETAIL
            else:
                categorical_fields += self.CATEGORICAL_FIELDS_CREDIT_COMMERCIAL
                nullable_fields += self.NULLABLE_FIELDS_CREDIT_COMMERCIAL
        else:
            numeric_fields = self.NUMERIC_FIELDS_PAYMENT
            categorical_fields = self.CATEGORICAL_FIELDS_PAYMENT
            nullable_fields = self.NULLABLE_FIELDS_PAYMENT

        client = get_clickhouse_client(database=self.ch_database)

        result = {
            'loan_type': loan_type,
            'data_type': data_type,
            'row_count': self._get_row_count(client, table, loan_type),
            'numeric_stats': self._get_numeric_stats(client, table, loan_type, numeric_fields),
            'categorical_stats': self._get_categorical_stats(
                client, table, loan_type, categorical_fields,
            ),
            'null_ratios': self._get_null_ratios(
                client, table, loan_type, nullable_fields,
            ),
            'completeness': self._get_completeness(
                client, table, loan_type, data_type, numeric_fields,
            ),
        }

        return result

    def _get_row_count(self, client, table: str, loan_type: str) -> int:
        result = client.query(
            f"SELECT count() FROM {table} WHERE loan_type = {{loan_type:String}}",
            parameters={'loan_type': loan_type},
        )
        return result.result_rows[0][0]

    def _get_numeric_stats(self, client, table: str, loan_type: str,
                           fields: list) -> dict:
        stats = {}
        if not fields:
            return stats

        select_parts = []
        for field in fields:
            select_parts.extend([
                f"min({field})",
                f"max({field})",
                f"avg({field})",
                f"stddevPop({field})",
                f"count()",
                f"countIf({field} = 0 OR isNull({field}))",
            ])

        query = (
            f"SELECT {', '.join(select_parts)} "
            f"FROM {table} "
            f"WHERE loan_type = {{loan_type:String}}"
        )

        result = client.query(query, parameters={'loan_type': loan_type})
        if not result.result_rows:
            return stats

        row = result.result_rows[0]
        idx = 0
        for field in fields:
            total = row[idx + 4] if row[idx + 4] else 1
            stats[field] = {
                'min': self._to_float(row[idx]),
                'max': self._to_float(row[idx + 1]),
                'avg': self._to_float(row[idx + 2]),
                'stddev': self._to_float(row[idx + 3]),
                'total_count': row[idx + 4],
                'zero_or_null_count': row[idx + 5],
                'zero_or_null_ratio': round(row[idx + 5] / total, 4) if total else 0,
            }
            idx += 6

        return stats

    def _get_categorical_stats(self, client, table: str, loan_type: str,
                               fields: list) -> dict:
        stats = {}
        for field in fields:
            # Get all values (no limit)
            query = (
                f"SELECT toString({field}) AS value, count() AS frequency "
                f"FROM {table} "
                f"WHERE loan_type = {{loan_type:String}} "
                f"GROUP BY value "
                f"ORDER BY frequency DESC"
            )
            result = client.query(query, parameters={'loan_type': loan_type})
            values = [
                {'value': row[0] if row[0] else None, 'frequency': row[1]}
                for row in result.result_rows
            ]
            # Skip fields where all values are NULL
            non_null = [v for v in values if v['value'] is not None]
            if non_null:
                stats[field] = {
                    'unique_count': len(non_null),
                    'values': values,
                }
        return stats

    def _get_null_ratios(self, client, table: str, loan_type: str,
                         fields: list) -> dict:
        if not fields:
            return {}

        select_parts = ["count()"]
        for field in fields:
            select_parts.append(f"countIf(isNull({field})) / count()")

        query = (
            f"SELECT {', '.join(select_parts)} "
            f"FROM {table} "
            f"WHERE loan_type = {{loan_type:String}}"
        )
        result = client.query(query, parameters={'loan_type': loan_type})
        if not result.result_rows:
            return {}

        row = result.result_rows[0]
        ratios = {}
        for i, field in enumerate(fields):
            ratios[field] = round(self._to_float(row[i + 1]), 4)
        return ratios

    def _get_completeness(self, client, table: str, loan_type: str,
                          data_type: str, numeric_fields: list) -> dict:
        """Get missing data ratio for non-numeric fields, filtered by loan type."""
        # Get all columns from the table
        col_result = client.query(
            "SELECT name, type FROM system.columns "
            "WHERE database = currentDatabase() AND table = {table:String} "
            "ORDER BY position",
            parameters={'table': table},
        )
        if not col_result.result_rows:
            return {}

        # Determine which fields to exclude (loan-type specific)
        exclude = set(self.META_FIELDS) | set(numeric_fields)
        if data_type == 'credit':
            if loan_type == 'RETAIL':
                exclude |= self.COMMERCIAL_ONLY_FIELDS
            else:
                exclude |= self.RETAIL_ONLY_FIELDS

        columns = []
        for name, col_type in col_result.result_rows:
            if name in exclude:
                continue
            columns.append((name, col_type))

        if not columns:
            return {}

        # Build query: for each column check null/empty based on type
        select_parts = ["count()"]
        for name, col_type in columns:
            if 'Nullable' in col_type:
                select_parts.append(f"countIf(isNull({name}))")
            elif 'String' in col_type:
                select_parts.append(f"countIf({name} = '')")
            else:
                # Non-nullable numeric: always filled, missing = 0
                select_parts.append("0")

        query = (
            f"SELECT {', '.join(select_parts)} "
            f"FROM {table} "
            f"WHERE loan_type = {{loan_type:String}}"
        )
        result = client.query(query, parameters={'loan_type': loan_type})
        if not result.result_rows:
            return {}

        row = result.result_rows[0]
        total = row[0] if row[0] else 1
        completeness = {}
        for i, (name, col_type) in enumerate(columns):
            missing = row[i + 1]
            completeness[name] = {
                'missing_count': missing,
                'missing_pct': round((missing / total) * 100, 2) if total else 0,
                'filled_pct': round(((total - missing) / total) * 100, 2) if total else 0,
                'total': total,
            }
        return completeness

    @staticmethod
    def _to_float(value):
        try:
            return float(value) if value is not None else 0.0
        except (ValueError, TypeError):
            return 0.0
