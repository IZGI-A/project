"""Cross-file validation: ensures referential integrity between credits and payments."""
import logging

from adapter.clickhouse_manager import get_clickhouse_client
from .base import ValidationResult, BatchValidationResult

logger = logging.getLogger(__name__)


class CrossFileValidator:
    """
    Validates payment records against credit records.

    valid_loans = batch_credits UNION clickhouse_existing_credits

    Payment loan_account_numbers must exist in this combined set.
    """

    def validate(self, valid_credits: list, payment_records: list,
                 ch_database: str, loan_type: str) -> BatchValidationResult:
        """
        Validate payment records against known credit loan_account_numbers.

        Args:
            valid_credits: List of validated credit records from the current batch
            payment_records: List of payment records to validate
            ch_database: ClickHouse database name for existing credits
            loan_type: RETAIL or COMMERCIAL
        """
        result = BatchValidationResult()

        # Build set of valid loan account numbers from current batch
        batch_loans = {
            r.get('loan_account_number', '').strip()
            for r in valid_credits
            if r.get('loan_account_number', '').strip()
        }

        # Get existing loan account numbers from ClickHouse
        existing_loans = self._get_existing_loans(ch_database, loan_type)

        # Union of both
        valid_loans = batch_loans | existing_loans

        for idx, row in enumerate(payment_records, start=1):
            vr = ValidationResult(row_number=idx)
            loan_num = row.get('loan_account_number', '').strip()

            if loan_num and loan_num not in valid_loans:
                vr.add_error(
                    'loan_account_number', 'CROSS_REFERENCE',
                    f'Payment references non-existent credit: {loan_num}',
                    raw_value=loan_num,
                )

            result.add_row_result(vr, row)

        return result

    def _get_existing_loans(self, ch_database: str, loan_type: str) -> set:
        """Fetch existing loan_account_numbers from ClickHouse fact_credit."""
        try:
            client = get_clickhouse_client(database=ch_database)
            query_result = client.query(
                "SELECT DISTINCT loan_account_number "
                "FROM fact_credit "
                "WHERE loan_type = {loan_type:String}",
                parameters={'loan_type': loan_type},
            )
            return {row[0] for row in query_result.result_rows}
        except Exception as e:
            logger.warning(
                "Could not fetch existing loans from ClickHouse (%s): %s. "
                "Cross-validation will use batch credits only.",
                ch_database, e,
            )
            return set()
