"""Rate normalization: ensures all rates are in decimal form (0.0 - 1.0)."""
from decimal import Decimal, InvalidOperation


class RateNormalizer:
    """
    Normalizes rate fields from CSV data.

    If rate > 1.0, it's a percentage and should be divided by 100.
    e.g., 55.47 -> 0.5547, 5.14 -> 0.0514, 0.0217 -> 0.0217
    """

    RATE_FIELDS = [
        'nominal_interest_rate', 'kkdf_rate', 'bsmv_rate',
    ]

    RATE_FIELDS_COMMERCIAL = RATE_FIELDS + ['default_probability']

    def normalize_credit(self, record: dict, loan_type: str) -> dict:
        """Normalize rate fields in a credit record."""
        fields = self.RATE_FIELDS_COMMERCIAL if loan_type == 'COMMERCIAL' else self.RATE_FIELDS
        for field in fields:
            record[field] = self._normalize_rate(record.get(field, ''))
        return record

    def _normalize_rate(self, value: str) -> Decimal:
        if not value or not str(value).strip():
            return Decimal('0')

        try:
            rate = Decimal(str(value).strip())
            if rate > 1:
                rate = rate / 100
            return rate
        except (InvalidOperation, ValueError):
            return Decimal('0')
