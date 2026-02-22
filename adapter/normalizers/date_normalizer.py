"""Date normalization: converts various date formats to YYYY-MM-DD."""
from datetime import date


class DateNormalizer:
    """
    Normalizes date fields from CSV data.

    Input formats:
        - YYYYMMDD (e.g., '20250302')
        - YYYY-MM-DD (e.g., '2025-03-02')
        - Empty string -> None
    """

    DATE_FIELDS_CREDIT = [
        'final_maturity_date', 'first_payment_date',
        'loan_start_date', 'loan_closing_date',
    ]

    DATE_FIELDS_PAYMENT = [
        'actual_payment_date', 'scheduled_payment_date',
    ]

    def normalize_credit(self, record: dict) -> dict:
        """Normalize date fields in a credit record."""
        for field in self.DATE_FIELDS_CREDIT:
            record[field] = self._normalize_date(record.get(field, ''))
        return record

    def normalize_payment(self, record: dict) -> dict:
        """Normalize date fields in a payment record."""
        for field in self.DATE_FIELDS_PAYMENT:
            record[field] = self._normalize_date(record.get(field, ''))
        return record

    def _normalize_date(self, value: str) -> date | None:
        if not value or not value.strip():
            return None

        value = value.strip()

        # Already YYYY-MM-DD
        if len(value) == 10 and value[4] == '-' and value[7] == '-':
            try:
                return date(int(value[:4]), int(value[5:7]), int(value[8:10]))
            except ValueError:
                return None

        # YYYYMMDD
        clean = value.replace('-', '')
        if len(clean) == 8 and clean.isdigit():
            try:
                return date(int(clean[:4]), int(clean[4:6]), int(clean[6:8]))
            except ValueError:
                return None

        return None
