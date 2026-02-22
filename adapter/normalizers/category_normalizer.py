"""Category normalization: maps coded values to standardized labels."""


class CategoryNormalizer:
    """
    Normalizes category fields from CSV data.

    Mappings:
        customer_type: I -> INDIVIDUAL, T -> TRADE, V -> VIP
        loan_status_code: A -> ACTIVE, K -> CLOSED
        installment_status: A -> ACTIVE, K -> CLOSED
        insurance_included: H -> 0, E -> 1
    """

    CUSTOMER_TYPE_MAP = {
        'I': 'INDIVIDUAL',
        'T': 'TRADE',
        'V': 'VIP',
    }

    STATUS_MAP = {
        'A': 'ACTIVE',
        'K': 'CLOSED',
    }

    INSURANCE_MAP = {
        'H': 0,
        'E': 1,
    }

    def normalize_credit(self, record: dict, loan_type: str) -> dict:
        """Normalize category fields in a credit record."""
        # Customer type
        raw_ct = record.get('customer_type', '').strip()
        record['customer_type'] = self.CUSTOMER_TYPE_MAP.get(raw_ct, raw_ct)

        # Loan status code
        raw_status = record.get('loan_status_code', '').strip()
        record['loan_status_code'] = self.STATUS_MAP.get(raw_status, raw_status)

        # Insurance (retail only)
        if loan_type == 'RETAIL':
            raw_ins = record.get('insurance_included', '').strip()
            record['insurance_included'] = self.INSURANCE_MAP.get(raw_ins, None)

        # Commercial loan_status_flag
        if loan_type == 'COMMERCIAL':
            raw_flag = record.get('loan_status_flag', '').strip()
            record['loan_status_flag'] = self.STATUS_MAP.get(raw_flag, raw_flag)

        return record

    def normalize_payment(self, record: dict) -> dict:
        """Normalize category fields in a payment record."""
        raw_status = record.get('installment_status', '').strip()
        record['installment_status'] = self.STATUS_MAP.get(raw_status, raw_status)
        return record
