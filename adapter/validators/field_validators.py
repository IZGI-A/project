"""Field-level validators for credit and payment plan data."""
from .base import BaseValidator, ValidationResult


class CreditFieldValidator(BaseValidator):
    """Validates individual fields in credit records."""

    COMMON_REQUIRED = [
        'loan_account_number', 'customer_id', 'customer_type',
        'loan_status_code', 'original_loan_amount',
        'outstanding_principal_balance',
    ]

    VALID_CUSTOMER_TYPES = {'I', 'T', 'V'}
    VALID_STATUS_CODES = {'A', 'K'}

    def validate_row(self, row: dict, row_number: int, loan_type: str) -> ValidationResult:
        result = ValidationResult(row_number=row_number)

        # Required fields
        for field_name in self.COMMON_REQUIRED:
            self.validate_required(result, row, field_name, 'credit')

        # Customer type
        self.validate_in_set(result, row, 'customer_type', self.VALID_CUSTOMER_TYPES)

        # Loan status code
        self.validate_in_set(result, row, 'loan_status_code', self.VALID_STATUS_CODES)

        # Numeric fields
        self.validate_decimal(result, row, 'original_loan_amount', min_val=0)
        self.validate_decimal(result, row, 'outstanding_principal_balance', min_val=0)
        self.validate_decimal(result, row, 'nominal_interest_rate', min_val=0)
        self.validate_decimal(result, row, 'total_interest_amount', min_val=0)
        self.validate_decimal(result, row, 'kkdf_rate', min_val=0)
        self.validate_decimal(result, row, 'kkdf_amount', min_val=0)
        self.validate_decimal(result, row, 'bsmv_rate', min_val=0)
        self.validate_decimal(result, row, 'bsmv_amount', min_val=0)

        # Integer fields
        self.validate_integer(result, row, 'days_past_due', min_val=0)
        self.validate_integer(result, row, 'total_installment_count', min_val=0)
        self.validate_integer(result, row, 'outstanding_installment_count', min_val=0)
        self.validate_integer(result, row, 'paid_installment_count', min_val=0)
        self.validate_integer(result, row, 'grace_period_months', min_val=0)
        self.validate_integer(result, row, 'installment_frequency', min_val=0)
        self.validate_integer(result, row, 'internal_rating')
        self.validate_integer(result, row, 'external_rating')

        # Date fields
        self.validate_date(result, row, 'final_maturity_date')
        self.validate_date(result, row, 'first_payment_date')
        self.validate_date(result, row, 'loan_start_date')
        self.validate_date(result, row, 'loan_closing_date')

        # Retail-specific
        if loan_type == 'RETAIL':
            insurance = row.get('insurance_included', '').strip()
            if insurance and insurance not in ('H', 'E'):
                result.add_error(
                    'insurance_included', 'VALUE',
                    f'insurance_included must be H or E, got: {insurance}',
                    raw_value=insurance,
                )

        # Commercial-specific
        if loan_type == 'COMMERCIAL':
            self.validate_integer(result, row, 'loan_product_type')
            self.validate_integer(result, row, 'sector_code')
            self.validate_integer(result, row, 'internal_credit_rating')
            self.validate_decimal(result, row, 'default_probability', min_val=0)
            self.validate_integer(result, row, 'risk_class')
            self.validate_integer(result, row, 'customer_segment')
            self.validate_in_set(result, row, 'loan_status_flag', {'A', 'K'})

        return result


class PaymentFieldValidator(BaseValidator):
    """Validates individual fields in payment plan records."""

    REQUIRED_FIELDS = [
        'loan_account_number', 'installment_number',
        'installment_amount', 'principal_component',
    ]

    VALID_STATUSES = {'A', 'K'}

    def validate_row(self, row: dict, row_number: int, loan_type: str) -> ValidationResult:
        result = ValidationResult(row_number=row_number)

        # Required fields
        for field_name in self.REQUIRED_FIELDS:
            self.validate_required(result, row, field_name, 'payment_plan')

        # Installment number
        self.validate_integer(result, row, 'installment_number', min_val=1)

        # Amount fields
        self.validate_decimal(result, row, 'installment_amount', min_val=0)
        self.validate_decimal(result, row, 'principal_component', min_val=0)
        self.validate_decimal(result, row, 'interest_component', min_val=0)
        self.validate_decimal(result, row, 'kkdf_component', min_val=0)
        self.validate_decimal(result, row, 'bsmv_component', min_val=0)
        self.validate_decimal(result, row, 'remaining_principal', min_val=0)
        self.validate_decimal(result, row, 'remaining_interest', min_val=0)
        self.validate_decimal(result, row, 'remaining_kkdf', min_val=0)
        self.validate_decimal(result, row, 'remaining_bsmv', min_val=0)

        # Status
        self.validate_in_set(result, row, 'installment_status', self.VALID_STATUSES)

        # Dates
        self.validate_date(result, row, 'actual_payment_date')
        self.validate_date(result, row, 'scheduled_payment_date')

        return result
