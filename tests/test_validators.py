"""Tests for field validators and normalizers."""
import pytest
from decimal import Decimal

from adapter.validators.field_validators import CreditFieldValidator, PaymentFieldValidator
from adapter.normalizers.date_normalizer import DateNormalizer
from adapter.normalizers.rate_normalizer import RateNormalizer
from adapter.normalizers.category_normalizer import CategoryNormalizer


class TestCreditFieldValidator:
    def setup_method(self):
        self.validator = CreditFieldValidator()

    def _make_row(self, **overrides):
        base = {
            'loan_account_number': 'LOAN_001',
            'customer_id': 'CUST_001',
            'customer_type': 'I',
            'loan_status_code': 'A',
            'original_loan_amount': '10000',
            'outstanding_principal_balance': '8000',
            'days_past_due': '0',
            'total_installment_count': '12',
            'outstanding_installment_count': '8',
            'paid_installment_count': '4',
            'nominal_interest_rate': '5.14',
            'total_interest_amount': '500',
            'kkdf_rate': '0',
            'kkdf_amount': '0',
            'bsmv_rate': '0',
            'bsmv_amount': '0',
            'grace_period_months': '0',
            'installment_frequency': '1',
            'final_maturity_date': '20260302',
            'first_payment_date': '20250402',
            'loan_start_date': '20250302',
            'loan_closing_date': '',
            'insurance_included': 'H',
            'customer_district_code': 'DISTRICT_A',
            'customer_province_code': 'PROVINCE_1',
            'internal_rating': '2',
            'external_rating': '1366',
        }
        base.update(overrides)
        return base

    def test_valid_retail_row(self):
        row = self._make_row()
        result = self.validator.validate_row(row, 1, 'RETAIL')
        assert result.is_valid
        assert len(result.errors) == 0

    def test_missing_required_field(self):
        row = self._make_row(loan_account_number='')
        result = self.validator.validate_row(row, 1, 'RETAIL')
        assert not result.is_valid
        assert any(e['field_name'] == 'loan_account_number' for e in result.errors)

    def test_invalid_customer_type(self):
        row = self._make_row(customer_type='X')
        result = self.validator.validate_row(row, 1, 'RETAIL')
        assert not result.is_valid
        assert any(e['error_type'] == 'VALUE' for e in result.errors)

    def test_invalid_status_code(self):
        row = self._make_row(loan_status_code='Z')
        result = self.validator.validate_row(row, 1, 'RETAIL')
        assert not result.is_valid

    def test_negative_amount(self):
        row = self._make_row(original_loan_amount='-100')
        result = self.validator.validate_row(row, 1, 'RETAIL')
        assert not result.is_valid
        assert any(e['field_name'] == 'original_loan_amount' for e in result.errors)

    def test_invalid_date_format(self):
        row = self._make_row(final_maturity_date='2025/03/02')
        result = self.validator.validate_row(row, 1, 'RETAIL')
        assert not result.is_valid

    def test_invalid_insurance_retail(self):
        row = self._make_row(insurance_included='X')
        result = self.validator.validate_row(row, 1, 'RETAIL')
        assert not result.is_valid

    def test_commercial_specific_fields(self):
        row = self._make_row(
            customer_type='T',
            loan_product_type='4',
            sector_code='3',
            internal_credit_rating='5',
            default_probability='0.02',
            risk_class='1',
            customer_segment='2',
        )
        result = self.validator.validate_row(row, 1, 'COMMERCIAL')
        assert result.is_valid


class TestPaymentFieldValidator:
    def setup_method(self):
        self.validator = PaymentFieldValidator()

    def _make_row(self, **overrides):
        base = {
            'loan_account_number': 'LOAN_001',
            'installment_number': '1',
            'actual_payment_date': '20250208',
            'scheduled_payment_date': '2025-02-08',
            'installment_amount': '17790',
            'principal_component': '13640',
            'interest_component': '4281.23',
            'kkdf_component': '727.56',
            'bsmv_component': '651.22',
            'installment_status': 'K',
            'remaining_principal': '0',
            'remaining_interest': '0',
            'remaining_kkdf': '0',
            'remaining_bsmv': '0',
        }
        base.update(overrides)
        return base

    def test_valid_row(self):
        row = self._make_row()
        result = self.validator.validate_row(row, 1, 'RETAIL')
        assert result.is_valid

    def test_missing_loan_account(self):
        row = self._make_row(loan_account_number='')
        result = self.validator.validate_row(row, 1, 'RETAIL')
        assert not result.is_valid

    def test_invalid_installment_number(self):
        row = self._make_row(installment_number='0')
        result = self.validator.validate_row(row, 1, 'RETAIL')
        assert not result.is_valid

    def test_invalid_status(self):
        row = self._make_row(installment_status='X')
        result = self.validator.validate_row(row, 1, 'RETAIL')
        assert not result.is_valid


class TestDateNormalizer:
    def setup_method(self):
        self.normalizer = DateNormalizer()

    def test_yyyymmdd(self):
        from datetime import date
        record = {'final_maturity_date': '20260302', 'first_payment_date': '',
                  'loan_start_date': '', 'loan_closing_date': ''}
        result = self.normalizer.normalize_credit(record)
        assert result['final_maturity_date'] == date(2026, 3, 2)

    def test_yyyy_mm_dd(self):
        from datetime import date
        record = {'actual_payment_date': '2025-02-08', 'scheduled_payment_date': ''}
        result = self.normalizer.normalize_payment(record)
        assert result['actual_payment_date'] == date(2025, 2, 8)

    def test_empty_date(self):
        record = {'final_maturity_date': '', 'first_payment_date': '',
                  'loan_start_date': '', 'loan_closing_date': ''}
        result = self.normalizer.normalize_credit(record)
        assert result['final_maturity_date'] is None

    def test_invalid_date(self):
        record = {'final_maturity_date': 'abc', 'first_payment_date': '',
                  'loan_start_date': '', 'loan_closing_date': ''}
        result = self.normalizer.normalize_credit(record)
        assert result['final_maturity_date'] is None


class TestRateNormalizer:
    def setup_method(self):
        self.normalizer = RateNormalizer()

    def test_percentage_rate_divided(self):
        record = {'nominal_interest_rate': '55.47', 'kkdf_rate': '15.14', 'bsmv_rate': '15.27'}
        result = self.normalizer.normalize_credit(record, 'RETAIL')
        assert result['nominal_interest_rate'] == Decimal('55.47') / 100
        assert result['kkdf_rate'] == Decimal('15.14') / 100

    def test_already_decimal(self):
        record = {'nominal_interest_rate': '0.0514', 'kkdf_rate': '0', 'bsmv_rate': '0.05'}
        result = self.normalizer.normalize_credit(record, 'RETAIL')
        assert result['nominal_interest_rate'] == Decimal('0.0514')
        assert result['bsmv_rate'] == Decimal('0.05')

    def test_zero_rate(self):
        record = {'nominal_interest_rate': '0', 'kkdf_rate': '0', 'bsmv_rate': '0'}
        result = self.normalizer.normalize_credit(record, 'RETAIL')
        assert result['nominal_interest_rate'] == Decimal('0')

    def test_empty_rate(self):
        record = {'nominal_interest_rate': '', 'kkdf_rate': '', 'bsmv_rate': ''}
        result = self.normalizer.normalize_credit(record, 'RETAIL')
        assert result['nominal_interest_rate'] == Decimal('0')

    def test_commercial_default_probability(self):
        record = {
            'nominal_interest_rate': '5.13',
            'kkdf_rate': '0',
            'bsmv_rate': '5.03',
            'default_probability': '0.0217',
        }
        result = self.normalizer.normalize_credit(record, 'COMMERCIAL')
        assert result['nominal_interest_rate'] == Decimal('5.13') / 100
        assert result['default_probability'] == Decimal('0.0217')


class TestCategoryNormalizer:
    def setup_method(self):
        self.normalizer = CategoryNormalizer()

    def test_customer_type_mapping(self):
        for code, expected in [('I', 'INDIVIDUAL'), ('T', 'TRADE'), ('V', 'VIP')]:
            record = {'customer_type': code, 'loan_status_code': 'A'}
            result = self.normalizer.normalize_credit(record, 'RETAIL')
            assert result['customer_type'] == expected

    def test_status_mapping(self):
        record = {'customer_type': 'I', 'loan_status_code': 'K'}
        result = self.normalizer.normalize_credit(record, 'RETAIL')
        assert result['loan_status_code'] == 'CLOSED'

    def test_insurance_mapping(self):
        for code, expected in [('H', 0), ('E', 1)]:
            record = {'customer_type': 'I', 'loan_status_code': 'A',
                      'insurance_included': code}
            result = self.normalizer.normalize_credit(record, 'RETAIL')
            assert result['insurance_included'] == expected

    def test_payment_status_mapping(self):
        for code, expected in [('A', 'ACTIVE'), ('K', 'CLOSED')]:
            record = {'installment_status': code}
            result = self.normalizer.normalize_payment(record)
            assert result['installment_status'] == expected

    def test_commercial_status_flag_removed(self):
        record = {'customer_type': 'T', 'loan_status_code': 'A', 'loan_status_flag': 'A'}
        result = self.normalizer.normalize_credit(record, 'COMMERCIAL')
        assert 'loan_status_flag' not in result
