"""Tests for cross-file validation logic."""
import pytest
from unittest.mock import patch

from adapter.validators.cross_validators import CrossFileValidator


class TestCrossFileValidator:
    def setup_method(self):
        self.validator = CrossFileValidator()

    @patch.object(CrossFileValidator, '_get_existing_loans', return_value=set())
    def test_valid_cross_reference(self, mock_ch):
        credits = [
            {'loan_account_number': 'LOAN_001'},
            {'loan_account_number': 'LOAN_002'},
        ]
        payments = [
            {'loan_account_number': 'LOAN_001', 'installment_number': '1'},
            {'loan_account_number': 'LOAN_002', 'installment_number': '1'},
        ]

        result = self.validator.validate(credits, payments, 'bank001_dw', 'RETAIL')
        assert result.valid_rows == 2
        assert result.error_count == 0

    @patch.object(CrossFileValidator, '_get_existing_loans', return_value=set())
    def test_invalid_cross_reference(self, mock_ch):
        credits = [{'loan_account_number': 'LOAN_001'}]
        payments = [
            {'loan_account_number': 'LOAN_001', 'installment_number': '1'},
            {'loan_account_number': 'LOAN_999', 'installment_number': '1'},
        ]

        result = self.validator.validate(credits, payments, 'bank001_dw', 'RETAIL')
        assert result.valid_rows == 1
        assert result.error_count == 1
        assert result.errors[0]['error_type'] == 'CROSS_REFERENCE'

    @patch.object(CrossFileValidator, '_get_existing_loans',
                  return_value={'LOAN_OLD_001', 'LOAN_OLD_002'})
    def test_existing_clickhouse_credits(self, mock_ch):
        """Payments can reference credits already in ClickHouse."""
        credits = [{'loan_account_number': 'LOAN_NEW_001'}]
        payments = [
            {'loan_account_number': 'LOAN_NEW_001', 'installment_number': '1'},
            {'loan_account_number': 'LOAN_OLD_001', 'installment_number': '1'},
            {'loan_account_number': 'LOAN_UNKNOWN', 'installment_number': '1'},
        ]

        result = self.validator.validate(credits, payments, 'bank001_dw', 'RETAIL')
        assert result.valid_rows == 2  # NEW_001 + OLD_001
        assert result.error_count == 1  # LOAN_UNKNOWN

    @patch.object(CrossFileValidator, '_get_existing_loans', return_value=set())
    def test_empty_payments(self, mock_ch):
        credits = [{'loan_account_number': 'LOAN_001'}]
        result = self.validator.validate(credits, [], 'bank001_dw', 'RETAIL')
        assert result.total_rows == 0
        assert result.error_count == 0
