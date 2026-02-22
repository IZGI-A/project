"""Tests for external bank in-memory storage."""
import pytest
from external_bank import storage


class TestExternalBankStorage:
    def setup_method(self):
        storage.clear_data()

    def teardown_method(self):
        storage.clear_data()

    def test_store_and_retrieve(self):
        records = [{'loan_account_number': 'LOAN_001', 'amount': '1000'}]
        storage.store_data('BANK001', 'RETAIL', 'credit', records)

        result = storage.get_data('BANK001', 'RETAIL', 'credit')
        assert len(result) == 1
        assert result[0]['loan_account_number'] == 'LOAN_001'

    def test_tenant_isolation(self):
        records_1 = [{'loan_account_number': 'LOAN_001'}]
        records_2 = [{'loan_account_number': 'LOAN_002'}]

        storage.store_data('BANK001', 'RETAIL', 'credit', records_1)
        storage.store_data('BANK002', 'RETAIL', 'credit', records_2)

        result_1 = storage.get_data('BANK001', 'RETAIL', 'credit')
        result_2 = storage.get_data('BANK002', 'RETAIL', 'credit')

        assert len(result_1) == 1
        assert result_1[0]['loan_account_number'] == 'LOAN_001'
        assert len(result_2) == 1
        assert result_2[0]['loan_account_number'] == 'LOAN_002'

    def test_loan_type_isolation(self):
        retail = [{'id': 'R1'}]
        commercial = [{'id': 'C1'}, {'id': 'C2'}]

        storage.store_data('BANK001', 'RETAIL', 'credit', retail)
        storage.store_data('BANK001', 'COMMERCIAL', 'credit', commercial)

        assert len(storage.get_data('BANK001', 'RETAIL', 'credit')) == 1
        assert len(storage.get_data('BANK001', 'COMMERCIAL', 'credit')) == 2

    def test_replace_on_store(self):
        storage.store_data('BANK001', 'RETAIL', 'credit', [{'v': '1'}] * 100)
        assert storage.get_row_count('BANK001', 'RETAIL', 'credit') == 100

        storage.store_data('BANK001', 'RETAIL', 'credit', [{'v': '2'}] * 200)
        assert storage.get_row_count('BANK001', 'RETAIL', 'credit') == 200

    def test_empty_data(self):
        result = storage.get_data('BANK999', 'RETAIL', 'credit')
        assert result == []

    def test_clear_specific(self):
        storage.store_data('BANK001', 'RETAIL', 'credit', [{'a': '1'}])
        storage.store_data('BANK001', 'RETAIL', 'payment_plan', [{'b': '2'}])
        storage.clear_data('BANK001', 'RETAIL', 'credit')

        assert storage.get_data('BANK001', 'RETAIL', 'credit') == []
        assert len(storage.get_data('BANK001', 'RETAIL', 'payment_plan')) == 1

    def test_clear_tenant(self):
        storage.store_data('BANK001', 'RETAIL', 'credit', [{'a': '1'}])
        storage.store_data('BANK001', 'COMMERCIAL', 'credit', [{'b': '2'}])
        storage.store_data('BANK002', 'RETAIL', 'credit', [{'c': '3'}])

        storage.clear_data('BANK001')

        assert storage.get_data('BANK001', 'RETAIL', 'credit') == []
        assert storage.get_data('BANK001', 'COMMERCIAL', 'credit') == []
        assert len(storage.get_data('BANK002', 'RETAIL', 'credit')) == 1

    def test_list_keys(self):
        storage.store_data('BANK001', 'RETAIL', 'credit', [{'a': '1'}])
        storage.store_data('BANK001', 'COMMERCIAL', 'payment_plan', [{'b': '2'}])

        keys = storage.list_keys()
        assert 'BANK001:RETAIL:credit' in keys
        assert 'BANK001:COMMERCIAL:payment_plan' in keys
