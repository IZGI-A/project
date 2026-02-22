"""Serializers for the REST API."""
from rest_framework import serializers

from adapter.models import Tenant, SyncLog, SyncConfiguration, ValidationError


class TenantSerializer(serializers.ModelSerializer):
    class Meta:
        model = Tenant
        fields = ['tenant_id', 'name', 'is_active', 'created_at']


class SyncConfigurationSerializer(serializers.ModelSerializer):
    class Meta:
        model = SyncConfiguration
        fields = [
            'id', 'loan_type', 'external_bank_url',
            'sync_interval_minutes', 'is_enabled',
            'last_sync_at', 'last_sync_status', 'created_at',
        ]
        read_only_fields = ['last_sync_at', 'last_sync_status', 'created_at']


class SyncLogSerializer(serializers.ModelSerializer):
    class Meta:
        model = SyncLog
        fields = [
            'id', 'loan_type', 'batch_id', 'status',
            'total_credit_rows', 'total_payment_rows',
            'valid_credit_rows', 'valid_payment_rows',
            'error_count', 'error_summary',
            'started_at', 'completed_at',
        ]


class ValidationErrorSerializer(serializers.ModelSerializer):
    class Meta:
        model = ValidationError
        fields = [
            'id', 'row_number', 'file_type', 'field_name',
            'error_type', 'error_message', 'raw_value',
        ]


class SyncTriggerSerializer(serializers.Serializer):
    loan_type = serializers.ChoiceField(choices=['RETAIL', 'COMMERCIAL'])
