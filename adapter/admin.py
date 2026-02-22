from django.contrib import admin

from .models import Tenant, SyncConfiguration, SyncLog, ValidationError


@admin.register(Tenant)
class TenantAdmin(admin.ModelAdmin):
    list_display = ('tenant_id', 'name', 'pg_schema', 'ch_database', 'is_active', 'created_at')
    list_filter = ('is_active',)
    search_fields = ('tenant_id', 'name')


@admin.register(SyncConfiguration)
class SyncConfigurationAdmin(admin.ModelAdmin):
    list_display = ('loan_type', 'external_bank_url', 'is_enabled',
                    'last_sync_status', 'last_sync_at')
    list_filter = ('loan_type', 'is_enabled', 'last_sync_status')


@admin.register(SyncLog)
class SyncLogAdmin(admin.ModelAdmin):
    list_display = ('id', 'loan_type', 'status', 'total_credit_rows',
                    'valid_credit_rows', 'error_count', 'started_at')
    list_filter = ('loan_type', 'status')
    readonly_fields = ('id', 'batch_id', 'started_at')


@admin.register(ValidationError)
class ValidationErrorAdmin(admin.ModelAdmin):
    list_display = ('sync_log', 'row_number', 'file_type', 'field_name',
                    'error_type', 'raw_value')
    list_filter = ('file_type', 'error_type')
    search_fields = ('field_name', 'error_message')
