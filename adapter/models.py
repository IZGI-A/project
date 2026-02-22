import uuid

from django.db import models


class Tenant(models.Model):
    """Tenant registry in the shared financial_shared database."""

    tenant_id = models.CharField(max_length=20, unique=True, db_index=True)
    name = models.CharField(max_length=100)
    api_key_hash = models.CharField(max_length=255)
    api_key_prefix = models.CharField(max_length=16)
    pg_schema = models.CharField(max_length=50)
    ch_database = models.CharField(max_length=50)
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'tenants'

    def __str__(self):
        return f"{self.tenant_id} - {self.name}"


class SyncConfiguration(models.Model):
    """Sync settings per loan_type. Lives in tenant-specific PG DB."""

    LOAN_TYPE_CHOICES = [
        ('RETAIL', 'Retail'),
        ('COMMERCIAL', 'Commercial'),
    ]

    loan_type = models.CharField(max_length=20, choices=LOAN_TYPE_CHOICES, unique=True)
    external_bank_url = models.CharField(max_length=500)
    sync_interval_minutes = models.IntegerField(default=60)
    is_enabled = models.BooleanField(default=True)
    last_sync_at = models.DateTimeField(null=True, blank=True)
    last_sync_status = models.CharField(max_length=20, default='PENDING')
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'sync_configurations'

    def __str__(self):
        return f"{self.loan_type} - {self.last_sync_status}"


class SyncLog(models.Model):
    """Tracks each sync operation. Lives in tenant-specific PG DB."""

    STATUS_CHOICES = [
        ('STARTED', 'Started'),
        ('FETCHING', 'Fetching'),
        ('VALIDATING', 'Validating'),
        ('NORMALIZING', 'Normalizing'),
        ('STORING', 'Storing'),
        ('COMPLETED', 'Completed'),
        ('FAILED', 'Failed'),
    ]

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    loan_type = models.CharField(max_length=20)
    batch_id = models.UUIDField(default=uuid.uuid4)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='STARTED')
    total_credit_rows = models.IntegerField(default=0)
    total_payment_rows = models.IntegerField(default=0)
    valid_credit_rows = models.IntegerField(default=0)
    valid_payment_rows = models.IntegerField(default=0)
    error_count = models.IntegerField(default=0)
    error_summary = models.JSONField(default=dict, blank=True)
    started_at = models.DateTimeField(auto_now_add=True)
    completed_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        db_table = 'sync_logs'
        ordering = ['-started_at']

    def __str__(self):
        return f"Sync {self.id} [{self.loan_type}] - {self.status}"


class ValidationError(models.Model):
    """Individual validation errors. Lives in tenant-specific PG DB."""

    ERROR_TYPE_CHOICES = [
        ('REQUIRED', 'Required field missing'),
        ('TYPE', 'Invalid type'),
        ('RANGE', 'Out of range'),
        ('FORMAT', 'Invalid format'),
        ('VALUE', 'Invalid value'),
        ('CROSS_REFERENCE', 'Cross-reference error'),
    ]

    sync_log = models.ForeignKey(
        SyncLog, on_delete=models.CASCADE, related_name='validation_errors'
    )
    row_number = models.IntegerField()
    file_type = models.CharField(max_length=20)  # 'credit' or 'payment_plan'
    field_name = models.CharField(max_length=100)
    error_type = models.CharField(max_length=50, choices=ERROR_TYPE_CHOICES)
    error_message = models.TextField()
    raw_value = models.TextField(null=True, blank=True)

    class Meta:
        db_table = 'validation_errors'
        indexes = [
            models.Index(fields=['sync_log']),
        ]

    def __str__(self):
        return f"Row {self.row_number}: {self.field_name} - {self.error_type}"
