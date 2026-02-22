import secrets

from django.contrib.auth.hashers import make_password
from django.core.management.base import BaseCommand

from adapter.models import Tenant, SyncConfiguration
from config.db_router import set_current_tenant_schema, clear_current_tenant_schema


TENANTS = [
    {
        'tenant_id': 'BANK001',
        'name': 'Bank 001',
        'pg_schema': 'bank001',
        'ch_database': 'bank001_dw',
    },
    {
        'tenant_id': 'BANK002',
        'name': 'Bank 002',
        'pg_schema': 'bank002',
        'ch_database': 'bank002_dw',
    },
    {
        'tenant_id': 'BANK003',
        'name': 'Bank 003',
        'pg_schema': 'bank003',
        'ch_database': 'bank003_dw',
    },
]

EXTERNAL_BANK_URL = 'http://web:8000/bank/api'


class Command(BaseCommand):
    help = 'Seed the 3 tenants (BANK001, BANK002, BANK003) with API keys and sync configs'

    def handle(self, *args, **options):
        for tenant_data in TENANTS:
            tenant, created = Tenant.objects.get_or_create(
                tenant_id=tenant_data['tenant_id'],
                defaults={
                    'name': tenant_data['name'],
                    'pg_schema': tenant_data['pg_schema'],
                    'ch_database': tenant_data['ch_database'],
                    'api_key_hash': '',
                    'api_key_prefix': '',
                },
            )

            if created:
                raw_api_key = f"sk_live_{secrets.token_hex(24)}"
                tenant.api_key_hash = make_password(raw_api_key)
                tenant.api_key_prefix = raw_api_key[:16]
                tenant.save()
                self.stdout.write(
                    self.style.SUCCESS(
                        f"Created {tenant.tenant_id}: API key = {raw_api_key}"
                    )
                )
                self.stdout.write(
                    self.style.WARNING(
                        "  Save this API key - it cannot be retrieved later!"
                    )
                )
            else:
                self.stdout.write(
                    self.style.NOTICE(
                        f"Tenant {tenant.tenant_id} already exists (prefix: {tenant.api_key_prefix}...)"
                    )
                )

            # Create sync configurations in the tenant's schema
            self._create_sync_configs(tenant)

        self.stdout.write(self.style.SUCCESS("Tenant seeding complete."))

    def _create_sync_configs(self, tenant):
        """Create RETAIL and COMMERCIAL sync configs in the tenant's schema."""
        set_current_tenant_schema(tenant.pg_schema)
        try:
            for loan_type in ['RETAIL', 'COMMERCIAL']:
                _, created = SyncConfiguration.objects.get_or_create(
                    loan_type=loan_type,
                    defaults={
                        'external_bank_url': EXTERNAL_BANK_URL,
                        'sync_interval_minutes': 60,
                        'is_enabled': True,
                    },
                )
                if created:
                    self.stdout.write(
                        f"  Created {loan_type} sync config for {tenant.tenant_id}"
                    )
        finally:
            clear_current_tenant_schema()
