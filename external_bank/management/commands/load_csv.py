"""
Management command to load CSV files into the external bank in-memory storage.

Usage:
    python manage.py load_csv --tenant_id BANK001 --loan_type RETAIL --file_type credit --file path/to/file.csv
    python manage.py load_csv --tenant_id BANK001 --loan_type RETAIL --file_type payment_plan --file path/to/file.csv
    python manage.py load_csv --all   # Load all sample data for all tenants
"""
import csv
import os

from django.core.management.base import BaseCommand

from external_bank import storage


class Command(BaseCommand):
    help = 'Load CSV files into the external bank in-memory storage'

    def add_arguments(self, parser):
        parser.add_argument('--tenant_id', type=str, help='Tenant ID (e.g., BANK001)')
        parser.add_argument('--loan_type', type=str, choices=['RETAIL', 'COMMERCIAL'],
                            help='Loan type')
        parser.add_argument('--file_type', type=str, choices=['credit', 'payment_plan'],
                            help='File type')
        parser.add_argument('--file', type=str, help='Path to CSV file')
        parser.add_argument('--all', action='store_true',
                            help='Load all sample data for all tenants')

    def handle(self, *args, **options):
        if options['all']:
            self._load_all()
        elif all([options['tenant_id'], options['loan_type'], options['file_type'], options['file']]):
            self._load_single(
                options['tenant_id'],
                options['loan_type'],
                options['file_type'],
                options['file'],
            )
        else:
            self.stderr.write(
                self.style.ERROR(
                    'Provide --tenant_id, --loan_type, --file_type, --file or use --all'
                )
            )

    def _load_single(self, tenant_id, loan_type, file_type, file_path):
        if not os.path.exists(file_path):
            self.stderr.write(self.style.ERROR(f'File not found: {file_path}'))
            return

        records = self._read_csv(file_path)
        storage.store_data(tenant_id, loan_type, file_type, records)
        self.stdout.write(
            self.style.SUCCESS(
                f'Loaded {len(records)} rows -> {tenant_id}:{loan_type}:{file_type}'
            )
        )

    def _load_all(self):
        from django.conf import settings
        base_dir = os.path.join(settings.BASE_DIR, 'teamsec-interview-data')

        file_map = {
            ('RETAIL', 'credit'): 'retail_credit_masked.csv',
            ('RETAIL', 'payment_plan'): 'retail_payment_plan_masked.csv',
            ('COMMERCIAL', 'credit'): 'commercial_credit_masked.csv',
            ('COMMERCIAL', 'payment_plan'): 'commercial_payment_plan_masked.csv',
        }

        tenant_ids = ['BANK001', 'BANK002', 'BANK003']

        for tenant_id in tenant_ids:
            for (loan_type, file_type), filename in file_map.items():
                file_path = os.path.join(base_dir, filename)
                if not os.path.exists(file_path):
                    self.stderr.write(
                        self.style.WARNING(f'File not found: {file_path}, skipping')
                    )
                    continue

                records = self._read_csv(file_path)
                storage.store_data(tenant_id, loan_type, file_type, records)
                self.stdout.write(
                    self.style.SUCCESS(
                        f'Loaded {len(records)} rows -> {tenant_id}:{loan_type}:{file_type}'
                    )
                )

        self.stdout.write(self.style.SUCCESS('All sample data loaded.'))

    def _read_csv(self, file_path):
        records = []
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter=';')
            for row in reader:
                records.append(dict(row))
        return records
