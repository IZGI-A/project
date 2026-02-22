from django.core.management.base import BaseCommand

from adapter.clickhouse_manager import init_clickhouse_databases


class Command(BaseCommand):
    help = 'Initialize ClickHouse databases and tables for all tenants'

    def handle(self, *args, **options):
        self.stdout.write("Initializing ClickHouse databases...")
        try:
            init_clickhouse_databases()
            self.stdout.write(
                self.style.SUCCESS(
                    "ClickHouse databases and tables created successfully:\n"
                    "  - bank001_dw (fact_credit, fact_payment, staging_credit, staging_payment)\n"
                    "  - bank002_dw (fact_credit, fact_payment, staging_credit, staging_payment)\n"
                    "  - bank003_dw (fact_credit, fact_payment, staging_credit, staging_payment)"
                )
            )
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Failed to initialize ClickHouse: {e}"))
            raise
