import clickhouse_connect
from django.conf import settings


def get_clickhouse_client(database='default'):
    """Create a ClickHouse client connection for the given database."""
    return clickhouse_connect.get_client(
        host=settings.CLICKHOUSE_HOST,
        port=settings.CLICKHOUSE_PORT,
        username=settings.CLICKHOUSE_USER,
        password=settings.CLICKHOUSE_PASSWORD,
        database=database,
    )


FACT_CREDIT_DDL = """
CREATE TABLE IF NOT EXISTS fact_credit (
    batch_id                        UUID,
    loan_type                       LowCardinality(String),
    loaded_at                       DateTime DEFAULT now(),

    loan_account_number             String,
    customer_id                     String,
    customer_type                   LowCardinality(String),
    loan_status_code                LowCardinality(String),
    days_past_due                   UInt32 DEFAULT 0,
    final_maturity_date             Nullable(Date),
    total_installment_count         UInt32 DEFAULT 0,
    outstanding_installment_count   UInt32 DEFAULT 0,
    paid_installment_count          UInt32 DEFAULT 0,
    first_payment_date              Nullable(Date),
    original_loan_amount            Decimal(18, 2),
    outstanding_principal_balance   Decimal(18, 2),
    nominal_interest_rate           Decimal(10, 6),
    total_interest_amount           Decimal(18, 2) DEFAULT 0,
    kkdf_rate                       Decimal(10, 6) DEFAULT 0,
    kkdf_amount                     Decimal(18, 2) DEFAULT 0,
    bsmv_rate                       Decimal(10, 6) DEFAULT 0,
    bsmv_amount                     Decimal(18, 2) DEFAULT 0,
    grace_period_months             UInt32 DEFAULT 0,
    installment_frequency           UInt32 DEFAULT 1,
    loan_start_date                 Nullable(Date),
    loan_closing_date               Nullable(Date),
    internal_rating                 Nullable(UInt32),
    external_rating                 Nullable(UInt32),

    loan_product_type               Nullable(UInt32),
    customer_region_code            Nullable(String),
    sector_code                     Nullable(UInt32),
    internal_credit_rating          Nullable(UInt32),
    default_probability             Nullable(Decimal(10, 6)),
    risk_class                      Nullable(UInt32),
    customer_segment                Nullable(UInt32),

    insurance_included              Nullable(UInt8),
    customer_district_code          Nullable(String),
    customer_province_code          Nullable(String)
)
ENGINE = ReplacingMergeTree(loaded_at)
PARTITION BY loan_type
ORDER BY (loan_type, loan_account_number)
SETTINGS index_granularity = 8192
"""

FACT_PAYMENT_DDL = """
CREATE TABLE IF NOT EXISTS fact_payment (
    batch_id                UUID,
    loan_type               LowCardinality(String),
    loaded_at               DateTime DEFAULT now(),

    loan_account_number     String,
    installment_number      UInt32,
    actual_payment_date     Nullable(Date),
    scheduled_payment_date  Nullable(Date),
    installment_amount      Decimal(18, 2),
    principal_component     Decimal(18, 2),
    interest_component      Decimal(18, 2) DEFAULT 0,
    kkdf_component          Decimal(18, 2) DEFAULT 0,
    bsmv_component          Decimal(18, 2) DEFAULT 0,
    installment_status      LowCardinality(String),
    remaining_principal     Decimal(18, 2) DEFAULT 0,
    remaining_interest      Decimal(18, 2) DEFAULT 0,
    remaining_kkdf          Decimal(18, 2) DEFAULT 0,
    remaining_bsmv          Decimal(18, 2) DEFAULT 0
)
ENGINE = ReplacingMergeTree(loaded_at)
PARTITION BY loan_type
ORDER BY (loan_type, loan_account_number, installment_number)
SETTINGS index_granularity = 8192
"""


TENANT_DBS = ['bank001_dw', 'bank002_dw', 'bank003_dw']


def init_clickhouse_databases():
    """Create all tenant ClickHouse databases and tables."""
    client = get_clickhouse_client(database='default')

    for db_name in TENANT_DBS:
        client.command(f"CREATE DATABASE IF NOT EXISTS {db_name}")

        db_client = get_clickhouse_client(database=db_name)

        db_client.command(FACT_CREDIT_DDL)
        db_client.command(FACT_PAYMENT_DDL)

        # Staging tables (same schema as fact tables)
        staging_credit_ddl = FACT_CREDIT_DDL.replace(
            'fact_credit', 'staging_credit'
        )
        staging_payment_ddl = FACT_PAYMENT_DDL.replace(
            'fact_payment', 'staging_payment'
        )
        db_client.command(staging_credit_ddl)
        db_client.command(staging_payment_ddl)

    client.close()
