# Veritabani Mimarisi - DB Schema

## Genel Yapi

Her tenant icin hem PostgreSQL hem ClickHouse'da ayri veritabanlari bulunur.

```
PostgreSQL Instance
├── financial_shared    ← Paylasimli: sadece tenant registry + auth
├── bank001_ops         ← BANK001 operasyonel verileri
├── bank002_ops         ← BANK002 operasyonel verileri
└── bank003_ops         ← BANK003 operasyonel verileri

ClickHouse Instance
├── bank001_dw          ← BANK001 data warehouse
├── bank002_dw          ← BANK002 data warehouse
└── bank003_dw          ← BANK003 data warehouse
```

Django Database Router ile request'teki tenant_id'ye gore dogru DB'ye yonlendirme yapilir.

---

## 1. PostgreSQL - Paylasimli DB: `financial_shared`

### `tenants`

Tenant kayitlari. Tum tenantlarin kimlik ve baglanti bilgilerini tutar.

```sql
CREATE TABLE tenants (
    id              SERIAL PRIMARY KEY,
    tenant_id       VARCHAR(20) UNIQUE NOT NULL,    -- 'BANK001', 'BANK002', 'BANK003'
    name            VARCHAR(100) NOT NULL,
    api_key_hash    VARCHAR(255) NOT NULL,           -- Hash'lenmis API key (plain text DEGIL)
    api_key_prefix  VARCHAR(8) NOT NULL,             -- "sk_live_a..." (tanimlama icin ilk 8 karakter)
    pg_database     VARCHAR(50) NOT NULL,            -- 'bank001_ops'
    ch_database     VARCHAR(50) NOT NULL,            -- 'bank001_dw'
    is_active       BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);
```

**Ornek veri:**
| tenant_id | name | pg_database | ch_database |
|-----------|------|-------------|-------------|
| BANK001 | Banka 1 | bank001_ops | bank001_dw |
| BANK002 | Banka 2 | bank002_ops | bank002_dw |
| BANK003 | Banka 3 | bank003_ops | bank003_dw |

---

## 2. PostgreSQL - Tenant DB'leri: `bank001_ops`, `bank002_ops`, `bank003_ops`

Her tenant DB'si ayni semaya sahiptir. Asagidaki tablolar her tenant DB'sinde bulunur.

### `sync_configurations`

Her tenant+loan_type kombinasyonu icin sync ayarlari.

```sql
CREATE TABLE sync_configurations (
    id                      SERIAL PRIMARY KEY,
    loan_type               VARCHAR(20) NOT NULL,           -- 'RETAIL' veya 'COMMERCIAL'
    external_bank_url       VARCHAR(500) NOT NULL,
    sync_interval_minutes   INTEGER DEFAULT 60,
    is_enabled              BOOLEAN DEFAULT TRUE,
    last_sync_at            TIMESTAMP NULL,
    last_sync_status        VARCHAR(20) DEFAULT 'PENDING',  -- PENDING, COMPLETED, FAILED
    created_at              TIMESTAMP DEFAULT NOW(),
    UNIQUE(loan_type)
);
```

### `sync_logs`

Her sync operasyonunun gecmisi ve sonuclari.

```sql
CREATE TABLE sync_logs (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    loan_type           VARCHAR(20) NOT NULL,               -- 'RETAIL' veya 'COMMERCIAL'
    batch_id            UUID NOT NULL,                      -- ClickHouse'daki batch ile eslesen ID
    status              VARCHAR(20) NOT NULL,               -- STARTED, FETCHING, VALIDATING,
                                                            -- NORMALIZING, STORING, COMPLETED, FAILED
    total_credit_rows   INTEGER DEFAULT 0,
    total_payment_rows  INTEGER DEFAULT 0,
    valid_credit_rows   INTEGER DEFAULT 0,
    valid_payment_rows  INTEGER DEFAULT 0,
    error_count         INTEGER DEFAULT 0,
    error_summary       JSONB DEFAULT '{}',                 -- Ozet hata bilgisi
    started_at          TIMESTAMP DEFAULT NOW(),
    completed_at        TIMESTAMP NULL
);
```

### `validation_errors`

Dogrulama hatalari. Her hatali satir icin bir kayit.

```sql
CREATE TABLE validation_errors (
    id              BIGSERIAL PRIMARY KEY,
    sync_log_id     UUID REFERENCES sync_logs(id) ON DELETE CASCADE,
    row_number      INTEGER NOT NULL,                       -- CSV'deki satir numarasi
    file_type       VARCHAR(20) NOT NULL,                   -- 'credit' veya 'payment_plan'
    field_name      VARCHAR(100) NOT NULL,                  -- Hatali alan adi
    error_type      VARCHAR(50) NOT NULL,                   -- REQUIRED, TYPE, RANGE, FORMAT, VALUE, CROSS_REFERENCE
    error_message   TEXT NOT NULL,                          -- Insan-okunabilir hata mesaji
    raw_value       TEXT NULL                               -- Hatali ham deger
);
CREATE INDEX idx_val_errors_sync ON validation_errors(sync_log_id);
```

**error_type degerleri:**
| error_type | Aciklama | Ornek |
|------------|----------|-------|
| REQUIRED | Zorunlu alan bos | loan_account_number bos |
| TYPE | Yanlis veri tipi | original_loan_amount = "abc" |
| RANGE | Deger aralik disinda | days_past_due = -5 |
| FORMAT | Gecersiz format | loan_start_date = "2025/01/15" |
| VALUE | Gecersiz deger | loan_status_code = "X" |
| CROSS_REFERENCE | Referans butunlugu hatasi | Payment'in kredisi bulunamadi |

---

## 3. ClickHouse - Tenant DW DB'leri: `bank001_dw`, `bank002_dw`, `bank003_dw`

Her tenant DW'si ayni semaya sahiptir. Asagidaki tablolar her tenant DB'sinde bulunur.

### `fact_credit` - Kredi verileri (RETAIL + COMMERCIAL birlesik)

Hem retail hem commercial krediler tek tabloda, `loan_type` ile ayrilir.
Retail'e ozel alanlar commercial icin NULL, commercial'e ozel alanlar retail icin NULL.

```sql
CREATE TABLE fact_credit (
    -- Meta Alanlar
    batch_id                        UUID,                           -- Sync batch ID
    loan_type                       LowCardinality(String),         -- 'RETAIL' veya 'COMMERCIAL'
    loaded_at                       DateTime DEFAULT now(),         -- Yukleme zamani

    -- Ortak Alanlar (her iki kredi tipinde de var)
    loan_account_number             String,                         -- Kredi hesap numarasi (PK benzeri)
    customer_id                     String,                         -- Musteri ID
    customer_type                   LowCardinality(String),         -- Normalize: 'TRADE','VIP','INDIVIDUAL'
    loan_status_code                LowCardinality(String),         -- Normalize: 'ACTIVE','CLOSED'
    days_past_due                   UInt32 DEFAULT 0,               -- Gecikme gunu
    final_maturity_date             Nullable(Date),                 -- Vade bitis tarihi
    total_installment_count         UInt32 DEFAULT 0,               -- Toplam taksit sayisi
    outstanding_installment_count   UInt32 DEFAULT 0,               -- Kalan taksit sayisi
    paid_installment_count          UInt32 DEFAULT 0,               -- Odenmis taksit sayisi
    first_payment_date              Nullable(Date),                 -- Ilk odeme tarihi
    original_loan_amount            Decimal(18, 2),                 -- Kredi tutari
    outstanding_principal_balance   Decimal(18, 2),                 -- Kalan anapara
    nominal_interest_rate           Decimal(10, 6),                 -- Faiz orani (decimal: 0.0514)
    total_interest_amount           Decimal(18, 2) DEFAULT 0,       -- Toplam faiz tutari
    kkdf_rate                       Decimal(10, 6) DEFAULT 0,       -- KKDF orani (decimal)
    kkdf_amount                     Decimal(18, 2) DEFAULT 0,       -- KKDF tutari
    bsmv_rate                       Decimal(10, 6) DEFAULT 0,       -- BSMV orani (decimal)
    bsmv_amount                     Decimal(18, 2) DEFAULT 0,       -- BSMV tutari
    grace_period_months             UInt32 DEFAULT 0,               -- Odemesiz donem (ay)
    installment_frequency           UInt32 DEFAULT 1,               -- Taksit sikligi
    loan_start_date                 Nullable(Date),                 -- Kredi baslangic tarihi
    loan_closing_date               Nullable(Date),                 -- Kredi kapanis tarihi (aktif ise NULL)
    internal_rating                 Nullable(UInt32),               -- Ic derecelendirme
    external_rating                 Nullable(UInt32),               -- Dis derecelendirme

    -- Commercial-only Alanlar (RETAIL icin NULL)
    loan_product_type               Nullable(UInt32),               -- Kredi urun tipi
    loan_status_flag                Nullable(String),               -- Durum bayragi
    customer_region_code            Nullable(String),               -- Bolge kodu
    sector_code                     Nullable(UInt32),               -- Sektor kodu
    internal_credit_rating          Nullable(UInt32),               -- Ic kredi derecesi
    default_probability             Nullable(Decimal(10, 6)),       -- Temerrut olasiligi
    risk_class                      Nullable(UInt32),               -- Risk sinifi
    customer_segment                Nullable(UInt32),               -- Musteri segmenti

    -- Retail-only Alanlar (COMMERCIAL icin NULL)
    insurance_included              Nullable(UInt8),                -- Sigorta dahil mi (H→0, E→1)
    customer_district_code          Nullable(String),               -- Ilce kodu
    customer_province_code          Nullable(String)                -- Il kodu
)
ENGINE = ReplacingMergeTree(loaded_at)
PARTITION BY loan_type
ORDER BY (loan_type, loan_account_number)
SETTINGS index_granularity = 8192;
```

**Kolon kaynagi haritalamasi:**

| ClickHouse Kolonu | Commercial CSV Kolonu | Retail CSV Kolonu | Normalizasyon |
|-------------------|-----------------------|-------------------|---------------|
| loan_account_number | loan_account_number | loan_account_number | - |
| customer_id | customer_id | customer_id | - |
| customer_type | customer_type (T, V) | customer_type (I) | T→TRADE, V→VIP, I→INDIVIDUAL |
| loan_status_code | loan_status_code (A, K) | loan_status_code (A, K) | A→ACTIVE, K→CLOSED |
| nominal_interest_rate | nominal_interest_rate (5.14) | nominal_interest_rate (55.47) | /100 → decimal |
| default_probability | default_probability (0.0217) | NULL | Zaten decimal, degismez |
| insurance_included | NULL | insurance_included (H, E) | H→0, E→1 |
| *tarih alanlari* | YYYYMMDD (20250901) | YYYYMMDD (20250302) | → Date (YYYY-MM-DD) |

### `fact_payment` - Odeme plani verileri

Hem retail hem commercial odeme planlari. Yapi her iki tip icin ayni.

```sql
CREATE TABLE fact_payment (
    -- Meta Alanlar
    batch_id                UUID,                               -- Sync batch ID
    loan_type               LowCardinality(String),             -- 'RETAIL' veya 'COMMERCIAL'
    loaded_at               DateTime DEFAULT now(),             -- Yukleme zamani

    -- Odeme Alanlari
    loan_account_number     String,                             -- Bagli kredi hesap numarasi
    installment_number      UInt32,                             -- Taksit numarasi
    actual_payment_date     Nullable(Date),                     -- Gercek odeme tarihi (NULL = odenmemis)
    scheduled_payment_date  Nullable(Date),                     -- Planli odeme tarihi
    installment_amount      Decimal(18, 2),                     -- Taksit tutari
    principal_component     Decimal(18, 2),                     -- Anapara bileseni
    interest_component      Decimal(18, 2) DEFAULT 0,           -- Faiz bileseni
    kkdf_component          Decimal(18, 2) DEFAULT 0,           -- KKDF bileseni
    bsmv_component          Decimal(18, 2) DEFAULT 0,           -- BSMV bileseni
    installment_status      LowCardinality(String),             -- Normalize: 'ACTIVE' veya 'CLOSED'
    remaining_principal     Decimal(18, 2) DEFAULT 0,           -- Kalan anapara
    remaining_interest      Decimal(18, 2) DEFAULT 0,           -- Kalan faiz
    remaining_kkdf          Decimal(18, 2) DEFAULT 0,           -- Kalan KKDF
    remaining_bsmv          Decimal(18, 2) DEFAULT 0            -- Kalan BSMV
)
ENGINE = ReplacingMergeTree(loaded_at)
PARTITION BY loan_type
ORDER BY (loan_type, loan_account_number, installment_number)
SETTINGS index_granularity = 8192;
```

**Tarih format normalizasyonu (payment dosyalarinda):**
| Alan | Commercial CSV | Retail CSV | Normalizasyon |
|------|----------------|------------|---------------|
| actual_payment_date | YYYYMMDD (20241031) veya YYYY-MM-DD | YYYYMMDD (20250208) | → Date |
| scheduled_payment_date | YYYY-MM-DD (2025-09-02) | YYYY-MM-DD (2025-02-08) | → Date |

**installment_status normalizasyonu:**
| CSV Degeri | Normalize Deger | Anlam |
|------------|-----------------|-------|
| A | ACTIVE | Aktif / Odenmemis |
| K | CLOSED | Odenmis / Kapali |

### `staging_credit` ve `staging_payment` - Atomic Replacement icin

```sql
-- fact_credit ile ayni sema, ayni partition yapisi
CREATE TABLE staging_credit AS fact_credit;

-- fact_payment ile ayni sema, ayni partition yapisi
CREATE TABLE staging_payment AS fact_payment;
```

Bu tablolar sadece sync sirasinda gecici olarak kullanilir. Yeni veri once staging'e yazilir, dogrulama basarili olursa `REPLACE PARTITION` ile fact tablosuyla atomic olarak degistirilir.

---

## 4. Atomic Replacement Stratejisi (ClickHouse - `REPLACE PARTITION`)

`PARTITION BY loan_type` sayesinde her loan_type (RETAIL / COMMERCIAL) kendi partition'inda bulunur.
`ALTER TABLE REPLACE PARTITION` ile sadece ilgili partition atomic olarak degistirilir.

### Neden `REPLACE PARTITION`?
- **Metadata-only operasyon** → veri boyutundan bagimsiz, anlik
- **Partition bazli** → RETAIL sync'i COMMERCIAL'a dokunmaz
- **Dogal rollback** → Fail durumunda fact tablosu dokunulmamis kalir
- **ClickHouse native** → ACID transaction gerektirmez

### Sync Akisi (orn: BANK001, RETAIL):

```
Adim 1: Staging tablolari temizle
   TRUNCATE TABLE bank001_dw.staging_credit
   TRUNCATE TABLE bank001_dw.staging_payment

Adim 2: Yeni veriyi staging'e yaz
   INSERT INTO bank001_dw.staging_credit (loan_type='RETAIL' verileri)
   INSERT INTO bank001_dw.staging_payment (loan_type='RETAIL' verileri)

Adim 3a: Validation BASARILI ise → Atomic swap
   ALTER TABLE bank001_dw.fact_credit
     REPLACE PARTITION 'RETAIL' FROM bank001_dw.staging_credit
   ALTER TABLE bank001_dw.fact_payment
     REPLACE PARTITION 'RETAIL' FROM bank001_dw.staging_payment

   → fact tablolarinda RETAIL partition'i yeni veriyle atomic olarak degisti
   → COMMERCIAL partition'i hic etkilenmedi

   TRUNCATE TABLE bank001_dw.staging_credit
   TRUNCATE TABLE bank001_dw.staging_payment

Adim 3b: Validation BASARISIZ ise → Rollback
   TRUNCATE TABLE bank001_dw.staging_credit
   TRUNCATE TABLE bank001_dw.staging_payment

   → fact tablolari hic degismedi, eski veri korundu
```

### Yontem Karsilastirmasi

| Yontem | Atomik? | Partition bazli? | Rollback? | Hiz |
|--------|---------|-------------------|-----------|-----|
| `REPLACE PARTITION` | Evet | Evet | Dogal (staging temizle) | Anlik (metadata-only) |
| `EXCHANGE TABLES` | Evet | Hayir (tum tablo) | Manuel geri swap | Anlik |
| `DROP PARTITION + INSERT` | Hayir | Evet | Yok (veri kaybolur) | Yavas |
| `ReplacingMergeTree + FINAL` | Hayir (async merge) | Hayir | Yok | Yavas sorgular |

---

## 5. Data Profiling - ClickHouse Real-Time Sorgulari

Profiling ayri bir tabloda saklanmaz. ClickHouse'un kolonel (columnar) yapisi ve vektorize motoru sayesinde dogrudan fact tablolarindan real-time hesaplanir.

### Sayisal Alanlar (orn: original_loan_amount, nominal_interest_rate, days_past_due)
```sql
SELECT
    min(original_loan_amount) AS min_val,
    max(original_loan_amount) AS max_val,
    avg(original_loan_amount) AS avg_val,
    stddevPop(original_loan_amount) AS stddev_val,
    count() AS total_count,
    countIf(original_loan_amount = 0 OR isNull(original_loan_amount)) AS null_count
FROM fact_credit
WHERE loan_type = 'RETAIL';
```

### Kategorik Alanlar (orn: loan_status_code, customer_type)
```sql
SELECT
    loan_status_code AS value,
    count() AS frequency
FROM fact_credit
WHERE loan_type = 'COMMERCIAL'
GROUP BY loan_status_code
ORDER BY frequency DESC;
```

### Null/Missing Ratio (tum alanlar icin tek sorguda)
```sql
SELECT
    countIf(loan_account_number = '') / count() AS loan_account_number_null_ratio,
    countIf(isNull(final_maturity_date)) / count() AS final_maturity_date_null_ratio,
    countIf(isNull(loan_closing_date)) / count() AS loan_closing_date_null_ratio,
    countIf(isNull(internal_rating)) / count() AS internal_rating_null_ratio,
    countIf(isNull(external_rating)) / count() AS external_rating_null_ratio
FROM fact_credit
WHERE loan_type = 'RETAIL';
```

ClickHouse bu sorgulari yuz binlerce satir uzerinde milisaniyeler icinde calistirir.
API endpoint'i (`GET /api/profiling/`) bu sorgulari dogrudan calistirip sonucu JSON olarak doner.

---

## 6. Django Database Router

```python
DATABASES = {
    'default': {'NAME': 'financial_shared', ...},   # Tenant registry
    'bank001_ops': {'NAME': 'bank001_ops', ...},     # BANK001 operasyonel
    'bank002_ops': {'NAME': 'bank002_ops', ...},     # BANK002 operasyonel
    'bank003_ops': {'NAME': 'bank003_ops', ...},     # BANK003 operasyonel
}

class TenantDatabaseRouter:
    """
    - Tenant model → 'default' (financial_shared)
    - SyncLog, ValidationError, SyncConfiguration → tenant-specific PG DB
    - ClickHouse tablolari → clickhouse-connect ile dogrudan erisim (Django ORM disinda)
    """
    TENANT_MODELS = ['SyncLog', 'ValidationError', 'SyncConfiguration']

    def db_for_read(self, model, **hints):
        if model.__name__ in self.TENANT_MODELS:
            return get_current_tenant_db()  # thread-local'dan tenant_id → pg_database
        return 'default'

    def db_for_write(self, model, **hints):
        if model.__name__ in self.TENANT_MODELS:
            return get_current_tenant_db()
        return 'default'
```
