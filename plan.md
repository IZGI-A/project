# Finansal Veri Entegrasyon Adaptoru - Uygulama Plani

## Context

TeamSec Full-Stack Developer case study projesi. Harici banka web servisinden kredi portfoy verilerini cekip, dogrulayip, normalize edip Data Warehouse'da saklayan bir adaptor.

**Tech Stack:**
- **Django + DRF** → Web framework & REST API
- **PostgreSQL** → Operasyonel veriler (tenant basina ayri DB)
- **ClickHouse** → Data Warehouse (tenant basina ayri DB)
- **Apache Airflow** → Task orchestration
- **Docker** → Tum servisler containerized
- **Grafana + Prometheus** → Monitoring
- **Frontend:** Django Templates + HTMX + Chart.js + Bootstrap 5

---

## 1. Veritabani Mimarisi (Tenant Basina Ayri DB)

### Genel Yapi

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

> Detayli tablo semalari icin bkz: [DB_SCHEMA.md](DB_SCHEMA.md)

### Onemli Tasarim Kararlari

**Multi-Tenant: Ayri DB'ler**
- Her tenant icin ayri PostgreSQL ve ClickHouse veritabani
- Tam veri izolasyonu - bir tenant'in verisi baska tenant'in DB'sine girmez
- Django Database Router ile otomatik yonlendirme

**Iki Kredi Tipi: Tek Birlesik Tablo**
- Retail ve Commercial %80 ortak kolon paylasir
- `loan_type` discriminator kolonu ile ayrim
- Tipe ozel kolonlar nullable (retail-only, commercial-only)

**Atomic Replacement: ClickHouse REPLACE PARTITION**
- Tablolar `PARTITION BY loan_type` ile partition'li
- Yeni veri staging tabloya yazilir
- `ALTER TABLE REPLACE PARTITION` ile atomic swap
- Fail durumunda staging temizlenir, fact korunur
- Detay: [DB_SCHEMA.md - Bolum 4](DB_SCHEMA.md)

**Data Profiling: ClickHouse Real-Time**
- Ayri tablo/cache yok
- ClickHouse'un kolonel motoru ile dogrudan fact tablolarindan hesaplanir
- min/max/avg/stddev, unique count, null ratio - milisaniyeler icinde
- Detay: [DB_SCHEMA.md - Bolum 5](DB_SCHEMA.md)

---

## 2. Proje Dizin Yapisi

```
project/
├── manage.py
├── config/                          # Django proje ayarlari
│   ├── __init__.py
│   ├── settings.py
│   ├── urls.py
│   ├── wsgi.py
│   ├── asgi.py
│   └── db_router.py                # TenantDatabaseRouter
├── external_bank/                   # Simule banka API
│   ├── __init__.py
│   ├── apps.py
│   ├── models.py
│   ├── views.py                     # Upload, Update, Retrieve endpoints
│   ├── urls.py
│   ├── serializers.py
│   └── storage.py                   # In-memory dict storage
├── adapter/                         # Ana adaptor kodu
│   ├── __init__.py
│   ├── apps.py
│   ├── models.py                    # PostgreSQL modelleri (SyncLog, ValidationError, vs.)
│   ├── clickhouse_manager.py        # ClickHouse tablo yonetimi + baglanti
│   ├── validators/
│   │   ├── __init__.py
│   │   ├── base.py                  # BaseValidator abstract class
│   │   ├── field_validators.py      # CreditFieldValidator, PaymentFieldValidator
│   │   └── cross_validators.py      # CrossFileValidator
│   ├── normalizers/
│   │   ├── __init__.py
│   │   ├── date_normalizer.py       # YYYYMMDD, YYYY-MM-DD → date
│   │   ├── rate_normalizer.py       # Yuzde → decimal
│   │   └── category_normalizer.py   # A→ACTIVE, K→CLOSED, H→0, E→1
│   ├── storage/
│   │   ├── __init__.py
│   │   └── manager.py              # ClickHouse atomic replacement (REPLACE PARTITION)
│   ├── sync/
│   │   ├── __init__.py
│   │   ├── engine.py               # Sync orchestrator
│   │   └── fetcher.py              # HTTP client (external bank'tan veri cekme)
│   └── profiling/
│       ├── __init__.py
│       └── engine.py               # ClickHouse real-time profiling sorgulari
├── api/                             # REST API
│   ├── __init__.py
│   ├── apps.py
│   ├── views.py                     # API view'lari
│   ├── urls.py
│   ├── serializers.py
│   ├── authentication.py           # JWT authentication + tenant_id claim
│   ├── permissions.py              # TenantIsolationPermission
│   └── middleware.py               # TenantMiddleware (JWT → thread-local)
├── frontend/                        # Web arayuzu
│   ├── __init__.py
│   ├── apps.py
│   ├── views.py
│   ├── urls.py
│   └── templates/
│       ├── base.html               # Bootstrap 5 + HTMX + Chart.js
│       ├── login.html
│       ├── dashboard.html
│       ├── upload.html
│       ├── sync.html
│       ├── data_view.html
│       ├── profiling.html
│       └── errors.html
├── tests/                           # Testler
│   ├── __init__.py
│   ├── conftest.py
│   ├── test_external_bank/
│   ├── test_validators/
│   ├── test_normalizers/
│   ├── test_storage/
│   ├── test_sync/
│   ├── test_api/
│   └── test_integration/
├── airflow/                         # Airflow DAG'lari
│   └── dags/
│       ├── sync_dag.py             # Periyodik sync DAG
│       └── profiling_dag.py        # Profiling DAG
├── infrastructure/                  # Monitoring yapilandirmasi
│   ├── prometheus/
│   │   └── prometheus.yml
│   └── grafana/
│       ├── dashboards/
│       └── datasources/
├── docker-compose.yml
├── Dockerfile
├── Dockerfile.airflow
├── requirements.txt
├── .env.example
├── DB_SCHEMA.md
├── README.md
└── Architecture.md
```

---

## 3. Docker Servisleri

| Servis | Port | Aciklama |
|--------|------|----------|
| web | 8000 | Django + Gunicorn |
| db | 5432 | PostgreSQL 16 (4 DB: shared + 3 tenant) |
| clickhouse | 8123/9000 | ClickHouse (3 tenant DB) |
| redis | 6379 | Cache |
| airflow-webserver | 8080 | Airflow UI |
| airflow-scheduler | - | DAG scheduler |
| prometheus | 9090 | Metrik toplama |
| grafana | 3000 | Dashboard |

---

## 4. Temel Bilesenler

### 4.1 Validator'lar

**CreditFieldValidator:**
- Required alanlar: loan_account_number, customer_id, loan_status_code, original_loan_amount, loan_start_date
- Tip kontrolleri: tutarlar sayisal, taksitler integer
- Aralik: days_past_due >= 0, original_loan_amount > 0, paid <= total installments
- Format: tarihler YYYYMMDD veya YYYY-MM-DD, status 'A' veya 'K'

**PaymentFieldValidator:**
- Required: loan_account_number, installment_number, installment_amount
- installment_number > 0, tutarlar >= 0
- Status: 'A' veya 'K'

**CrossFileValidator:**
- Payment'daki loan_account_number gecerli bir krediye ait mi?
- Gecerli kredi seti = mevcut batch kredileri ∪ ClickHouse'daki mevcut krediler
  ```
  valid_loans = batch_credits ∪ clickhouse_existing_credits

  1. Mevcut batch'teki gecerli credit'lerden loan_account_number set'i olustur
  2. ClickHouse'dan tenant+loan_type icin mevcut kredileri cek:
     SELECT DISTINCT loan_account_number FROM fact_credit WHERE loan_type = '{loan_type}'
  3. Ikisini birlestir (union)
  4. Payment satirlarini bu birlesik set'e karsi dogrula
  ```

### 4.2 Normalizer'lar

**DateNormalizer:**
- YYYYMMDD (20250901) → date(2025, 9, 1)
- YYYY-MM-DD (2025-09-01) → date(2025, 9, 1)
- Bos/None → None

**RateNormalizer:**
- Deger > 1.0 → /100 (5.14 → 0.0514, 55.47 → 0.5547)
- default_probability zaten decimal → degismez
- 0 → 0

**CategoryNormalizer:**
- Kredi status: A→ACTIVE, K→CLOSED
- Odeme status: A→ACTIVE, K→CLOSED
- Sigorta: H→0, E→1
- Musteri tipi: T→TRADE, V→VIP, I→INDIVIDUAL

### 4.3 Storage Manager (ClickHouse Atomic Replacement)
- Staging tablolari temizle
- Yeni veriyi staging'e INSERT (batch_size=10000, chunk processing)
- REPLACE PARTITION ile atomic swap
- Hata → staging temizle, fact korunsun

### 4.4 Sync Engine Pipeline
```
FETCH → VALIDATE → NORMALIZE → STORE
```
- External bank'tan HTTP ile veri cek
- Field-level + cross-file validation
- Normalize (tarih, oran, kategori)
- ClickHouse'a atomic write
- Hata orani %50+ ise sync iptal, eski veri korunur

### 4.5 Airflow DAG'lari
- **sync_dag**: Her tenant+loan_type icin periyodik sync
- **profiling_dag**: Ihtiyac halinde profiling tetikleme
- Django management command'lari Airflow'dan BashOperator ile cagirilir

---

## 5. API Endpoint'leri

| Method | Endpoint | Aciklama |
|--------|----------|----------|
| POST | `/api/auth/token/` | JWT token al (tenant_id + api_key) |
| POST | `/api/auth/token/refresh/` | Token yenile |
| POST | `/api/sync/` | Sync tetikle {tenant_id, loan_type} |
| GET | `/api/sync/{id}/` | Sync durumu |
| GET | `/api/sync/history/` | Sync gecmisi |
| GET | `/api/data/` | DWH verisi ?tenant_id=X&loan_type=Y (paginated) |
| GET | `/api/data/credits/` | Kredi verileri |
| GET | `/api/data/payments/` | Odeme verileri |
| GET | `/api/profiling/` | Profiling istatistikleri ?tenant_id=X&loan_type=Y |
| GET | `/api/validation-errors/` | Validation hatalari ?sync_id=X |
| POST | `/bank/api/upload/` | Banka'ya CSV yukle |
| PUT | `/bank/api/update/` | Banka verisini guncelle |
| GET | `/bank/api/data/` | Banka verisini JSON al |

**Authentication:** JWT token ile. Her token'da tenant_id claim'i bulunur.
**Tenant Izolasyonu:** Middleware JWT'den tenant_id cikarir, permission class request'teki tenant_id'yi token'daki ile eslestirir.

---

## 6. Frontend Sayfalari

**Tech:** Django Templates + HTMX + Chart.js + Bootstrap 5 + DataTables.js

| Sayfa | Islevler |
|-------|----------|
| login.html | Tenant giris (tenant_id + api_key) |
| dashboard.html | Genel bakis kartlari, son sync'ler, hizli aksiyonlar |
| upload.html | CSV yukleme (tenant/loan_type secimi + dosya) |
| sync.html | Sync tetikleme + HTMX polling ile durum takibi |
| data_view.html | Kredi/odeme verileri (DataTables, filtreleme, pagination) |
| profiling.html | Chart.js grafikleri (dagilim, pie chart, null ratio bar) |
| errors.html | Validation hatalari tablosu (sync_id bazli filtreleme) |

**Grafikler:**
- Kredi tutari dagilimi (histogram)
- Faiz orani dagilimi (histogram)
- Kredi durum dagilimi (pie chart - ACTIVE vs CLOSED)
- Odeme durum dagilimi (pie chart - ACTIVE vs CLOSED)
- Gecikme gunleri (bar chart - 0, 1-30, 31-60, 61-90, 90+)
- Null ratio (horizontal bar - alan bazli)

---

## 7. Uygulama Fazlari

### Faz 1: Proje Iskeleti + Veritabani
- Django projesi + apps olustur
- PostgreSQL: 4 DB olustur (shared + 3 tenant), migration'lari her DB'ye uygula
- ClickHouse: 3 DB olustur, fact_credit + fact_payment + staging tablolari
- TenantDatabaseRouter
- Tenant seed (BANK001, BANK002, BANK003)
- Docker: web, db, clickhouse, redis
- requirements.txt

### Faz 2: Harici Banka Simulasyonu
- In-memory storage backend
- CSV upload endpoint (`;` delimiter, chunk okuma)
- Guncelleme ve JSON retrieval endpoint'leri
- load_csv management command (CSV'leri banka'ya yukleme)

### Faz 3: Validator'lar ve Normalizer'lar
- BaseValidator + CreditFieldValidator + PaymentFieldValidator
- CrossFileValidator (batch + ClickHouse mevcut kredilere karsi)
- DateNormalizer, RateNormalizer, CategoryNormalizer
- Unit testler

### Faz 4: Storage Manager + Sync Engine
- ClickHouse StorageManager (staging + REPLACE PARTITION)
- DataFetcher (external bank HTTP client)
- SyncEngine pipeline (fetch→validate→normalize→store)
- Integration testler (4 senaryo dahil)

### Faz 5: REST API + Authentication
- JWT auth with tenant_id claim
- TenantMiddleware + TenantIsolationPermission
- Tum API endpoint'leri
- API testleri

### Faz 6: Data Profiling (ClickHouse Real-Time)
- ClickHouse'dan dogrudan aggregation sorgulari (min/max/avg/stddev, unique count, null ratio)
- Ayri cache tablosu yok - ClickHouse zaten milisaniyelerde hesaplar
- ProfilingEngine: tenant'in ClickHouse DB'sine baglanip sorgulari calistirir, JSON doner

### Faz 7: Frontend
- Tum sayfalar (login, dashboard, upload, sync, data_view, profiling, errors)
- Chart.js grafikleri
- HTMX ile dinamik guncelleme
- DataTables.js ile veri tablolari

### Faz 8: Airflow + Docker + Monitoring + Dokumantasyon
- Airflow DAG'lari (sync_dag, profiling_dag)
- docker-compose.yml (tum 8 servis)
- Prometheus metrikleri + Grafana dashboard'lari
- README.md (kurulum, multi-tenant karar, test calistirma, API ornekleri)
- Architecture.md (sistem mimarisi, veri akisi, bilesen diyagrami)

---

## 8. Test Senaryolari

1. **Tenant izolasyonu**: BANK001 verisi yukle → BANK002 token'i ile cek → veri gelmemeli
2. **Cift kredi tipi**: Bir tenant'a RETAIL + COMMERCIAL yukle → ikisi ayri cekilebilmeli
3. **Replacement**: 1000 kredi yukle → 2000 kredi yukle → DWH'da 2000 olmali (3000 degil)
4. **Validation fail**: Gecerli veri yukle → gecersiz veri yukle → eski veri korunmali

---

## 9. Dogrulama

1. `docker-compose up` ile tum servisler ayaga kalkar
2. `load_csv` komutu ile 3 tenant'a veri yuklenir
3. JWT token alinip `/api/sync/` ile sync tetiklenir
4. `/api/data/` ve `/api/profiling/` ile veri ve istatistikler kontrol edilir
5. Frontend'den upload, sync, gorsellestirme test edilir
6. `pytest tests/` ile tum testler gecer
7. Airflow UI'dan DAG'lar gorunur ve tetiklenebilir
8. Grafana dashboard'da metrikler gorunur
