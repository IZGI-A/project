# Financial Data Integration Adapter

Multi-tenant financial data integration platform. Banks upload loan portfolio CSV files, system validates, normalizes and stores data in a ClickHouse data warehouse. Includes automated sync with Celery Beat, real-time profiling, monitoring dashboards and a web UI.

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Web Framework | Django 5.x + DRF |
| Operational DB | PostgreSQL 16 |
| Data Warehouse | ClickHouse |
| Cache / Storage | Redis 7 |
| Task Queue | Celery + Celery Beat |
| Web Server | Gunicorn |
| Monitoring | Prometheus + Grafana |
| Frontend | Django Templates + HTMX + Chart.js |
| Container | Docker + Docker Compose |

## Architecture

```
                 ┌─────────────┐
                 │  Web UI :8000│
                 │  (HTMX)     │
                 └──────┬──────┘
                        │
          ┌─────────────┼─────────────┐
          │             │             │
    ┌─────▼─────┐ ┌─────▼─────┐ ┌────▼─────┐
    │PostgreSQL │ │ClickHouse │ │  Redis   │
    │  :5432    │ │  :8123    │ │  :6379   │
    │(multi-    │ │(data      │ │(cache +  │
    │ schema)   │ │ warehouse)│ │ staging) │
    └───────────┘ └───────────┘ └──────────┘
          │
    ┌─────▼──────────────────────────────┐
    │  Celery Beat (60s polling)         │
    │  → check_and_sync → run_sync      │
    └────────────────────────────────────┘
          │
    ┌─────▼──────────────────────────────┐
    │  Prometheus :9090 → Grafana :3000  │
    └────────────────────────────────────┘
```

**Multi-Tenancy**: PostgreSQL schema-based isolation (bank001, bank002, bank003). ClickHouse'da tenant basina ayri database (bank001_dw, bank002_dw, bank003_dw).

## Quick Start

### Ongereksinimler

- **Docker** ve **Docker Compose** yuklu olmali
- Minimum **4 GB RAM**
- Su portlar musait olmali: `8000`, `8123`, `5432`, `6379`, `3000`, `9090`

### Adim 1 — Repo'yu klonla

```bash
git clone https://github.com/<your-username>/case-study-project.git
cd case-study-project
```

### Adim 2 — Servisleri baslat

**Docker Hub image ile (onerilen):**

```bash
docker compose -f docker-compose.hub.yml up -d
```

**Yerel build ile:**

```bash
docker compose up -d
```

### Adim 3 — Baslangic surecini izle

```bash
# Docker Hub image kullaniyorsan:
docker compose -f docker-compose.hub.yml logs -f web

# Yerel build kullaniyorsan:
docker compose logs -f web
```

Servisler su sirayla ayaga kalkar:

```
1. PostgreSQL, ClickHouse, Redis   (altyapi servisleri, health check bekler)
      ↓ hazir
2. Web (Django)                    (baslangic komutlarini calistirir)
      ↓ hazir
3. Celery Worker + Celery Beat     (web basladiktan sonra baslar)
4. Prometheus → Grafana            (monitoring)
```

Web servisi ilk acilista su komutlari **otomatik** calistirir:

| Sira | Komut | Ne yapar |
|------|-------|----------|
| 1 | `migrate` | PostgreSQL tablolarini olusturur |
| 2 | `setup_schemas` | Tenant schema'larini olusturur (bank001, bank002, bank003) |
| 3 | `seed_tenants` | 3 tenant + API key'leri olusturur |
| 4 | `init_clickhouse` | ClickHouse database ve tablolari olusturur (bank001_dw, bank002_dw, bank003_dw) |
| 5 | `gunicorn` | Web sunucusunu baslatir |

Su log satirini gordugunuzde sistem hazirdir:
```
Listening at: http://0.0.0.0:8000
```

### Adim 4 — API Key'leri al

Tenant'lar ilk olusturulduklarinda API key'leri docker log'larinda gorunur:

```bash
# Docker Hub image:
docker compose -f docker-compose.hub.yml logs web | grep "API key"

# Yerel build:
docker compose logs web | grep "API key"
```

Eger log'lar kaybolmussa yeni key uret:

```bash
docker compose -f docker-compose.hub.yml exec web python manage.py shell -c "
import secrets
from django.contrib.auth.hashers import make_password
from adapter.models import Tenant

for t in Tenant.objects.all().order_by('tenant_id'):
    raw_key = 'sk_live_' + secrets.token_hex(24)
    t.api_key_hash = make_password(raw_key)
    t.api_key_prefix = raw_key[:16]
    t.save()
    print(f'{t.tenant_id}: {raw_key}')
"
```

Cikti:
```
BANK001: sk_live_abc123...
BANK002: sk_live_def456...
BANK003: sk_live_ghi789...
```

### Adim 5 — Web UI'a giris yap

1. Tarayicida `http://localhost:8000` adresine git
2. API key ile giris yap (ornegin BANK001'in key'i)
3. Dashboard sayfasi acilir

### Adim 6 — Test verisi yukle ve sync et

```bash
# Tum test verilerini yukle (3 tenant x 2 loan type x 2 file type)
docker compose -f docker-compose.hub.yml exec web python manage.py load_csv --all
```

Yukleme sonrasi iki secenek:
- **Otomatik:** 60 saniye icinde Celery Beat otomatik sync yapacak
- **Manuel:** Web UI'da Sync sayfasindan "Sync" butonuna bas

Sync durumunu izle:
```bash
docker compose -f docker-compose.hub.yml logs -f celery-worker
```

### Adim 7 — Dogrulama

| Kontrol | Nasil |
|---------|-------|
| Web UI calisiyor mu | `http://localhost:8000` |
| Dashboard'da veri var mi | Giris yaptiktan sonra satir sayilari gorunmeli |
| Celery calisiyor mu | `docker compose -f docker-compose.hub.yml logs celery-worker` |
| ClickHouse'da veri var mi | Data View sayfasinda kayitlar gorunmeli |
| Profiling calisiyor mu | Profiling sayfasinda istatistikler gorunmeli |
| Grafana calisiyor mu | `http://localhost:3000` (admin/admin) |
| Prometheus calisiyor mu | `http://localhost:9090` |

## Automated Sync (Celery Beat)

Celery Beat her 60 saniyede bir Redis'i kontrol eder. Yeni veri yuklenmisse otomatik sync baslatir.

```
Celery Beat (her 60s)
  → check_and_sync: Redis'te yeni veri var mi?
    → Lock kontrolu: sync_lock:{tenant}:{loan_type} bos mu?
      → Varsa: run_sync(tenant_id, loan_type) dispatch et
        → SyncEngine.sync() pipeline'i calistirir
```

**Concurrent Sync Korumasi:** Ayni tenant/loan_type icin esanli sync baslatilmasini onlemek icin Redis distributed lock kullanilir (`sync_lock:{tenant}:{loan_type}`, TTL: 600s). Celery Beat dispatch oncesi lock kontrol eder; SyncEngine baslarken lock alir, bitince serbest birakir.

Celery loglarini izlemek icin:
```bash
docker compose logs -f celery-worker
```

## Test Verisi Yukleme

### Yontem 1: Web UI'dan

1. **Upload CSV** sayfasina git
2. Loan Type (RETAIL/COMMERCIAL) ve File Type (credit/payment_plan) sec
3. CSV dosyasini yukle (delimiter: `;`)
4. 60 saniye icinde Celery otomatik sync yapacak, veya **Sync** sayfasindan manuel tetikle

### Yontem 2: Management komutu ile

```bash
# Tum test verilerini yukle (3 tenant x 2 loan type x 2 file type)
docker compose -f docker-compose.hub.yml exec web python manage.py load_csv --all

# Tek dosya yukle
docker compose -f docker-compose.hub.yml exec web python manage.py load_csv \
  --tenant_id BANK001 \
  --loan_type RETAIL \
  --file_type credit \
  --file data-test/BANK001/RETAIL/retail_credit.csv
```

### Yontem 3: API ile manuel sync tetikle

```bash
curl -X POST http://localhost:8000/api/sync/ \
  -H "Authorization: Api-Key sk_live_YOUR_KEY_HERE" \
  -H "Content-Type: application/json" \
  -d '{"tenant_id": "BANK001", "loan_type": "RETAIL"}'
```

## Web UI Sayfalari

| Sayfa | URL | Aciklama |
|-------|-----|----------|
| Dashboard | `/` | Ozet istatistikler, son sync islemleri, ClickHouse satir sayilari |
| Upload CSV | `/upload/` | CSV dosyasi yukleme (loan type + file type secimi) |
| Sync | `/sync/` | Sync tetikleme, gecmis sync loglari, basarisiz kayitlar |
| Data View | `/data/` | ClickHouse'daki verileri goruntuleme, server-side pagination, siralama |
| Profiling | `/profiling/` | Veri profili — min/max/avg/stddev, null oranlari, dagilimlar |
| Errors | `/errors/` | Validasyon hatalari detayi |
| Settings | `/settings/` | Tenant bilgileri, API key yenileme |

## REST API

Tum API istekleri `Authorization: Api-Key sk_live_...` header'i gerektirir.

### Sync

```bash
# Sync tetikle
POST /api/sync/
Body: {"tenant_id": "BANK001", "loan_type": "RETAIL"}

# Sync konfigurasyonlari
GET /api/sync/configs/

# Sync loglari
GET /api/sync/logs/?loan_type=RETAIL&limit=50

# Tek sync log detayi
GET /api/sync/logs/{uuid}/
```

### Data

```bash
# Kredi/odeme verileri
GET /api/data/?tenant_id=BANK001&loan_type=RETAIL&data_type=credit
```

### Profiling

```bash
# Veri profili (min, max, avg, stddev, null ratios)
GET /api/profiling/?tenant_id=BANK001&loan_type=RETAIL&data_type=credit
```

### Validation Errors

```bash
# Bir sync islemine ait hatalar
GET /api/sync/logs/{sync_log_id}/errors/
```

## Data Pipeline

```
CSV Upload → Redis (staging) → Fetch → Validate → Normalize → ClickHouse (warehouse)
```

### Validation
- **Field validation**: Zorunlu alan, tip, aralik, format kontrolleri
- **Cross-file validation**: Odeme kayitlarinin kredi kayitlarina referans butunlugu

### Normalization
- **Tarih**: `20240115` → `2024-01-15`
- **Oran**: `18.5` → `0.185`
- **Kategori**: `A` → `ACTIVE`, `C` → `CLOSED`

### Storage (ClickHouse)
- Atomic replacement: `REPLACE PARTITION` ile veri degisimi
- Ayni loan type icin yeni yukleme eskisinin yerine gecer (append degil)
- Staging tablolar **MergeTree** engine kullanir (background merge dedup'u onlemek icin)
- Fact tablolar **ReplacingMergeTree(loaded_at)** engine kullanir

## Monitoring

### Grafana Dashboards

`http://localhost:3000` (admin/admin)

- **Application Overview**: HTTP request/response metrikleri, sync basari oranlari
- **Infrastructure**: PostgreSQL baglantilari, Redis bellek, ClickHouse, container kaynak kullanimi

### Prometheus

`http://localhost:9090`

Metrik kaynaklari:
- Django uygulama metrikleri (`:8000/metrics`)
- PostgreSQL exporter (`:9187`)
- Redis exporter (`:9121`)
- ClickHouse (`:9363`)
- cAdvisor container metrikleri (`:8081`)

### Alert Rules

- HTTP 5xx hata orani > %5
- P95 latency > 2s
- Sync hata orani > %30
- PostgreSQL connection pool > %85
- Redis bellek > %80
- Container CPU > %80

## Servisler ve Portlar

| Servis | Port | Aciklama |
|--------|------|----------|
| web | 8000 | Django web uygulamasi |
| celery-worker | — | Celery task worker |
| celery-beat | — | Celery Beat scheduler (60s polling) |
| db | 5432 | PostgreSQL |
| clickhouse | 8123 | ClickHouse HTTP |
| redis | 6379 | Redis (DB0: Celery, DB1: staging, DB2: cache), maxmemory: 512MB |
| prometheus | 9090 | Prometheus |
| grafana | 3000 | Grafana |
| cadvisor | 8081 | Container metrikleri |
| postgres-exporter | 9187 | PostgreSQL metrikleri |
| redis-exporter | 9121 | Redis metrikleri |

## Proje Yapisi

```
project/
├── adapter/                 # Core integration logic
│   ├── models.py           # Tenant, SyncLog, SyncConfiguration, ValidationError
│   ├── tasks.py            # Celery tasks (check_and_sync, run_sync)
│   ├── validators/         # Field + cross-file validation
│   ├── normalizers/        # Date, rate, category normalization
│   ├── storage/            # ClickHouse atomic replacement
│   ├── sync/               # SyncEngine + DataFetcher
│   ├── profiling/          # ClickHouse profiling queries
│   └── management/commands/
├── api/                    # REST API (DRF)
│   ├── views.py            # API endpoints
│   ├── authentication.py   # API key auth
│   └── permissions.py      # Tenant isolation
├── frontend/               # Web UI
│   ├── views.py            # Page views
│   └── templates/          # HTMX templates
├── external_bank/          # Simulated bank API
│   └── storage.py          # Redis gzip storage
├── core/                   # Shared utilities
│   └── cache.py            # Redis cache layer
├── config/                 # Django settings + Celery app
│   ├── settings.py
│   ├── celery.py           # Celery app + beat schedule
│   └── urls.py
├── infrastructure/         # Monitoring configs
│   ├── prometheus/
│   ├── grafana/
│   ├── clickhouse/
│   └── init_pg.sql
├── data-test/              # Sample CSV files
├── tests/                  # Test suite
├── docker-compose.yml      # Local development
├── docker-compose.hub.yml  # Docker Hub images
└── Dockerfile              # Django app + Celery
```

## Testler

> Not: Testler Docker container icinde calistirilir. Redis ve ClickHouse baglantisi gerektirir.

### Unit Testler (pytest)

Validator, normalizer ve storage katmanlarini test eder. ClickHouse'a erismez, Redis gerektirir.

```bash
# Tum unit testleri calistir
docker compose exec web pytest

# Detayli cikti
docker compose exec web pytest -v

# Tek bir test dosyasi calistir
docker compose exec web pytest tests/test_validators.py
docker compose exec web pytest tests/test_storage.py
docker compose exec web pytest tests/test_cross_validator.py

# Sadece belirli bir test sinifi
docker compose exec web pytest tests/test_validators.py::TestCreditFieldValidator

# Sadece belirli bir test
docker compose exec web pytest tests/test_validators.py::TestRateNormalizer::test_percentage_rate_divided
```

**Test dosyalari:**

| Dosya | Kapsam | Test Sayisi |
|-------|--------|-------------|
| `tests/test_validators.py` | Kredi/odeme alan dogrulamasi, tarih/oran/kategori normalizasyonu | 21 |
| `tests/test_storage.py` | Redis gzip storage, tenant izolasyonu, replace semantigi | 8 |
| `tests/test_cross_validator.py` | Odeme → kredi referans butunlugu, ClickHouse union | 4 |

**Test senaryolari (spec'ten):**

- Zorunlu alan eksik → hata
- Gecersiz musteri tipi (X) → VALUE hatasi
- Negatif tutar → RANGE hatasi
- Gecersiz tarih formati → FORMAT hatasi
- BANK001 verisi yuklendi → BANK002 goremez (tenant izolasyonu)
- 100 kayit yukle, 200 kayit yukle → Redis'te 200 olmali (replace semantigi)
- Odeme kaydi var olmayan krediye referans → CROSS_REFERENCE hatasi
- ClickHouse'taki mevcut kredi + batch'teki kredi birlikte kontrol

### Stres Testi (200MB+)

Buyuk veri dosyalariyla tum ETL pipeline'ini test eder. `data-test/large-data/` dizininde buyuk CSV dosyalari gerektirir.

```bash
# 200MB+ stres testi (upload → sync → ClickHouse verify)
docker compose exec web python /app/tests/test_large_upload.py
```

Bu test su adimlari calistirir:
1. CSV dosyalarini streaming upload ile Redis'e yukler
2. SyncEngine.sync() ile tum pipeline'i calistirir (validate → normalize → ClickHouse)
3. ClickHouse'da dogru satir sayisini dogrular
4. Memory kullanimi ve sure olcumu yapar

Beklenen cikti:
```
LARGE FILE STRESS TEST (200MB+)
  retail_credit_large.csv: 183.5 MB
  retail_payment_plan_large.csv: 22.1 MB

UPLOADING: retail_credit_large.csv
  Rows stored: 1,394,000
  Time: 47.3s

RUNNING SYNC: BANK001 / RETAIL
  Status: COMPLETED
  Credit rows: 1,394,000/1,394,000
  Payment rows: 2,607,535/2,607,535

VERIFYING CLICKHOUSE: bank001_dw
  fact_credit (RETAIL):  1,394,000 rows
  fact_payment (RETAIL): 2,607,535 rows

  RESULT: PASSED
```

### Veritabanini Test Icin Sifirla

```bash
docker compose exec web python manage.py shell -c "
from adapter.clickhouse_manager import get_clickhouse_client
from adapter.models import SyncLog, ValidationError, Tenant
from config.db_router import set_current_tenant_schema, clear_current_tenant_schema
import redis

# ClickHouse
for db in ['bank001_dw', 'bank002_dw', 'bank003_dw']:
    client = get_clickhouse_client(database=db)
    for t in ['fact_credit','fact_payment','staging_credit','staging_payment']:
        client.command(f'TRUNCATE TABLE {t}')

# PostgreSQL
for t in Tenant.objects.all():
    set_current_tenant_schema(t.pg_schema)
    ValidationError.objects.all().delete()
    SyncLog.objects.all().delete()
    clear_current_tenant_schema()

# Redis (tum DB'ler)
for db in [0, 1, 2]:
    redis.Redis(host='redis', port=6379, db=db).flushdb()

print('Tum veritabanlari sifirlandi.')
"
```

## Docker Hub

Image Docker Hub'da mevcut:

```bash
docker pull whale99/findata-web:latest
```

## Yararli Komutlar

> Not: Yerel build kullaniyorsan `-f docker-compose.hub.yml` kismini kaldir.

```bash
# Loglari izle
docker compose -f docker-compose.hub.yml logs -f web
docker compose -f docker-compose.hub.yml logs -f celery-worker

# Django shell
docker compose -f docker-compose.hub.yml exec web python manage.py shell

# Testleri calistir
docker compose -f docker-compose.hub.yml exec web pytest

# Veritabanini sifirla
docker compose -f docker-compose.hub.yml down -v
docker compose -f docker-compose.hub.yml up -d

# Tek servisi yeniden baslat
docker compose -f docker-compose.hub.yml restart web

# Tum servisleri durdur
docker compose -f docker-compose.hub.yml down
```
