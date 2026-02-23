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

## Prerequisites

- **Docker** ve **Docker Compose** yuklu olmali
- Minimum 4 GB RAM (tum servisler icin)
- Portlar musait olmali: 8000, 8123, 5432, 6379, 3000, 9090

## Quick Start

### 1. Repo'yu klonla

```bash
git clone https://github.com/<your-username>/case-study-project.git
cd case-study-project
```

### 2. Docker Hub'dan cek ve calistir

```bash
docker compose -f docker-compose.hub.yml up -d
```

> Yerel build ile calistirmak istersen:
> ```bash
> docker compose up -d
> ```

### 3. Servislerin hazir olmasini bekle

```bash
docker compose logs -f web
```

Su log satirini gorene kadar bekle:
```
Listening at: http://0.0.0.0:8000
```

Ilk acilista otomatik olarak sunlar calisir:
- `migrate` — PostgreSQL tablolarini olusturur
- `setup_schemas` — Tenant schema'larini olusturur (bank001, bank002, bank003)
- `seed_tenants` — 3 tenant + API key'leri olusturur
- `init_clickhouse` — ClickHouse database ve tablolari olusturur

### 4. API Key'leri al

Tenant'lar ilk olusturulduklarinda API key'leri docker log'larinda gorunur:

```bash
docker compose logs web | grep "API key"
```

Eger log'lar kaybolmussa yeni key uret:

```bash
docker compose exec web python manage.py shell -c "
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

### 5. Web UI'a giris yap

1. Tarayicida `http://localhost:8000` adresine git
2. API key ile giris yap
3. Dashboard sayfasi acilir

## Automated Sync (Celery Beat)

Celery Beat her 60 saniyede bir Redis'i kontrol eder. Yeni veri yuklenmisse otomatik sync baslatir.

```
Celery Beat (her 60s)
  → check_and_sync: Redis'te yeni veri var mi?
    → Varsa: run_sync(tenant_id, loan_type) dispatch et
      → SyncEngine.sync() pipeline'i calistirir
```

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
docker compose exec web python manage.py load_csv --all

# Tek dosya yukle
docker compose exec web python manage.py load_csv \
  --tenant_id BANK001 \
  --loan_type RETAIL \
  --file_type credit \
  --file data-test/BANK001/RETAIL/retail_credit.csv
```

Yukleme sonrasi sync tetikle (manuel):

```bash
# Web UI'dan: Sync sayfasi → "Sync" butonu
# veya API ile:
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
| Data View | `/data/` | ClickHouse'daki verileri goruntuleme, filtreleme, siralama |
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
| redis | 6379 | Redis (DB0: Celery, DB1: staging, DB2: cache) |
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

## Docker Hub

Image Docker Hub'da mevcut:

```bash
docker pull whale99/findata-web:latest
```

## Yararli Komutlar

```bash
# Loglari izle
docker compose logs -f web
docker compose logs -f celery-worker

# Django shell
docker compose exec web python manage.py shell

# Testleri calistir
docker compose exec web pytest

# Veritabanini sifirla
docker compose down -v
docker compose up -d

# Tek servisi yeniden baslat
docker compose restart web

# Tum servisleri durdur
docker compose down
```
