# Architecture

Multi-tenant finansal veri entegrasyon platformu. Bankalar kredi portfoyü CSV dosyalarini yukler, sistem validate eder, normalize eder ve ClickHouse data warehouse'a yazar.

## Genel Bakis

```
                    ┌──────────────────────────────────────┐
                    │          Web UI (:8000)               │
                    │     Django Templates + HTMX           │
                    └──────────────┬───────────────────────┘
                                   │
                    ┌──────────────▼───────────────────────┐
                    │         Django + DRF                  │
                    │  ┌──────────┬──────────┬───────────┐ │
                    │  │ Frontend │   API    │ External  │ │
                    │  │  App     │   App    │ Bank App  │ │
                    │  └──────────┴──────────┴───────────┘ │
                    └───┬──────────┬──────────┬────────────┘
                        │          │          │
           ┌────────────▼┐  ┌─────▼──────┐  ┌▼───────────┐
           │ PostgreSQL   │  │ ClickHouse │  │   Redis    │
           │ :5432        │  │ :8123      │  │   :6379    │
           │              │  │            │  │            │
           │ public:      │  │ bank001_dw │  │ DB0:Celery │
           │  └ tenants   │  │ bank002_dw │  │ DB1:Stage  │
           │ bank001:     │  │ bank003_dw │  │ DB2:Cache  │
           │  └ sync_*    │  │            │  │            │
           │ bank002:     │  │ fact_credit│  │            │
           │  └ sync_*    │  │ fact_pay.. │  │            │
           │ bank003:     │  │ staging_*  │  │            │
           │  └ sync_*    │  │            │  │            │
           └──────────────┘  └────────────┘  └────────────┘
                                                   │
                    ┌──────────────────────────────▼───────┐
                    │        Celery Beat (60s)              │
                    │  check_and_sync → run_sync            │
                    └──────────────────────────────────────┘
                                   │
                    ┌──────────────▼───────────────────────┐
                    │  Prometheus :9090 → Grafana :3000     │
                    │  postgres-exporter | redis-exporter   │
                    │  cAdvisor | ClickHouse metrics        │
                    └──────────────────────────────────────┘
```

---

## Multi-Tenancy Modeli

Her tenant (banka) icin izole ortam:

```
PostgreSQL                          ClickHouse
┌─────────────────────┐            ┌─────────────────┐
│ public schema       │            │ bank001_dw      │
│  └ tenants (kayit)  │            │  ├ fact_credit   │
│                     │            │  ├ fact_payment   │
│ bank001 schema      │            │  ├ staging_credit │
│  ├ sync_configs     │            │  └ staging_payment│
│  ├ sync_logs        │            │                   │
│  └ validation_errors│            │ bank002_dw        │
│                     │            │  └ ...             │
│ bank002 schema      │            │                   │
│  └ ...              │            │ bank003_dw        │
│                     │            │  └ ...             │
│ bank003 schema      │            └───────────────────┘
│  └ ...              │
└─────────────────────┘
```

**Izolasyon mekanizmasi:**
- Her istek icin PostgreSQL `search_path` tenant'in schema'sina ayarlanir (`TenantMixin`)
- ClickHouse sorgulari tenant'in kendi database'ine yonlendirilir
- Thread-local storage ile cross-tenant veri sizintisi onlenir

---

## Django Uygulamalari

### adapter (Core ETL)
Tum senkronizasyon pipeline'inin kalbi:
- `models.py` — Tenant, SyncConfiguration, SyncLog, ValidationError
- `sync/engine.py` — Pipeline orkestrasyon (fetch → validate → normalize → store)
- `sync/fetcher.py` — External bank API'den veri cekme
- `validators/` — Alan ve dosyalar arasi dogrulama
- `normalizers/` — Tarih, oran, kategori donusumleri
- `storage/manager.py` — ClickHouse atomic yazma (REPLACE PARTITION)
- `profiling/engine.py` — Gercek zamanli veri profilleme
- `tasks.py` — Celery task'lari (check_and_sync, run_sync)
- `clickhouse_manager.py` — DDL, tablo olusturma, baglanti yonetimi
- `metrics.py` — Prometheus metrikleri

### api (REST API)
Programatik erisim:
- `authentication.py` — API key dogrulama (cache destekli)
- `permissions.py` — Tenant izolasyon kontrolu
- `middleware.py` — PostgreSQL search_path yonetimi
- `views.py` — Sync, Data, Profiling, Error endpoint'leri
- `serializers.py` — DRF serializer'lar

### frontend (Web UI)
HTMX tabanli arayuz:
- Dashboard, Upload, Sync, Data View, Profiling, Errors, Settings sayfalari
- HTMX ile partial page update (tam sayfa yenileme yok)
- Chart.js ile gorsellestime

### external_bank (Banka Simulatoru)
CSV verilerini Redis'te saklar, sync sirasinda SyncEngine tarafindan okunur:
- `storage.py` — Gzip sikistirma, Redis pipeline, TTL yonetimi
- `views.py` — Upload/Retrieve REST endpoint'leri

### core (Yardimci)
- `cache.py` — Redis cache katmani (safe wrapper, TTL, invalidation)

### config (Konfiguration)
- `settings.py` — Django ayarlari
- `celery.py` — Celery app + Beat schedule
- `db_router.py` — Schema-based routing
- `urls.py` — URL routing

---

## Sync Pipeline

```
CSV Upload (Web UI / API)
       │
       ▼
Redis DB1 (gzip ile sakla)
  key: extbank:{tenant}:{loan_type}:{file_type}
       │
       │  ◄── Celery Beat her 60s kontrol eder (get_row_count)
       │
       ▼
┌─────────────────────────────────────────────┐
│              SyncEngine.sync()               │
│                                              │
│  1. FETCH                                    │
│     DataFetcher → Redis'ten credit +         │
│     payment kayitlarini oku                  │
│                                              │
│  2. VALIDATE                                 │
│     ├─ Field Validation                      │
│     │   ├ CreditFieldValidator               │
│     │   └ PaymentFieldValidator              │
│     │   (zorunlu alan, tip, aralik, format)  │
│     │                                        │
│     └─ Cross-File Validation                 │
│        └ Odeme kayitlari → kredi referansi   │
│          (batch + ClickHouse union)          │
│                                              │
│  3. ERROR CHECK                              │
│     Hata orani > %50 ise ABORT               │
│     (eski veri korunur)                      │
│                                              │
│  4. NORMALIZE                                │
│     ├ Tarih: 20240115 → 2024-01-15          │
│     ├ Oran: 18.5 → 0.185                    │
│     └ Kategori: A → ACTIVE, I → INDIVIDUAL  │
│                                              │
│  5. STORE (Atomic)                           │
│     TRUNCATE staging → INSERT → REPLACE      │
│     PARTITION → TRUNCATE staging             │
│                                              │
│  6. CLEANUP                                  │
│     ├ Basarisiz kayitlari Redis'e tasi       │
│     │   (extbank_failed:* , 72s TTL)         │
│     └ Upload verisini sil                    │
│                                              │
│  7. LOG + METRICS                            │
│     ├ SyncLog kaydi (PostgreSQL)             │
│     ├ ValidationError kayitlari              │
│     ├ Prometheus metrikleri                  │
│     └ Cache invalidation                     │
└─────────────────────────────────────────────┘
       │
       ▼
ClickHouse fact_credit / fact_payment
```

---

## Redis Mimarisi

```
Redis :6379
├── DB 0 — Celery Broker + Result Backend
│   ├ Task mesajlari (kuyruk)
│   └ Task sonuclari (JSON)
│
├── DB 1 — External Bank Staging
│   ├ extbank:{tenant}:{loan_type}:{file_type}
│   │   └ Gzip JSON blob (TTL: 24 saat)
│   ├ extbank:{tenant}:{loan_type}:{file_type}:count
│   │   └ Satir sayisi — O(1) kontrol (TTL: 24 saat)
│   └ extbank_failed:{tenant}:{loan_type}:{file_type}
│       └ Redis List — basarisiz kayitlar (TTL: 72 saat)
│
└── DB 2 — Django Cache
    ├ findata:{tenant}:tenant_auth:{prefix}     (5 dk)
    ├ findata:{tenant}:sync_configs             (2 dk)
    ├ findata:{tenant}:sync_logs:recent:{limit} (1 dk)
    ├ findata:{tenant}:ch_count:{table}:{type}  (5 dk)
    ├ findata:{tenant}:ch_schema:{table}        (1 saat)
    ├ findata:{tenant}:profile:{type}:{data}    (10 dk)
    ├ findata:{tenant}:val_errors:{log_id}      (30 dk)
    └ findata:{tenant}:existing_loans:{type}    (5 dk)
```

**Ozellikler:**
- Bellek limiti: 256 MB, eviction: `allkeys-lru`
- DB 1'de gzip sikistirma (~%50-60 kazanim)
- DB 1'de Redis Pipeline ile atomic islemler
- DB 2'de tum islemler try-except ile sarili (Redis down = DB'ye fallback)
- Sync sonrasi otomatik cache invalidation

---

## Authentication Akisi

```
Client                          Django                    Redis DB2         PostgreSQL
  │                               │                         │                  │
  │  Authorization: Api-Key sk_.. │                         │                  │
  ├──────────────────────────────►│                         │                  │
  │                               │  cache_get(prefix)      │                  │
  │                               ├────────────────────────►│                  │
  │                               │  cache hit?             │                  │
  │                               │◄────────────────────────┤                  │
  │                               │                         │                  │
  │                               │  [miss ise]             │                  │
  │                               │  SELECT * FROM tenants  │                  │
  │                               │  WHERE api_key_prefix=? │                  │
  │                               ├────────────────────────────────────────────►│
  │                               │◄────────────────────────────────────────────┤
  │                               │                         │                  │
  │                               │  check_password(key, hash)                 │
  │                               │  set_current_tenant_schema(pg_schema)      │
  │                               │                         │                  │
  │         200 OK / 403          │                         │                  │
  │◄──────────────────────────────┤                         │                  │
```

**API Key formati:** `sk_live_<48 hex karakter>` (toplam 56 karakter)
- Veritabaninda sadece hash saklanir (PBKDF2-SHA256)
- Prefix (ilk 16 karakter) hizli arama icin indexli
- Cache hit'te bile password verify yapilir

---

## ClickHouse Storage Stratejisi

### Atomic Replacement (REPLACE PARTITION)

```
1. TRUNCATE staging_credit
       ↓
2. INSERT INTO staging_credit (normalized records)
       ↓
3. ALTER TABLE fact_credit
   REPLACE PARTITION 'RETAIL'
   FROM staging_credit
       ↓
4. TRUNCATE staging_credit
```

- **ReplacingMergeTree** engine, `loaded_at` version column
- Partition by `loan_type` (RETAIL / COMMERCIAL)
- Yeni yukleme eskisinin tamamen yerine gecer (append degil)
- Basarisiz sync'te eski veri korunur

### Tablo Yapisi

**fact_credit** (35 kolon):
- Kredi hesap numarasi, musteri bilgileri, tutar, faiz, vade, durum
- Decimal(18,2) para alanlari, Decimal(10,6) oran alanlari

**fact_payment** (16 kolon):
- Taksit detaylari, odeme tutarlari, tarihleri

---

## Validation Katmani

### Alan Dogrulamasi (Field Validation)

```
Her satir icin:
├─ Zorunlu alan kontrolu (REQUIRED)
├─ Tip kontrolu — integer, decimal, date (TYPE)
├─ Aralik kontrolu — min/max deger (RANGE)
├─ Format kontrolu — tarih formati (FORMAT)
└─ Deger kontrolu — enum, gecerli kodlar (VALUE)
```

**Kredi-spesifik:** loan_account_number, customer_id, amounts, rates, dates, status codes
**Odeme-spesifik:** installment_number, payment amounts, dates
**Loan-type spesifik:** RETAIL (insurance_included), COMMERCIAL (sector_code, loan_product_type)

### Dosyalar Arasi Dogrulama (Cross-File Validation)

```
Odeme kayitlari                    Kredi kayitlari
┌────────────────┐                ┌────────────────┐
│ loan_acc: L001 │───referans───►│ loan_acc: L001 │ ✓
│ loan_acc: L002 │───referans───►│ loan_acc: L002 │ ✓
│ loan_acc: L999 │───referans───►│       ???       │ ✗ CROSS_REFERENCE
└────────────────┘                └────────────────┘
                                         +
                                  ClickHouse'taki
                                  mevcut kayitlar
```

- Batch'teki + ClickHouse'taki kredi kayitlarinin union'u alinir
- Odeme kayitlari bu set'e referans vermeli

---

## Normalization Kurallari

| Alan | Girdi | Cikti | Ornek |
|------|-------|-------|-------|
| Tarih | YYYYMMDD | YYYY-MM-DD | 20240115 → 2024-01-15 |
| Oran | Yuzde (>1.0) | Ondalik | 18.5 → 0.185 |
| Musteri Tipi | Kod | Tam isim | I → INDIVIDUAL, T → TRADE |
| Kredi Durumu | Kod | Tam isim | A → ACTIVE, K → CLOSED |
| Sigorta | Kod | Boolean | H → 0, E → 1 |

---

## Monitoring ve Observability

```
┌──────────────┐     ┌───────────────┐     ┌──────────────┐
│ Django App   │────►│  Prometheus   │────►│   Grafana    │
│ /metrics     │     │  :9090        │     │   :3000      │
└──────────────┘     └───────┬───────┘     └──────────────┘
                             │
              ┌──────────────┼──────────────┐
              │              │              │
     ┌────────▼──┐  ┌───────▼───┐  ┌───────▼───┐
     │ PG Export  │  │ Redis Exp │  │  cAdvisor  │
     │ :9187      │  │ :9121     │  │  :8081     │
     └────────────┘  └───────────┘  └────────────┘
```

### Prometheus Metrikleri
- `sync_operations_total` — tenant, loan_type, status bazinda
- `sync_duration_seconds` — histogram
- `validation_errors_total` — hata sayaci
- `clickhouse_rows_inserted_total` — eklenen satir sayisi
- `data_upload_bytes_total` — yuklenen veri boyutu
- HTTP request latency, status code dagilimi (django-prometheus)

### Alert Kurallari
- HTTP 5xx > %5
- P95 latency > 2s
- Sync hata orani > %30
- PostgreSQL connection > %85
- Redis bellek > %80
- Container CPU > %80

---

## Celery Beat Zamanlayici

```
Her 60 saniye:
Celery Beat
    │
    ▼
check_and_sync()
    │
    ├─ Tenant.objects.filter(is_active=True)
    │   └─ Her tenant icin:
    │       └─ SyncConfiguration.objects.filter(is_enabled=True)
    │           └─ Her config icin:
    │               ├─ get_row_count(tenant, loan_type, 'credit')
    │               └─ get_row_count(tenant, loan_type, 'payment_plan')
    │                   └─ Veri varsa → run_sync.delay(tenant_id, loan_type)
    │
    ▼
run_sync(tenant_id, loan_type)
    └─ SyncEngine(tenant_id, pg_schema, ch_database, url)
        └─ engine.sync(loan_type)
```

- `get_row_count()` Redis'te O(1) islem (ayri counter key)
- Kor sync yok: sadece veri varsa tetiklenir
- `run_sync` max 2 retry, 60s aralikla

---

## REST API Endpoint'leri

| Method | Endpoint | Aciklama |
|--------|----------|----------|
| POST | `/api/sync/` | Sync tetikle `{tenant_id, loan_type}` |
| GET | `/api/sync/configs/` | Sync konfigurasyonlari |
| GET | `/api/sync/configs/<id>/` | Tek konfigurasyon |
| GET | `/api/sync/logs/` | Sync log listesi |
| GET | `/api/sync/logs/<uuid>/` | Tek sync log detayi |
| GET | `/api/sync/logs/<uuid>/errors/` | Sync'e ait hatalar |
| GET | `/api/data/?tenant_id=X&loan_type=Y` | ClickHouse verileri |
| GET | `/api/profiling/?tenant_id=X&loan_type=Y` | Veri profili |

Tum endpoint'ler `Authorization: Api-Key sk_live_...` header'i gerektirir.

---

## Servisler ve Portlar

| Servis | Port | Teknoloji | Gorev |
|--------|------|-----------|-------|
| web | 8000 | Django + Gunicorn | Web uygulamasi |
| celery-worker | — | Celery | Task isci |
| celery-beat | — | Celery Beat | Zamanlayici (60s) |
| db | 5432 | PostgreSQL 16 | Operasyonel DB |
| clickhouse | 8123 | ClickHouse | Data Warehouse |
| redis | 6379 | Redis 7 | Cache + Queue + Stage |
| prometheus | 9090 | Prometheus | Metrik toplama |
| grafana | 3000 | Grafana | Dashboard |
| cadvisor | 8081 | cAdvisor | Container metrikleri |
| postgres-exporter | 9187 | PG Exporter | PG metrikleri |
| redis-exporter | 9121 | Redis Exporter | Redis metrikleri |

---

## Tech Stack

| Katman | Teknoloji |
|--------|-----------|
| Framework | Django 5.x + DRF |
| Operasyonel DB | PostgreSQL 16 (schema-based multi-tenancy) |
| Data Warehouse | ClickHouse (ReplacingMergeTree) |
| Cache / Queue / Stage | Redis 7 (3 DB) |
| Task Queue | Celery 5.3 + Celery Beat |
| Web Server | Gunicorn |
| Frontend | Django Templates + HTMX + Chart.js |
| Monitoring | Prometheus + Grafana |
| Container | Docker + Docker Compose |
| Dil | Python 3.12 |
