# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Financial Data Integration Adapter for Asset-Backed Securities (ABS) analysis. This is a greenfield Django + PostgreSQL project with multi-tenant architecture. The full specification is in `Full-Stack Task.pdf` (Turkish).

The system connects to an external bank's web service, pulls loan portfolio data (CSV), validates, normalizes, and stores it in a data warehouse for analysis. The adapter polls the bank for new data at configurable intervals.

## Architecture

Five components, each mapping to a directory or service:

1. **`external_bank/`** — Simulated bank API (POST upload CSV, PUT/PATCH update, GET return JSON). Storage can be in-memory, JSON file, or simple DB.
2. **`adapter/`** — Core integration logic: validators, normalizers, and atomic data warehouse storage engine.
3. **`api/`** — Django REST API with three endpoints (all require JWT or API key auth):
   - `POST /api/sync` — body: `{tenant_id, loan_type}`
   - `GET /api/data?tenant_id=X&loan_type=Y`
   - `GET /api/profiling?tenant_id=X&loan_type=Y`
4. **Frontend** — Web UI for upload, sync trigger, data tables/charts, validation errors, and profiling dashboard.
5. **Monitoring** — Grafana + Prometheus, fully Dockerized.

## Multi-Tenancy

- 3 tenants: `BANK001`, `BANK002`, `BANK003`
- 2 loan types per tenant: `RETAIL`, `COMMERCIAL`
- Complete data isolation enforced at auth and storage layers
- Data warehouse key: `(tenant_id, loan_type)` — new data **replaces** old atomically (not append)
- If validation fails, old data is preserved unchanged

## Data Details

Sample data lives in `teamsec-interview-data/` (gitignored). Four CSV files:

| File | Type | Size |
|------|------|------|
| `retail_credit_masked.csv` | Credit records | ~3.8 MB |
| `retail_payment_plan_masked.csv` | Payment plans | ~22 MB |
| `commercial_credit_masked.csv` | Credit records | ~600 KB |
| `commercial_payment_plan_masked.csv` | Payment plans | ~1.9 MB |

### CSV Parsing Rules
- Delimiter is **semicolon** (`;`), not comma
- Credit files have **different column schemas** per loan type:
  - Commercial credits include: `loan_product_type`, `sector_code`, `risk_class`, `default_probability`, `customer_segment`, `customer_region_code`
  - Retail credits include: `insurance_included`, `customer_district_code`, `customer_province_code`
- Payment plan schemas are identical across retail/commercial

### Normalization Rules
- **Dates**: Mixed formats even within the same file — `YYYYMMDD` (e.g. `20250618`), `YYYY-MM-DD` (e.g. `2025-09-02`), `DD.MM.YYYY`. All must normalize to a single format. Actual payment dates can be empty (unpaid installments).
- **Rates**: `18.5%`, `0.185`, `1850 bps` — all must convert to decimal (e.g. `0.185`)
- **Status codes**: `"K"` / `"Kapalı"` / `"Paid"` = closed/paid; `"A"` = active. Must map to unified codes.

### Validation Rules
- Field-level: required fields present, correct types, values in valid ranges
- Cross-file: payment plan `loan_account_number` values must reference valid credit records
- Must handle files up to 200MB with streaming/chunked reads

## Data Profiling

The `/api/profiling` endpoint computes per-field statistics:
- **Numeric fields**: min, max, avg, stddev
- **Categorical fields**: unique count, most frequent value
- **All fields**: null/missing ratio percentage

## Key Test Scenarios (from spec)

1. Fetch BANK001 data — BANK002 data must NOT appear (tenant isolation)
2. Upload RETAIL + COMMERCIAL for one tenant — both retrievable independently
3. Upload 1000 credits, then upload 2000 — warehouse must have 2000 (not 3000, atomic replace)
4. Upload valid data, then upload invalid data — old valid data must be preserved


## Tech Stack

- **Backend**: Django, Django REST Framework
- **Database**: PostgreSQL (application data, auth, tenants)
- **Data Warehouse**: ClickHouse (loan/payment analytics storage)
- **Monitoring**: Grafana, Prometheus (Dockerized)
- **Containerization**: Docker / Docker Compose — **tüm servisler** (Django API, external_bank, PostgreSQL, ClickHouse, Grafana, Prometheus) Dockerize edilir ve `docker-compose` ile ayağa kaldırılır
- **Auth**: JWT or API key (tenant-scoped)
