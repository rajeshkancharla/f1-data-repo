# F1 Analytics Pipeline - Architecture Documentation

## Overview

An ELT pipeline that extracts Formula 1 data from OpenF1 API, loads it into BigQuery, and transforms it using dbt for analytics.

**Tech Stack:** Python → BigQuery → dbt (transformations & tests) → GitHub Actions

---

## 1. Orchestration

### Current Solution: GitHub Actions
- Simple to configure and maintain
- Integrates directly with our code repository
- Free for our use case
- Runs automatically on code changes or schedule

**Pipeline Workflow:**
```
1. Trigger (push to main, daily schedule, or manual)
2. Extract data from OpenF1 API (Python script)
3. Load data into BigQuery raw tables
4. Run dbt models (staging → dimensions → facts)
5. Run dbt tests (28+ data quality checks)
6. Send notifications on failure
```

**Key Files:**
- `.github/workflows/dbt-ci.yml` - Main production pipeline

### Alternative for Future: Airflow / Dagster
If we need more complex scheduling or visual DAGs later, we can migrate to something like AirFlow or Dagster

---

## 2. Ingestion

### Data Extraction

**Script:** `run_data_extraction.py`

**What it does:**
1. Connects to OpenF1 API
2. Extracts 4 datasets: drivers, laps, locations, pit stops
3. Writes to BigQuery using MERGE (idempotent upserts)

**Two extraction modes:**
```bash
# Mode 1: By session ID
python run_data_extraction.py --session_key 9472

# Mode 2: By race (auto-finds session)
python run_data_extraction.py --country "Singapore" --year 2024
```

### Idempotency

**How we prevent duplicates:**
- Use `MERGE` statements instead of `INSERT`
- Primary keys ensure same data doesn't load twice:
  - Drivers: `(session_key, driver_number)`
  - Laps: `(session_key, driver_number, lap_number)`
  - Locations: `(session_key, driver_number, date)`
  - Pit Stops: `(session_key, driver_number, lap_number)`

**Example:**
```sql
MERGE INTO raw_table T
USING new_data S
ON T.session_key = S.session_key 
   AND T.driver_number = S.driver_number
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...
```

This means we can safely re-run extractions without creating duplicates.

### Error Handling

**Challenge:** OpenF1 API has rate limits and sometimes rejects large requests.

**Solution:**
- Retry failed requests 3 times with backoff
- For locations (high volume), break into 5-minute time windows
- Log all errors for debugging

---

## 3. Transformation & Storage

### dbt Project Structure

```
models/
├── staging/          # Clean raw data (views)
│   ├── stg_drivers.sql
│   ├── stg_laps.sql
│   ├── stg_locations.sql
│   └── stg_pit.sql
│
└── marts/     # Dimension tables (tables)
    ├── dim_drivers.sql
    ├── dim_sessions.sql
    └── fct_lap_summary.sql
```

### Data Model

**Star Schema Design:**

**Dimensions (who/what/where):**
- `dim_drivers` - Driver profiles (name, team, country)
- `dim_sessions` - Race sessions (circuit, date, type)

**Facts (measurements):**
- `fct_lap_summary` - Aggregated lap statistics

---

## 4. Testing & Monitoring

### Data Quality Tests (28+ tests)

**Layer 1: Source freshness**
- Alert if raw data is >24 hours old

**Layer 2: Schema validation**
- Check for required columns
- Validate data types

**Layer 3: Data integrity**
- No null values in key fields
- No duplicate records
- Valid relationships between tables

**Layer 4: Business logic**
- Lap times are positive
- Speed values are realistic
- Lap numbers are sequential

---

## 5. Scaling & Governance

### Current Scale

**Single race:**
- Extraction time: ~2-5 minutes
- Storage: ~500 MB raw data

**Full season (23 races):**
- Extraction time: ~1-2 hours (sequential)
- Storage: ~11.5 GB

### Scaling Options

**If we need faster processing:**

**Option 1: Parallel extraction** (easiest)
- Run multiple sessions in parallel using GitHub Actions matrix
- 10x faster with no code changes

**Option 2: Cloud Run Jobs** (for many races)
- Serverless container execution
- True horizontal scaling
- Pay only for execution time

**Option 3: Streaming** (for real-time)
- Use Pub/Sub + Dataflow for live race data
- Sub-30 second latency

### Data Governance

**Access Control:**
- Data Engineers: Read/write to all tables
- Analysts: Read-only on transformed tables
- Service accounts: Minimal permissions needed

**Data Retention:**
- Raw data: 1 year (auto-delete after)
- Transformed data: Keep indefinitely for historical analysis
- Logs: 90 days

**Documentation:**
- dbt auto-generates docs with lineage graphs
- View with: `dbt docs generate && dbt docs serve`

---

## 6. Production Deployment

### Architecture Diagram

```
GitHub (Code) 
    ↓
GitHub Actions (Orchestration)
    ↓
Python Script (Extraction)
    ↓
BigQuery Raw Tables (f1_raw_data)
    ↓
dbt Models (Transformation)
    ↓
BigQuery Analytics Tables (staging, marts)
    ↓
BI Tools (Looker, Tableau, etc.)
```

### Deployment Steps

1. **Setup GCP:**
   - Create BigQuery datasets: `f1_raw_data`, `f1_analytics`
   - Create service account with BigQuery permissions
   - Save service account key as GitHub secret

2. **Configure GitHub Actions:**
   - Add `GCP_SA_KEY` secret
   - Enable workflows in repository settings

3. **Run pipeline:**
   - Push code to main branch → automatic run
   - Or trigger manually from GitHub Actions UI

### Cost Estimate

**Monthly costs (full production):**
- BigQuery storage: $5-10
- BigQuery queries: $5-15
- GitHub Actions: Free (or ~$5 for private repos)
- **Total: ~$10-30/month**

---

## 7. Alternative Cloud Platforms

**Current: BigQuery (Google Cloud)**
- Best for: SQL-heavy analytics workloads
- Cost: Lowest
- Performance: Good

**Alternative 1: Snowflake**
- Better for: Complex joins, time travel feature
- Cost: 2-3x BigQuery
- Migration: Swap `dbt-bigquery` for `dbt-snowflake`

**Alternative 2: Databricks**
- Better for: Large-scale data processing, ML workflows
- Cost: 3-5x BigQuery
- Migration: Use Delta Lake format, `dbt-databricks`

---


**Project structure:**
```
├── run_data_extraction.py       # Extraction script
├── data_ingestion/
│   └── data_extractor.py        # Extraction logic
├── dbt_project/
│   └── models/                  # dbt models
├── .github/workflows/           # CI/CD pipelines
└── docs/architecture.md         # This file
```

---
