# F1 Analytics Pipeline

A production-ready ELT pipeline that extracts Formula 1 telemetry data from the OpenF1 API and transforms it into analytics-ready dimensional models in BigQuery.

**Tech Stack:** 
- Python
- BigQuery
- dbt
- GitHub Actions

---

## Features

**Idempotent data ingestion** - Safe to re-run, no duplicates  
**Dimensional modeling** - Star schema with fact and dimension tables  
**Automated CI/CD** - GitHub Actions for extraction, transformation, and testing  
**Comprehensive testing** - 28+ data quality tests  
**Production-ready** - Error handling, logging, monitoring  

---

## Quick Start

### Prerequisites

- Python 3.9+
- Google Cloud Platform account with BigQuery enabled
- dbt installed (`pip install dbt-bigquery`)

### Installation

```bash
# Clone the repository
git clone <your-repo-url>
cd f1-analytics-pipeline

# Install dependencies
pip install -r requirements.txt

# Configure GCP credentials
export GOOGLE_APPLICATION_CREDENTIALS="path/to/service-account-key.json"
```

### Run the Pipeline

**1. Extract F1 Data**
```bash
# Extract by race
python run_data_extraction.py --country "Singapore" --year 2024

# Or extract by session ID
python run_data_extraction.py --session_key 9472
```

**2. Transform with dbt**
```bash
cd dbt_project

# Run all models
dbt run

# Run tests
dbt test

# Generate documentation
dbt docs generate
dbt docs serve
```

---

## Project Structure

```
├── run_data_extraction.py       # Main extraction script
├── config.py                    # Configuration settings
├── logger.py                    # Custom logging
├── requirements.txt             # Python dependencies
│
├── data_ingestion/
│   └── data_extractor.py        # F1BigQueryExtractor class
│
├── dbt_project/
│   ├── models/
│   │   ├── staging/             # Staging views (raw data cleanup)
│   │   └── marts/               # Fact tables (analytics-ready)
│   ├── tests/                   # Custom dbt tests
│   └── dbt_project.yml
│
├── .github/
│   └── workflows/
│       └── dbt-ci.yml           # Production pipeline
│
├── docs/
│   └── architecture.md          # Architecture documentation
│
└── logs/                        # Extraction logs
```

---

## Data Model

**Star Schema Design:**

**Dimensions:**
- `dim_drivers` - Driver profiles (name, team, country)
- `dim_sessions` - Race sessions (circuit, date, type)

**Facts:**
- `fct_lap_summary` - Aggregated lap statistics

---

## CI/CD Pipeline

The pipeline runs automatically via GitHub Actions:

1. **Extract** data from OpenF1 API
2. **Load** into BigQuery raw tables (idempotent MERGE)
3. **Transform** using dbt (staging → dimensions → facts)
4. **Test** data quality (28+ tests)

**Triggers:**
- Push to `main` branch
- Pull requests
- Daily schedule (6 AM UTC)
- Manual workflow dispatch

---

## Configuration

### BigQuery Setup

1. Create two datasets in your GCP project:
   ```sql
   CREATE SCHEMA `your-project.f1_raw_data`;
   CREATE SCHEMA `your-project.f1_raw_data_staging`;
   CREATE SCHEMA `your-project.f1_raw_data_mart`;
   ```

2. Create a service account with these roles:
   - `roles/bigquery.dataEditor`
   - `roles/bigquery.jobUser`

3. Update `config.py`:
   ```python
   PROJECT_ID = "your-project-id"
   DATASET_ID = "f1_raw_data"
   ```

### dbt Configuration

Update `dbt_project/profiles.yml`:
```yaml
f1_project:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: service-account
      project: your-project-id
      dataset: f1_analytics
      keyfile: /path/to/service-account-key.json
      location: US
```

---

## Testing

**Run all tests:**
```bash
cd dbt_project
dbt test
```

**Test coverage:**
- Source freshness checks (4 sources)
- Schema validation (not_null, unique)
- Referential integrity (foreign keys)
- Business logic (lap times, speeds)

---

## Documentation

**[Architecture Documentation](docs/architecture.md)** - Detailed Architecture Documentation

**[DBT Documentation](docs/dbt.md)** - DBT Documentation with screenshots from dbt cloud portal

**[Big Query Documentation](docs/google_big_query.md)** - Google BigQuery screenshots from Google Cloud Platform Console

---

## Troubleshooting

**Issue: API returns 422 error**
- Solution: Add more specific filters (session_key, date range)
- The API rejects requests for too much data at once

**Issue: BigQuery permission denied**
- Verify service account has `bigquery.dataEditor` role
- Check `GOOGLE_APPLICATION_CREDENTIALS` is set correctly

---
