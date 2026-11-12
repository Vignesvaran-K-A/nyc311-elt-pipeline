# NYC 311 — dbt Transformation Project

This is the **dbt** layer of the NYC 311 ELT pipeline. It reads from the
BigQuery bronze layer (`bronze_layer.raw_nyc_311`) and produces clean,
analytics-ready tables in the silver layer (`silver_layer`).

## Model Lineage

```
bronze_layer.raw_nyc_311          (source — Python ingestion)
        │
        ▼
staging/stg_nyc_311               (incremental, partitioned by day)
        │
        ▼
marts/mart_agency_performance     (table — pre-aggregated analytics)
```

## Directory Layout

```
nyc_transform/
├── models/
│   ├── sources.yml                   # Bronze source declaration
│   ├── schema.yml                    # Column docs + data tests
│   ├── staging/
│   │   └── stg_nyc_311.sql           # Cleaning, dedup, type casting
│   └── marts/
│       └── mart_agency_performance.sql  # Agency KPI aggregation
├── tests/
│   └── assert_no_closed_before_created.sql  # Custom singular test
├── macros/                           # Reserved for future helpers
└── dbt_project.yml                   # Project + materialisation config
```

## Materialisation Strategy

| Layer | Strategy | Why |
|---|---|---|
| `stg_nyc_311` | **incremental (merge)** | Avoids full-table rebuild on every daily run; merges new/updated complaints by `complaint_id` |
| `mart_agency_performance` | **table** | Small pre-aggregation; always rebuilt from the latest staging data |

## Running Locally

```bash
# Activate credentials
export GOOGLE_APPLICATION_CREDENTIALS="secrets/gcp.json"

# Run all models (incremental by default)
dbt run --project-dir nyc_transform --profiles-dir .

# Full rebuild (forces stg_nyc_311 to scan all bronze data)
dbt run --project-dir nyc_transform --profiles-dir . --full-refresh

# Run tests
dbt test --project-dir nyc_transform --profiles-dir .
```

## Data Tests Defined

| Model | Column | Test |
|---|---|---|
| `stg_nyc_311` | `complaint_id` | `unique`, `not_null` |
| `stg_nyc_311` | `created_at` | `not_null` |
| `stg_nyc_311` | `agency` | `not_null` |
| `stg_nyc_311` | `status` | `accepted_values` (warn) |
| `mart_agency_performance` | `agency` | `not_null` |
| `mart_agency_performance` | `total_complaints` | `not_null` |
| `mart_agency_performance` | `closed_complaints` | `not_null` |
| _(singular)_ | — | `assert_no_closed_before_created` |
