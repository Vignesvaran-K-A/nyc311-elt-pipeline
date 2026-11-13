# 🏙️ NYC 311 Service-Request Analytics Pipeline

A cloud-native data engineering pipeline that ingests, cleans, transforms, and visualizes NYC 311 public service request data. Built as a **batch ELT pipeline triggered on demand via GitHub Actions**, implementing the Medallion Architecture across Google Cloud BigQuery, dbt, and Streamlit.

---

## ⚡ Pipeline Execution & Test Metrics

* **dbt Transformations:** Builds `stg_nyc_311` (incremental staging), `mart_agency_performance` (table), and `mart_daily_volume` (table) in **11.19 seconds**.
* **Data Quality Verification:** **13 automated dbt tests** passing in **11.02 seconds** (`PASS=13 WARN=0 ERROR=0`), testing referential integrity, status domain values, null constraints, and custom logical assertions.
* **Environment Portability:** Fully decoupled configuration via environment variables (`GCP_PROJECT_ID`, `DBT_KEYFILE`, etc.) with zero hardcoded credentials or project IDs.
* **Incremental Processing with Late-Arriving Updates:** Staging layer merges on `complaint_id` with high-water mark tracking (`updated_at` / `resolution_action_updated_date`) and partition pruning predicates to capture downstream status transitions from Open to Closed without re-scanning historical tables.

---

## 🏗️ Architecture & Tech Stack

```text
┌─────────────────────────┐
│   NYC 311 Socrata API   │
└────────────┬────────────┘
             │  Python Ingestion (Dynamic Date Filtering & Pagination)
             ▼
┌────────────────────────────────────────────────────────┐
│  Google Cloud BigQuery: Bronze Layer (raw_nyc_311)     │
└────────────┬───────────────────────────────────────────┘
             │  dbt (Incremental Merge, Data Cleaning, Quality Tests)
             ▼
┌────────────────────────────────────────────────────────┐
│  Google Cloud BigQuery: Silver Layer                   │
│  ├─ stg_nyc_311             (Cleaned & Standardized)   │
│  ├─ mart_daily_volume       (Time-Grained Trend Mart)  │
│  └─ mart_agency_performance (Agency Performance Mart)  │
└────────────┬───────────────────────────────────────────┘
             │  BigQuery Connector (Parameterized Queries & Caching)
             ▼
┌─────────────────────────┐
│   Streamlit Dashboard   │
└─────────────────────────┘
```

* **Cloud Data Warehouse:** Google Cloud BigQuery
* **Data Transformation:** dbt (`dbt-bigquery`)
* **Ingestion & Automation:** Python, Docker, GitHub Actions CI/CD
* **Frontend Visualization:** Streamlit & Plotly Express

---

## 📊 Key Features & Pipeline Layers

1. **Bronze Layer (Ingestion):**
   * Python-based extractor (`extract_load.py`) that queries NYC Open Data (Socrata API).
   * Supports dynamic date filtering (`--start-date`, `--end-date`), rolling windows (`--days`), and configurable batch limits (`--max-records`).
   * Automatic pagination (25,000 records/page) prevents API truncation for multi-month data pulls.
   * Streams raw JSON records directly into BigQuery bronze tables with auto-schema detection (`WRITE_APPEND`).

2. **Silver Layer (Transformations & Modeling):**
   * **`stg_nyc_311`**: Cleans whitespace, converts empty strings to `NULL`, standardizes city casing, validates 5-digit ZIP codes, safely casts timestamps, and removes duplicate requests via window functions (`row_number() over (partition by unique_key order by created_date desc)`). Configured with BigQuery daily partitioning on `created_at` and clustering by `agency`.
   * **`mart_daily_volume`**: Aggregates daily complaint volumes with month truncations, open vs. closed counts, and average resolution times.
   * **`mart_agency_performance`**: Aggregates total complaint volumes, closed case tallies, and agency-level average resolution times in hours.
   * **Data Quality Tests**: 13 dbt schema tests (`unique`, `not_null`, `accepted_values`) plus singular custom SQL assertions (`assert_no_closed_before_created.sql`).

3. **CI/CD Automation (GitHub Actions):**
   * Containerized pipeline packaged with **Docker**.
   * Automated GitHub Actions workflow (`pipeline.yml`) with manual trigger (`workflow_dispatch`).
   * Allows engineers to specify custom start/end dates and full refresh flags directly from GitHub's UI.
   * Executes ingestion, connection verification, `dbt run`, and automated `dbt test` with step summary reporting.

4. **Interactive Dashboard (`app.py`):**
   * **High-Level KPIs**: Real-time tracking of total requests, closed cases, active cases, average resolution hours/days, and closure rates.
   * **Daily Volume Trends**: Time-series visualization with a 7-day rolling moving average to identify request spikes.
   * **Top Complaints Breakdown**: Ranked horizontal bar chart highlighting primary municipal complaint categories.
   * **Agency Performance Ranking**: Compares fastest and slowest resolving agencies using pre-aggregated dbt marts.
   * **Dynamic Filters**: Real-time filtering by created date range, municipal agency, and duration units (Hours vs. Days).

---

## 📁 Repository Structure

```text
nyc311-elt-pipeline/
├── .github/workflows/
│   └── pipeline.yml                        # GitHub Actions CI/CD ELT workflow
├── nyc_transform/                           # dbt project directory
│   ├── models/
│   │   ├── sources.yml                     # Bronze layer BigQuery source definitions
│   │   ├── schema.yml                      # Model documentation & 13 data tests
│   │   ├── staging/
│   │   │   └── stg_nyc_311.sql             # Incremental cleaned silver staging model
│   │   └── marts/
│   │       ├── mart_daily_volume.sql       # Time-grained daily metrics mart
│   │       └── mart_agency_performance.sql # Aggregated agency performance mart
│   ├── tests/
│   │   └── assert_no_closed_before_created.sql # Custom singular data assertion
│   ├── dbt_project.yml                     # dbt project & materialization config
│   └── README.md                           # dbt documentation
├── app.py                                  # Streamlit web application & dashboard
├── extract_load.py                         # Dynamic ingestion script (Socrata -> BigQuery)
├── Dockerfile                              # Container definition for pipeline execution
├── profiles.yml                            # dbt BigQuery connection profile
├── requirements.txt                        # Python project dependencies
├── results/                                # Dashboard screenshots & pipeline results
│   ├── dashboard_overview.png              # Live KPIs & daily volume trendlines
│   └── dashboard_breakdown.png             # Top complaints & agency performance rankings
└── README.md                               # Project documentation
```

---

## 🚀 Getting Started Locally

### 1. Clone the Repository

```bash
git clone https://github.com/Vignesvaran-K-A/nyc311-elt-pipeline.git
cd nyc311-elt-pipeline
```

### 2. Set Up Environment & Secrets

Create a `secrets/` directory in the root folder and add your GCP Service Account JSON key:

```text
secrets/gcp.json
```

Set the required environment variables:

* **Windows (PowerShell):**
  ```powershell
  $env:GOOGLE_APPLICATION_CREDENTIALS = "secrets/gcp.json"
  $env:GCP_PROJECT_ID = "your-gcp-project-id"
  ```
* **Linux / macOS:**
  ```bash
  export GOOGLE_APPLICATION_CREDENTIALS="secrets/gcp.json"
  export GCP_PROJECT_ID="your-gcp-project-id"
  ```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Run Data Ingestion (Extract & Load)

You can run the ingestion script with custom dates, rolling windows, or default to yesterday:

```bash
# Ingest data for a specific date range
python extract_load.py --start-date 2026-09-01 --end-date 2026-10-31 --max-records 50000

# Or ingest the last 30 days
python extract_load.py --days 30

# Or run with defaults (yesterday's data)
python extract_load.py
```

### 5. Run dbt Transformations & Tests

Execute dbt to incrementally process models and validate data quality:

```bash
# Incremental transformation run
dbt run --project-dir nyc_transform --profiles-dir .

# Rebuild all models from scratch
dbt run --project-dir nyc_transform --profiles-dir . --full-refresh

# Run all 13 data quality tests
dbt test --project-dir nyc_transform --profiles-dir .
```

### 6. Launch the Streamlit Dashboard

Start the local web dashboard:

```bash
streamlit run app.py
```

The interactive dashboard will open automatically in your browser at `http://localhost:8501`.

---

## ⚙️ Triggering the Pipeline via GitHub Actions

1. Navigate to the **Actions** tab in this GitHub repository.
2. Select **NYC 311 ELT Pipeline** from the workflow sidebar.
3. Click **Run workflow**.
4. (Optional) Provide:
   * **Start Date:** `YYYY-MM-DD` (e.g., `2026-09-01`)
   * **End Date:** `YYYY-MM-DD` (e.g., `2026-10-31`)
   * **Max Records:** e.g., `50000` (or `0` for unlimited)
   * **dbt Target:** `dev` or `prod`
   * **Full Refresh:** Check to force full recreation of incremental models
5. Click **Run workflow** to trigger the automated containerized build, ingestion, transformation, and testing.

---

## 📈 Dashboard Insights & Visuals

### Overview & Daily Volume Tracking
![NYC 311 Dashboard Overview](results/dashboard_overview.png)
* **Real-time KPI Metrics**: Tracks total volume, average resolution duration (hours/days), closed tickets, active backlog, and overall closure rates.
* **Daily Trend Analysis**: Visualizes daily complaint counts with a 7-day rolling average to identify operational spikes.

### Complaint Breakdown & Agency Performance
![NYC 311 Complaint Types & Agency Performance](results/dashboard_breakdown.png)
* **Top Complaint Categories**: Ranked horizontal breakdown of top municipal complaint types (e.g., residential noise, illegal parking, commercial noise).
* **Agency Resolution Efficiency**: Benchmarks municipal agency resolution speed powered by BigQuery silver layer marts (`mart_agency_performance`).

---

## 📜 License

This project is open-source and available under the [MIT License](LICENSE).
