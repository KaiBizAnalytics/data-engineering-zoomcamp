# NBA Home Court Advantage — Does It Still Matter?

An end-to-end data engineering project analyzing 70+ years of NBA game results to measure home court advantage — and whether the 2019-20 COVID bubble season (zero fans) changed it.

---

## Problem Description

Home court advantage is one of the most cited statistics in basketball. Home teams historically win around 58–60% of games. But the *reason* has always been debated: Is it the crowd? Travel fatigue? Referee bias? Familiarity with the arena?

The 2019-20 NBA season created a rare natural experiment. Due to COVID-19, the season was completed inside a quarantine "bubble" at Walt Disney World in Orlando — with **zero fans in attendance**. Every game was played on a neutral court, stripping away the crowd factor entirely.

This project builds a complete data pipeline to answer:

1. **How strong is home court advantage historically?** — measured across all 30 franchises from 1946 to 2024
2. **Did it collapse during the bubble?** — comparing 2019-20 against the historical baseline
3. **Does crowd attendance correlate with home advantage?** — tracking attendance figures alongside win rates over time
4. **Which teams relied most on home crowd support?** — identifying franchises where the bubble had the biggest impact

The dataset comes from the [wyattowalsh/basketball](https://www.kaggle.com/datasets/wyattowalsh/basketball) Kaggle dataset: ~65,000 regular season games going back to the NBA's founding season.

---

## Architecture

```
Kaggle API
    │
    ▼
[Python Ingestion]  ← ingest.py (Click CLI)
    │  game.csv + game_info.csv → Parquet
    ▼
GCS Bucket  gs://sanguine-mark-366002-nba-lake/raw/
    │                                              ← Data Lake (raw layer)
    ▼
[PySpark Transform]  ← transform.py
    │  Filter regular season, add season_year,
    │  home_win flag, is_bubble_season flag,
    │  join attendance from game_info
    ▼
GCS Bucket  gs://.../processed/game_cleaned.parquet
    │                                              ← Data Lake (processed layer)
    ▼
BigQuery  nba_raw.games                            ← Data Warehouse (raw)
    │  Partitioned by game_date (MONTH)
    │  Clustered by team_abbreviation_home, season_year
    ▼
[dbt]
    │  stg_games (view)  →  mart_home_court_advantage (table)
    ▼
BigQuery  nba_dbt_marts.mart_home_court_advantage  ← Data Warehouse (analytics)
    │  Partitioned by season_year (INT64 range)
    │  Clustered by team_abbr
    ▼
[Streamlit Dashboard]  ← app.py
    4 interactive tiles

[Kestra]     orchestrates all pipeline steps  ← Workflow Orchestration
[Terraform]  provisions GCS + BigQuery        ← IaC
```

---

## Tech Stack

| Layer | Tool |
|---|---|
| Infrastructure (IaC) | Terraform (GCP provider) |
| Cloud Platform | Google Cloud Platform |
| Data Lake | Google Cloud Storage |
| Workflow Orchestration | Kestra (self-hosted, Docker) |
| Batch Processing | Apache Spark (PySpark 3.5) |
| Data Warehouse | BigQuery |
| Transformations | dbt-bigquery 1.8 |
| Dashboard | Streamlit + Plotly |
| Containerization | Docker / Docker Compose |
| Language | Python 3.12 |

---

## Dataset

**Source:** [wyattowalsh/basketball on Kaggle](https://www.kaggle.com/datasets/wyattowalsh/basketball)

| File | Rows | Description |
|---|---|---|
| `game.csv` | ~65,700 | One row per game: teams, scores, W/L results, season ID |
| `game_info.csv` | ~58,000 | Game metadata including attendance figures |

Both files are joined on `game_id` to enrich game results with attendance data.

---

## Cloud Infrastructure

All cloud resources are provisioned with **Terraform** (`terraform/main.tf`):

- **GCS bucket** `sanguine-mark-366002-nba-lake` — stores raw and processed Parquet files
- **BigQuery dataset** `nba_raw` — landing zone for raw game data
- **BigQuery dataset** `nba_dbt_marts` — analytics-ready dbt output tables

```bash
make infra-up    # terraform init + apply
make infra-down  # terraform destroy
```

---

## Data Ingestion & Orchestration

The pipeline has **4 steps orchestrated end-to-end by Kestra**:

| Step | Task | Output |
|---|---|---|
| 1 | Python ingest from Kaggle | `raw/game.parquet`, `raw/game_info.parquet` in GCS |
| 2 | PySpark transform | `processed/game_cleaned.parquet` in GCS |
| 3 | BigQuery load | `nba_raw.games` (partitioned + clustered) |
| 4 | dbt run + test | `nba_dbt_marts.mart_home_court_advantage` |

Kestra runs locally via Docker Compose (`kestra/docker-compose.yaml`). The flow definition is at `kestra/flows/nba_pipeline.yaml`. Each step runs in its own Docker container so there are no dependency conflicts between the Python ingestion, Spark, and dbt environments.

The pipeline is scheduled to run **every Monday at 06:00 UTC** — pulling the latest data from Kaggle, re-processing with Spark, reloading BigQuery, and rebuilding the dbt models. This makes it a proper weekly batch pipeline that stays current as the Kaggle dataset is updated with new game results.

---

## Data Warehouse

### Raw table: `nba_raw.games`

Loaded directly from the processed Parquet via BigQuery's LoadFromGcs.

- **Partitioned by `game_date` (MONTH)** — dashboard queries filter by season year ranges, so month partitioning means BigQuery scans only the relevant date partitions rather than the full table
- **Clustered by `team_abbreviation_home`, `season_year`** — the two most common GROUP BY / filter columns in both the dbt models and ad-hoc queries

### Staging model: `nba_dbt_marts.stg_games` (view)

Casts all types, normalizes team abbreviations to uppercase, filters out rows missing `game_date`, `wl_home`, or team abbreviation. It's a view on top of the raw table — no storage cost.

### Mart table: `nba_dbt_marts.mart_home_court_advantage` (table)

Grain: **one row per (team, season_year)**. Aggregates home and away win rates, computes the delta, and joins league-wide averages.

- **Partitioned by `season_year` (INT64 range, 1946–2030)** — the dashboard's season year slider filters on this column directly
- **Clustered by `team_abbr`** — the team multiselect filter in the dashboard benefits from this

Key columns:

| Column | Description |
|---|---|
| `home_win_pct` | Fraction of home games won |
| `away_win_pct` | Fraction of away games won |
| `home_advantage_delta` | `home_win_pct − away_win_pct` (positive = home advantage exists) |
| `avg_attendance` | Average fans at home games that season |
| `league_home_win_pct` | League-wide baseline for comparison |
| `is_bubble_season` | `True` for 2019-20 (the fan-free season) |

---

## Transformations

**Spark** (`spark/transform.py`):
- Filters to regular season games only (`season_id` starts with `"2"`)
- Extracts `season_year` from the last 4 digits of `season_id`
- Adds `home_win` (1/0) and `is_bubble_season` (bool) columns
- Left-joins attendance data from `game_info`
- Drops rows missing critical fields

**dbt** (`dbt/models/`):
- `stg_games`: staging view with type casting and normalization
- `mart_home_court_advantage`: final analytics table with CTEs for home stats, away stats, and league averages
- dbt tests cover `not_null`, `unique`, and `accepted_values` on all key columns

---

## Dashboard

The Streamlit dashboard (`dashboard/app.py`) connects directly to BigQuery and presents 4 interactive tiles:

**Tile 1 — Home Win % by Team (All Time)**
Bar chart of career home win percentage for all 30 teams, sorted descending. A dashed red line marks the league average. Reveals which franchises have historically dominated at home.

**Tile 2 — League-Wide Home Win % by Season**
Line chart from 1946 to present. The 2019-20 bubble season is marked with a red star. This is the core visual: does removing fans visibly drop the line?

**Tile 3 — Fan Attendance vs Home Advantage Over Time**
Dual-axis chart: attendance bars (with the bubble year highlighted in red) overlaid with the home win % line. Shows whether attendance and home advantage move together historically.

**Tile 4 — Bubble Season Impact by Team**
Grouped bar chart comparing each team's normal-season home advantage delta vs their bubble-season delta. Teams that relied most on crowd energy show the biggest drop.

Sidebar filters: season year range slider + team multiselect apply to all four tiles simultaneously.

---

## Project Structure

```
Project/
├── terraform/              # IaC — GCS bucket + BigQuery datasets
│   ├── main.tf
│   └── variables.tf
├── ingestion/              # Kaggle download → GCS raw/
│   └── ingest.py
├── spark/                  # PySpark transform → GCS processed/
│   └── transform.py
├── kestra/                 # Workflow orchestration
│   ├── docker-compose.yaml
│   └── flows/
│       └── nba_pipeline.yaml
├── dbt/                    # Analytics transformations
│   ├── dbt_project.yml
│   ├── packages.yml
│   └── models/
│       ├── staging/
│       │   ├── sources.yml
│       │   ├── stg_games.sql
│       │   └── schema.yml
│       └── marts/
│           ├── mart_home_court_advantage.sql
│           └── schema.yml
├── dashboard/              # Streamlit app
│   └── app.py
├── requirements.txt
├── Makefile
└── .env.example
```

---

## How to Run

### Prerequisites

- Docker Desktop
- Python 3.12+
- `terraform.exe` in the repo root (or `terraform` on PATH)
- GCP service account JSON with roles: `BigQuery Admin`, `Storage Object Admin`
- Kaggle account with API credentials

### 1. Clone and configure

```bash
git clone <this-repo>
cd Project
cp .env.example .env
```

Edit `.env`:

```env
GCP_PROJECT=your-gcp-project-id
GCS_BUCKET=your-gcs-bucket-name
GCP_CREDS_PATH=/path/to/service-account.json
KAGGLE_USERNAME=your-kaggle-username
KAGGLE_KEY=your-kaggle-api-key
BQ_DATASET=nba_dbt_marts
```

### 2. Set up Python environment

```bash
python -m venv .venv
source .venv/Scripts/activate      # Windows
# source .venv/bin/activate        # Mac/Linux
pip install -r requirements.txt
```

### 3. Provision cloud infrastructure

```bash
make infra-up
```

Creates the GCS bucket and both BigQuery datasets via Terraform.

### 4. Run the pipeline

**Option A — run each step manually:**

```bash
make ingest      # Download game.csv + game_info.csv from Kaggle → GCS raw/
make spark       # Spark transform → GCS processed/
make dbt-run     # Build stg_games and mart_home_court_advantage in BigQuery
make dbt-test    # Run dbt data quality tests
```

**Option B — run all steps at once:**

```bash
make all
```

### 5. Run via Kestra (orchestrated end-to-end)

```bash
make kestra-up   # → http://localhost:8080  (admin@kestra.io / Admin1234!)
```

In the Kestra UI:

1. Go to **Namespaces → nba → KV Store** and add three keys:
   - `KAGGLE_USERNAME` — your Kaggle username
   - `KAGGLE_KEY` — your Kaggle API key
   - `GCP_SERVICE_ACCOUNT` — paste the full contents of your service account JSON file
2. Go to **Flows → Create** and paste the contents of `kestra/flows/nba_pipeline.yaml`
3. Click **Execute** — all 4 steps run automatically in Docker containers

### 6. View the dashboard

```bash
make dashboard   # → http://localhost:8501
```

---

## Key Findings

- **Home advantage is real and consistent:** League-wide home win rate sits between 58–62% across most of NBA history
- **The bubble effect is measurable:** The 2019-20 season shows a notable drop toward 50/50, consistent with crowd noise being a meaningful contributor
- **Teams vary significantly:** Historic franchises like Boston, San Antonio, and Utah show noticeably higher home win rates than the league average
- **Attendance tracks the advantage:** Seasons with higher attendance generally show stronger home advantage, supporting the crowd effect hypothesis
- **Post-COVID recovery:** Home win rates returned to near-historical norms once fans were allowed back into arenas
