# Data Engineering Zoomcamp — My Learning Repo

Personal notes and code from the **[DataTalks.Club Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp)** (2026 Cohort, started January 12, 2026).

This repo tracks my hands-on work across all 7 modules — from spinning up containers to streaming pipelines in production-grade tools.

---

## Tech Stack

| Area | Tools |
|---|---|
| Containerization | Docker, Docker Compose |
| Infrastructure as Code | Terraform, Google Cloud Platform |
| Workflow Orchestration | Kestra |
| Data Warehousing | BigQuery |
| Analytics Engineering | dbt, DuckDB |
| Data Platforms | Bruin |
| Batch Processing | Apache Spark, PySpark |
| Streaming | Apache Kafka, Apache Flink |
| Languages | Python, SQL |

---

## Modules

### [Module 1 — Containerization & Infrastructure as Code](Module-01-docker-terraform/)
Docker fundamentals, Docker Compose, running PostgreSQL locally, and provisioning GCP resources with Terraform.

### [Module 2 — Workflow Orchestration](Module-02-workflow-orchestration/)
Building and scheduling data pipelines with Kestra. Covers data lake concepts and end-to-end orchestration flows.

### [Workshop — Data Ingestion with dlt](Homework/Module_2/)
Loading data from APIs with dlt (data load tool). Incremental loading, data normalization, and scalable ingestion patterns.

### [Module 3 — Data Warehousing](Module-03-data-warehouse/)
BigQuery deep dive: partitioning, clustering, cost optimization, and running ML models directly in BigQuery.

### [Module 4 — Analytics Engineering](taxi_rides_ny/)
Data modeling with dbt on the NYC Taxi dataset. Staging, intermediate, and mart layers, testing, documentation, and deployment to both DuckDB and BigQuery.

### [Module 5 — Data Platforms](Module-05-data-platforms/)
End-to-end pipelines with Bruin: ingestion, transformation, data quality checks, and cloud deployment.

### [Module 6 — Batch Processing](spark/)
Apache Spark and PySpark for large-scale batch processing. DataFrames, Spark SQL, GroupBy internals, and joins. Exercises use NYC Taxi and FHV trip data.

### [Module 7 — Streaming](Module-07-streaming/)
Real-time data pipelines with Apache Kafka and Apache Flink. Producers, consumers, schema management with Avro, and stateful stream processing.

---

## Homework

Completed homework assignments are in the [Homework/](Homework/) directory, organized by module. Each folder contains the problem statement and my solution.

---

## Capstone Project — NBA Home Court Advantage

**[→ Project/](Project/)**

An end-to-end data engineering pipeline applying all 7 modules to a single question:

> *Does home court advantage still matter in the NBA — and did the 2019-20 COVID bubble season (zero fans) prove that crowds are the real driver?*

Built on 70+ years of NBA game data from Kaggle (~65,000 games). The full pipeline runs weekly on a Kestra schedule, pulling the latest data through Spark, loading to BigQuery, rebuilding dbt models, and surfacing results in an interactive Streamlit dashboard.

| Layer | Tool |
|---|---|
| IaC | Terraform → GCS bucket + BigQuery datasets |
| Ingestion | Python (Kaggle API) → GCS raw/ |
| Batch Processing | PySpark → GCS processed/ |
| Orchestration | Kestra (weekly schedule, Docker) |
| Data Warehouse | BigQuery (partitioned + clustered) |
| Transformations | dbt-bigquery (staging view + mart table) |
| Dashboard | Streamlit + Plotly (4 tiles) |

---

## Course

- **Course repo:** [DataTalksClub/data-engineering-zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp)
- **Community:** [DataTalks.Club Slack](https://datatalks.club/slack.html) — `#course-data-engineering`
- **Playlist:** [YouTube](https://www.youtube.com/playlist?list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb)
