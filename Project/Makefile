# Auto-load .env
ifneq (,$(wildcard .env))
  include .env
  export
endif

# Use the terraform.exe already in the repo root
TERRAFORM := ../terraform.exe

# Use the local venv managed by uv
PYTHON    := .venv/Scripts/python
DBT       := .venv/Scripts/dbt
STREAMLIT := .venv/Scripts/streamlit

.PHONY: help setup infra-up infra-down ingest spark kestra-up kestra-down dbt-run dbt-test dashboard all

help:
	@echo ""
	@echo "NBA Home Court Advantage — Pipeline Commands"
	@echo "--------------------------------------------"
	@echo "  make setup         Create venv and install all dependencies"
	@echo "  make infra-up      Provision GCS bucket + BigQuery datasets with Terraform"
	@echo "  make infra-down    Destroy Terraform-managed infrastructure"
	@echo "  make ingest        Download NBA data from Kaggle → GCS raw/"
	@echo "  make spark         Spark transform → GCS processed/ (runs in Docker)"
	@echo "  make kestra-up     Start Kestra orchestrator (localhost:8080)"
	@echo "  make kestra-down   Stop Kestra"
	@echo "  make dbt-run       Run dbt models against BigQuery"
	@echo "  make dbt-test      Run dbt tests"
	@echo "  make dashboard     Start Streamlit dashboard (localhost:8501)"
	@echo "  make all           Full pipeline: infra → ingest → spark → dbt"
	@echo ""

# ── Setup ─────────────────────────────────────────────────────────────────────
setup:
	uv venv
	uv pip install -e .
	@echo ""
	@echo "Activate with:  source .venv/Scripts/activate"

# ── Infrastructure ─────────────────────────────────────────────────────────────
infra-up:
	cd terraform && $(TERRAFORM) init && $(TERRAFORM) apply -auto-approve

infra-down:
	cd terraform && $(TERRAFORM) destroy -auto-approve

# ── Ingestion (runs locally in venv) ──────────────────────────────────────────
ingest:
	GOOGLE_APPLICATION_CREDENTIALS=$(GCP_CREDS_PATH) \
	$(PYTHON) ingestion/ingest.py \
		--gcs-bucket $(GCS_BUCKET) \
		--kaggle-user $(KAGGLE_USERNAME) \
		--kaggle-key $(KAGGLE_KEY)

# ── Spark transform (runs in Docker — needs GCS connector JAR) ─────────────────
spark:
	docker compose --profile spark run --rm --build spark

# ── Kestra ─────────────────────────────────────────────────────────────────────
kestra-up:
	cd kestra && docker compose up -d
	@echo "Kestra UI  → http://localhost:8080"
	@echo "Login      → admin@kestra.io / Admin1234!"

kestra-down:
	cd kestra && docker compose down

# ── dbt (runs locally in venv) ─────────────────────────────────────────────────
dbt-run:
	cd dbt && GOOGLE_APPLICATION_CREDENTIALS=$(GCP_CREDS_PATH) \
	$(CURDIR)/$(DBT) deps && \
	GOOGLE_APPLICATION_CREDENTIALS=$(GCP_CREDS_PATH) \
	$(CURDIR)/$(DBT) run --profiles-dir .

dbt-test:
	cd dbt && GOOGLE_APPLICATION_CREDENTIALS=$(GCP_CREDS_PATH) \
	$(CURDIR)/$(DBT) test --profiles-dir .

# ── Dashboard (runs locally in venv) ──────────────────────────────────────────
dashboard:
	GOOGLE_APPLICATION_CREDENTIALS=$(GCP_CREDS_PATH) \
	GCP_PROJECT=$(GCP_PROJECT) \
	BQ_DATASET=$(BQ_DATASET) \
	$(STREAMLIT) run dashboard/app.py

# ── Full pipeline ──────────────────────────────────────────────────────────────
all: infra-up ingest spark dbt-run
	@echo "Pipeline complete. Run 'make dashboard' to view results."
