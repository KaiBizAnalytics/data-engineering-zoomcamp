terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.16.0"
    }
  }
}

provider "google" {
  credentials = file(var.credentials)
  project     = var.project
  region      = var.region
}

# GCS data lake bucket
resource "google_storage_bucket" "nba_lake" {
  name          = "${var.project}-nba-lake"
  location      = var.location
  force_destroy = true

  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 60
    }
    action {
      type = "Delete"
    }
  }
}

# BigQuery dataset for raw data (loaded from GCS)
resource "google_bigquery_dataset" "nba_raw" {
  dataset_id  = "nba_raw"
  location    = var.location
  description = "Raw NBA game data loaded from GCS"

  delete_contents_on_destroy = true
}

# BigQuery dataset for dbt-transformed analytics
resource "google_bigquery_dataset" "nba_dbt" {
  dataset_id  = "nba_dbt"
  location    = var.location
  description = "dbt-transformed NBA analytics models"

  delete_contents_on_destroy = true
}
