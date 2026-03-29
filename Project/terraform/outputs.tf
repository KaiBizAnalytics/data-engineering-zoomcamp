output "bucket_name" {
  description = "GCS data lake bucket name"
  value       = google_storage_bucket.nba_lake.name
}

output "bucket_url" {
  description = "GCS data lake bucket URL"
  value       = google_storage_bucket.nba_lake.url
}

output "raw_dataset" {
  description = "BigQuery raw dataset ID"
  value       = google_bigquery_dataset.nba_raw.dataset_id
}

output "dbt_dataset" {
  description = "BigQuery dbt dataset ID"
  value       = google_bigquery_dataset.nba_dbt.dataset_id
}
