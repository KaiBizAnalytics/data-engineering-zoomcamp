variable "credentials" {
  description = "Path to GCP service account JSON key file"
  default     = "../../terrademo/keys/my-creds.json"
}

variable "project" {
  description = "GCP project ID"
  default     = "sanguine-mark-366002"
}

variable "region" {
  description = "GCP region"
  default     = "us-west1"
}

variable "location" {
  description = "GCS bucket and BigQuery dataset location"
  default     = "US"
}
