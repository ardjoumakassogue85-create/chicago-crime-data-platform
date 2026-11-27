output "bucket_url" {
  value       = "gs://${google_storage_bucket.data_lake.name}"
  description = "URL of the data lake bucket"
}

output "service_account_email" {
  value       = google_service_account.de_sa.email
  description = "Email of the service account"
}

output "service_account_key" {
  value       = google_service_account_key.de_sa_key.private_key
  description = "Private key for the service account (base64 encoded)"
  sensitive   = true
}

output "datasets" {
  value = {
    staging = google_bigquery_dataset.staging.dataset_id
    silver  = google_bigquery_dataset.silver.dataset_id
    marts   = google_bigquery_dataset.marts.dataset_id
  }
  description = "BigQuery datasets created"
}
