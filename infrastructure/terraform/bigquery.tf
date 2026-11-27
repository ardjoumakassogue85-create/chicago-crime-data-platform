# ----- Dataset Silver -----
resource "google_bigquery_dataset" "silver" {
  dataset_id                 = "silver"
  location                   = var.region
  delete_contents_on_destroy = true
  
  description = "Silver layer: Data nettoyé de Spark ETL"
}

# ----- Dataset Staging -----
resource "google_bigquery_dataset" "staging" {
  dataset_id                 = "staging"
  location                   = var.region
  delete_contents_on_destroy = true
  
  description = "Staging views pour dbt"
}

# ----- Dataset Marts (Gold) -----
resource "google_bigquery_dataset" "marts" {
  dataset_id                 = "marts"
  location                   = var.region
  delete_contents_on_destroy = true
  
  description = "Marts: star schema tables (Gold)"
}

