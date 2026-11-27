# ----- Service Account -----
resource "google_service_account" "de_sa" {
  account_id   = var.service_account_id
  display_name = "Chicago Crime Data Engineer SA"
  description  = "Service account for the Chicago Crime data platform"
}

# ----- Rôles IAM -----
locals {
  roles = [
    "roles/storage.admin",
    "roles/bigquery.admin",
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser",
    "roles/bigquery.readSessionUser",
  ]
}

resource "google_project_iam_member" "sa_roles" {
  for_each = toset(local.roles)
  project  = var.project_id
  role     = each.value
  member   = "serviceAccount:${google_service_account.de_sa.email}"
}

# ----- Clé du Service Account -----
resource "google_service_account_key" "de_sa_key" {
  service_account_id = google_service_account.de_sa.name
  public_key_type    = "TYPE_X509_PEM_FILE"
}
