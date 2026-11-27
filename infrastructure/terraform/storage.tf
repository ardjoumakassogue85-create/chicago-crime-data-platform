# ----- Bucket principal (Data Lake) -----
resource "google_storage_bucket" "data_lake" {
  name          = var.bucket_name
  location      = var.region
  force_destroy = true  # permet de détruire même s'il y a des objets

  uniform_bucket_level_access = true

  # Versioning (garder l'historique des versions de fichiers)
  versioning {
    enabled = true
  }

  # Lifecycle : déplacer en Coldline après 365 jours, Archive après 730
  lifecycle_rule {
    condition {
      age = 365
    }
    action {
      type          = "SetStorageClass"
      storage_class = "COLDLINE"
    }
  }

  lifecycle_rule {
    condition {
      age = 730
    }
    action {
      type          = "SetStorageClass"
      storage_class = "ARCHIVE"
    }
  }
}

# ----- Dossiers logiques (préfixes) -----
locals {
  folders = ["raw", "bronze", "silver", "gold", "scripts", "logs"]
}

resource "google_storage_bucket_object" "folders" {
  for_each = toset(local.folders)
  name     = "${each.value}/"
  bucket   = google_storage_bucket.data_lake.name
  content  = ""
}

# ----- Bucket pour le state Terraform -----
resource "google_storage_bucket" "tfstate" {
  name          = var.tfstate_bucket
  location      = var.region
  force_destroy = true
  uniform_bucket_level_access = true
}
