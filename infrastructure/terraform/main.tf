# ----- Configuration Terraform -----
terraform {
  required_version = ">= 1.6.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
  
  # Stocker le state dans GCS (partagé, versionné)
  backend "gcs" {
    bucket = "chicago-crime-tfstate-kass"
    prefix = "terraform/state"
  }
}

# ----- Provider Google Cloud -----
provider "google" {
  project = var.project_id
  region  = var.region
}