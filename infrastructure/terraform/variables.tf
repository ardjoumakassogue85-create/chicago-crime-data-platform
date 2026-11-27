variable "project_id" {
  type        = string
  description = "GCP project ID"
}

variable "region" {
  type        = string
  description = "GCP region"
  default     = "europe-west1"
}

variable "bucket_name" {
  type        = string
  description = "GCS bucket pour la data lake"
}

variable "service_account_id" {
  type        = string
  description = "Service account ID"
  default     = "chicago-de-sa"
}

variable "environment" {
  type        = string
  description = "Environment (dev, staging, prod)"
  default     = "dev"
}

variable "tfstate_bucket" {
  type        = string
  description = "Bucket pour stocker les state terraform"
}
