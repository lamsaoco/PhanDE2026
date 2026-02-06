terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project
  region  = var.region
  credentials = var.credentials
}

resource "google_storage_bucket" "free_tier_bucket" {
  name          = var.gcs_bucket_name
  location      = var.location
  storage_class = "STANDARD"
  force_destroy = true
  public_access_prevention = "enforced"

  versioning {
    enabled = false
  }

  labels = {
    tier       = "free"
    managed_by = "terraform"
  }
}

resource "google_bigquery_dataset" "demo_dataset" {
  dataset_id = "trips_data_all"
  location   = var.location
}

resource "google_bigquery_table" "external_table" {
  dataset_id = google_bigquery_dataset.demo_dataset.dataset_id
  table_id   = "yellow_tripdata_external"

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://de-zoomcamp-hw3-phan/yellow_tripdata_2024-*.parquet"]
  }
}