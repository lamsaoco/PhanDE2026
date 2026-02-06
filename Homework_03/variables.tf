variable "credentials" {
  description = "My Credentials"
  default     = "./my-gcp-creds.json"
}

variable "project" {
  description = "Project"
  default     = "de-phannh"
}

variable "region" {
  description = "Region"
  default     = "us-central1"
}

variable "zone" {
  description = "Zone"
  default     = "us-central1-a"
}

variable "location" {
  description = "Project Location"
  default     = "US"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  default     = "demo_dataset"
}

variable "gcs_bucket_name" {
  description = "DE zoomcampt homework 03 bucket"
  default     = "de-zoomcamp-hw3-phan"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}