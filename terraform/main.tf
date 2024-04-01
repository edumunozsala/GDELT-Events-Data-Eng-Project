terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "4.51.0"
    }
  }
}

provider "google" {
# Credentials only needs to be set if you do not have the GOOGLE_APPLICATION_CREDENTIALS
  credentials = file(var.credentials)
  project = var.project
  region  = var.region
}

# Enable BigQuery API
#resource "google_project_service" "bigquery" {
#  service            = "bigquery.googleapis.com"
#  disable_on_destroy = true
#}

# Create the bucket to store the data lake: bronze, silver and gold stages
resource "google_storage_bucket" "data-lake-bucket" {
  name          = var.gdelt-project-data-bucket
  location      = var.location

  # Optional, but recommended settings:
  storage_class = "STANDARD"
  uniform_bucket_level_access = true
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
        }
  }
}

# Create the bucket to store the data lake: bronze, silver and gold stages
resource "google_storage_bucket" "temp-data-lake-bucket" {
  name          = var.gdelt-project-temp-bucket
  location      = var.location

  # Optional, but recommended settings:
  storage_class = "STANDARD"
  uniform_bucket_level_access = true
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
        }
  }
}

resource "google_bigquery_dataset" "dataset" {
  dataset_id = var.bq_dataset_name
  project    = var.project
  location   = var.location
  delete_contents_on_destroy=true
}

