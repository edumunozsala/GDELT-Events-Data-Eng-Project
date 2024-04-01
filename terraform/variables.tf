variable "credentials" {
  description = "GCP Credentials"
  default     = "gdelt-project-credentials.json"
  #ex: if you have a directory where this file is called keys with your service account json file
  #saved there as my-creds.json you could use default = "./keys/my-creds.json"
}


variable "project" {
  description = "Project"
  default     = "gdelt-project-data"
}

variable "region" {
  description = "Region"
  #Update the below to your desired region
  default     = "us-east1"
}

variable "location" {
  description = "Project Location"
  #Update the below to your desired location
  default     = "US"
}

variable "bq_dataset_name" {
  description = "GDELT Events BigQuery Dataset"
  #Update the below to what you want your dataset to be called
  default     = "gdelt_events"
}

variable "gdelt-project-data-bucket"{
  description = "GDELT Project Bucket"
  #Update the below to a unique bucket name
  default     = "gdelt-project-events-datalake"
}

variable "gdelt-project-temp-bucket"{
  description = "GDELT Project Bucket"
  #Update the below to a unique bucket name
  default     = "gdelt-project-temp-datalake"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
} 