variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "europe-west2"
}

variable "env" {
  description = "Environment name"
  type        = string
  default     = "dev"
}

variable "db_password" {
  description = "Cloud SQL database password"
  type        = string
  sensitive   = true
}
