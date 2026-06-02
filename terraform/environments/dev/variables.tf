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

variable "deploy_databricks" {
  description = "Set to true after Databricks workspace is ready (Phase B)"
  type        = bool
  default     = false
}

variable "databricks_host" {
  description = "Databricks workspace URL (set after workspace creation)"
  type        = string
  default     = "https://placeholder.cloud.databricks.com"
}

variable "db_password" {
  description = "Cloud SQL database password"
  type        = string
  sensitive   = true
}
