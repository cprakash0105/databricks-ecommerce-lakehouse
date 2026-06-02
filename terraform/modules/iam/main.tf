variable "project_id" { type = string }
variable "env" { type = string }

# Databricks service account — used by workspace clusters
resource "google_service_account" "databricks_sa" {
  account_id   = "databricks-lakehouse-${var.env}"
  display_name = "Databricks Lakehouse SA (${var.env})"
  project      = var.project_id
}

# Storage access for Databricks
resource "google_project_iam_member" "databricks_storage" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.databricks_sa.email}"
}

# Pub/Sub subscriber for streaming ingestion
resource "google_project_iam_member" "databricks_pubsub" {
  project = var.project_id
  role    = "roles/pubsub.subscriber"
  member  = "serviceAccount:${google_service_account.databricks_sa.email}"
}

# Data generator service account — writes to landing zone + Pub/Sub
resource "google_service_account" "data_generator_sa" {
  account_id   = "data-generator-${var.env}"
  display_name = "Data Generator SA (${var.env})"
  project      = var.project_id
}

resource "google_storage_bucket_iam_member" "generator_landing_write" {
  bucket = "${var.project_id}-landing-${var.env}"
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.data_generator_sa.email}"
}

resource "google_project_iam_member" "generator_pubsub_publish" {
  project = var.project_id
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${google_service_account.data_generator_sa.email}"
}

output "databricks_sa_email" {
  value = google_service_account.databricks_sa.email
}

output "data_generator_sa_email" {
  value = google_service_account.data_generator_sa.email
}
