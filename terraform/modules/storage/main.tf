variable "project_id" { type = string }
variable "region" { type = string }
variable "env" { type = string }

# Landing zone — raw files arrive here
resource "google_storage_bucket" "landing_zone" {
  name          = "${var.project_id}-landing-${var.env}"
  project       = var.project_id
  location      = var.region  # Data locality: region-pinned
  force_destroy = true
  storage_class = "STANDARD"

  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition { age = 30 }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  labels = {
    env     = var.env
    purpose = "landing-zone"
    domain  = "ecommerce"
  }
}

# Lakehouse bucket — Delta tables (Bronze/Silver/Gold)
resource "google_storage_bucket" "lakehouse" {
  name          = "${var.project_id}-lakehouse-${var.env}"
  project       = var.project_id
  location      = var.region  # Data locality: same region as compute
  force_destroy = true
  storage_class = "STANDARD"

  uniform_bucket_level_access = true

  versioning {
    enabled = false  # Delta handles versioning via transaction log
  }

  labels = {
    env     = var.env
    purpose = "lakehouse"
    domain  = "ecommerce"
  }
}

# Checkpoints bucket — streaming checkpoints
resource "google_storage_bucket" "checkpoints" {
  name          = "${var.project_id}-checkpoints-${var.env}"
  project       = var.project_id
  location      = var.region
  force_destroy = true
  storage_class = "STANDARD"

  uniform_bucket_level_access = true

  labels = {
    env     = var.env
    purpose = "streaming-checkpoints"
  }
}

output "landing_bucket_name" {
  value = google_storage_bucket.landing_zone.name
}

output "lakehouse_bucket_name" {
  value = google_storage_bucket.lakehouse.name
}

output "checkpoints_bucket_name" {
  value = google_storage_bucket.checkpoints.name
}
