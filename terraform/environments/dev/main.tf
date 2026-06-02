terraform {
  required_version = ">= 1.5"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
  # backend "gcs" {
  #   bucket = "cp-ecomm-demo-tfstate"
  #   prefix = "terraform/state"
  # }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# ─── GCP Infrastructure ──────────────────────────────────────────────────────

module "networking" {
  source     = "../../modules/networking"
  project_id = var.project_id
  region     = var.region
  env        = var.env
}

module "storage" {
  source     = "../../modules/storage"
  project_id = var.project_id
  region     = var.region
  env        = var.env
}

module "iam" {
  source     = "../../modules/iam"
  project_id = var.project_id
  env        = var.env
}

module "pubsub" {
  source     = "../../modules/pubsub"
  project_id = var.project_id
  env        = var.env
}

module "cloudsql" {
  source      = "../../modules/cloudsql"
  project_id  = var.project_id
  region      = var.region
  env         = var.env
  network_id  = module.networking.vpc_id
  db_password = var.db_password

  depends_on = [module.networking]
}

# ─── Outputs ──────────────────────────────────────────────────────────────────

output "vpc_id" {
  value = module.networking.vpc_id
}

output "landing_bucket" {
  value = module.storage.landing_bucket_name
}

output "lakehouse_bucket" {
  value = module.storage.lakehouse_bucket_name
}

output "cloudsql_private_ip" {
  value = module.cloudsql.private_ip
}

output "clickstream_topic" {
  value = module.pubsub.clickstream_topic_id
}

output "transactions_topic" {
  value = module.pubsub.transactions_topic_id
}
