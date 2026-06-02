terraform {
  required_version = ">= 1.5"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.40"
    }
  }
  # backend "gcs" {
  #   bucket = "YOUR_PROJECT_ID-tfstate"
  #   prefix = "terraform/state"
  # }
  # Uncomment above after creating the bucket. Use local state initially.
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# Databricks provider — only needed when deploy_databricks = true
# Set DATABRICKS_HOST and DATABRICKS_TOKEN env vars
provider "databricks" {
  host = var.databricks_host
}

# ─── GCP Infrastructure (Phase A) ────────────────────────────────────────────

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
  source     = "../../modules/cloudsql"
  project_id = var.project_id
  region     = var.region
  env        = var.env
  network_id = module.networking.vpc_id
}

# ─── Databricks Catalog Setup (Phase B) ──────────────────────────────────────
# Set deploy_databricks = true after workspace is ready and env vars are set

module "databricks" {
  count               = var.deploy_databricks ? 1 : 0
  source              = "../../modules/databricks"
  project_id          = var.project_id
  region              = var.region
  env                 = var.env
  vpc_id              = module.networking.vpc_id
  subnet_id           = module.networking.databricks_subnet_id
  storage_bucket      = module.storage.lakehouse_bucket_name
  service_account_email = module.iam.databricks_sa_email
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
