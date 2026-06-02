terraform {
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.40"
    }
  }
}

variable "project_id" { type = string }
variable "region" { type = string }
variable "env" { type = string }
variable "vpc_id" { type = string }
variable "subnet_id" { type = string }
variable "storage_bucket" { type = string }
variable "service_account_email" { type = string }

# NOTE: Workspace is created via Databricks console (trial account).
# This module configures Unity Catalog and schemas within the existing workspace.
# Set DATABRICKS_HOST and DATABRICKS_TOKEN env vars before running.

# External location — gives Databricks access to your GCS lakehouse bucket
resource "databricks_storage_credential" "gcs_credential" {
  name = "lakehouse-gcs-${var.env}"
  databricks_gcp_service_account {}
}

resource "databricks_external_location" "lakehouse" {
  name            = "lakehouse-${var.env}"
  url             = "gs://${var.storage_bucket}"
  credential_name = databricks_storage_credential.gcs_credential.name
  comment         = "Lakehouse Delta tables on GCS"
}

# Catalog structure
resource "databricks_catalog" "ecommerce" {
  name    = "ecommerce_${var.env}"
  comment = "E-commerce lakehouse catalog"
}

resource "databricks_schema" "bronze" {
  catalog_name = databricks_catalog.ecommerce.name
  name         = "bronze"
  comment      = "Raw ingested data"
}

resource "databricks_schema" "silver" {
  catalog_name = databricks_catalog.ecommerce.name
  name         = "silver"
  comment      = "Cleaned and deduplicated data"
}

resource "databricks_schema" "gold" {
  catalog_name = databricks_catalog.ecommerce.name
  name         = "gold"
  comment      = "Business-ready aggregations"
}

resource "databricks_schema" "governance" {
  catalog_name = databricks_catalog.ecommerce.name
  name         = "governance"
  comment      = "DQ results, freshness metrics, observability"
}

# Groups for access control demo
resource "databricks_group" "analysts" {
  display_name = "analysts"
}

resource "databricks_group" "pii_readers" {
  display_name = "pii_readers"
}

# Grant analysts read on gold only
resource "databricks_grants" "gold_grants" {
  schema = databricks_schema.gold.id
  grant {
    principal  = databricks_group.analysts.display_name
    privileges = ["USE_SCHEMA", "SELECT"]
  }
}

output "workspace_url" {
  value = "https://${var.region}.gcp.databricks.com"  # Will be overridden by actual URL
}
