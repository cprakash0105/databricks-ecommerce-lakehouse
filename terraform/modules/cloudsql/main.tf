variable "project_id" { type = string }
variable "region" { type = string }
variable "env" { type = string }
variable "network_id" { type = string }

resource "google_sql_database_instance" "ecommerce_operational" {
  name             = "ecommerce-operational-${var.env}"
  project          = var.project_id
  region           = var.region
  database_version = "POSTGRES_15"

  settings {
    tier              = "db-f1-micro"  # Trial-friendly
    availability_type = "ZONAL"

    ip_configuration {
      ipv4_enabled    = false
      private_network = var.network_id
    }

    backup_configuration {
      enabled = true
      point_in_time_recovery_enabled = true
    }

    database_flags {
      name  = "log_statement"
      value = "all"
    }
  }

  deletion_protection = false  # Dev only
}

resource "google_sql_database" "ecommerce_db" {
  name     = "ecommerce"
  instance = google_sql_database_instance.ecommerce_operational.name
  project  = var.project_id
}

resource "google_sql_user" "app_user" {
  name     = "ecommerce_app"
  instance = google_sql_database_instance.ecommerce_operational.name
  project  = var.project_id
  password = var.db_password
}

variable "db_password" {
  type      = string
  sensitive = true
}

output "instance_connection_name" {
  value = google_sql_database_instance.ecommerce_operational.connection_name
}

output "private_ip" {
  value = google_sql_database_instance.ecommerce_operational.private_ip_address
}
