variable "project_id" { type = string }
variable "region" { type = string }
variable "env" { type = string }

# VPC
resource "google_compute_network" "lakehouse_vpc" {
  name                    = "lakehouse-vpc-${var.env}"
  project                 = var.project_id
  auto_create_subnetworks = false
}

# Databricks subnet (delegated for Databricks GKE nodes)
resource "google_compute_subnetwork" "databricks_subnet" {
  name          = "databricks-subnet-${var.env}"
  project       = var.project_id
  region        = var.region
  network       = google_compute_network.lakehouse_vpc.id
  ip_cidr_range = "10.0.0.0/20"

  secondary_ip_range {
    range_name    = "pods"
    ip_cidr_range = "10.1.0.0/16"
  }
  secondary_ip_range {
    range_name    = "services"
    ip_cidr_range = "10.2.0.0/20"
  }

  private_ip_google_access = true
}

# Private services subnet (Cloud SQL, etc.)
resource "google_compute_subnetwork" "private_services_subnet" {
  name          = "private-services-subnet-${var.env}"
  project       = var.project_id
  region        = var.region
  network       = google_compute_network.lakehouse_vpc.id
  ip_cidr_range = "10.3.0.0/24"

  private_ip_google_access = true
}

# Cloud NAT for outbound internet (Databricks nodes need PyPI, etc.)
resource "google_compute_router" "nat_router" {
  name    = "nat-router-${var.env}"
  project = var.project_id
  region  = var.region
  network = google_compute_network.lakehouse_vpc.id
}

resource "google_compute_router_nat" "nat" {
  name                               = "lakehouse-nat-${var.env}"
  project                            = var.project_id
  router                             = google_compute_router.nat_router.name
  region                             = var.region
  nat_ip_allocate_option             = "AUTO_ONLY"
  source_subnetwork_ip_ranges_to_nat = "ALL_SUBNETWORKS_ALL_IP_RANGES"
}

# Firewall: allow internal communication
resource "google_compute_firewall" "allow_internal" {
  name    = "allow-internal-${var.env}"
  project = var.project_id
  network = google_compute_network.lakehouse_vpc.id

  allow {
    protocol = "tcp"
    ports    = ["0-65535"]
  }
  allow {
    protocol = "udp"
    ports    = ["0-65535"]
  }
  allow {
    protocol = "icmp"
  }

  source_ranges = ["10.0.0.0/8"]
}

# Private Service Connect for Google APIs (GCS, Pub/Sub)
resource "google_compute_global_address" "psc_address" {
  name         = "psc-google-apis-${var.env}"
  project      = var.project_id
  purpose      = "VPC_PEERING"
  address_type = "INTERNAL"
  prefix_length = 16
  network      = google_compute_network.lakehouse_vpc.id
}

resource "google_service_networking_connection" "private_service_connection" {
  network                 = google_compute_network.lakehouse_vpc.id
  service                 = "servicenetworking.googleapis.com"
  reserved_peering_ranges = [google_compute_global_address.psc_address.name]
}

output "vpc_id" {
  value = google_compute_network.lakehouse_vpc.id
}

output "databricks_subnet_id" {
  value = google_compute_subnetwork.databricks_subnet.id
}

output "private_services_subnet_id" {
  value = google_compute_subnetwork.private_services_subnet.id
}
