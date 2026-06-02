variable "project_id" { type = string }
variable "env" { type = string }

# Clickstream events topic
resource "google_pubsub_topic" "clickstream" {
  name    = "ecommerce-clickstream-${var.env}"
  project = var.project_id

  labels = {
    env    = var.env
    domain = "ecommerce"
    type   = "streaming"
  }
}

resource "google_pubsub_subscription" "clickstream_databricks" {
  name    = "clickstream-databricks-${var.env}"
  project = var.project_id
  topic   = google_pubsub_topic.clickstream.id

  ack_deadline_seconds = 60
  retain_acked_messages = false
  message_retention_duration = "86400s"  # 24h

  expiration_policy {
    ttl = ""  # Never expires
  }
}

# Transaction events topic
resource "google_pubsub_topic" "transactions" {
  name    = "ecommerce-transactions-${var.env}"
  project = var.project_id

  labels = {
    env    = var.env
    domain = "ecommerce"
    type   = "streaming"
  }
}

resource "google_pubsub_subscription" "transactions_databricks" {
  name    = "transactions-databricks-${var.env}"
  project = var.project_id
  topic   = google_pubsub_topic.transactions.id

  ack_deadline_seconds = 60
  retain_acked_messages = false
  message_retention_duration = "86400s"

  expiration_policy {
    ttl = ""
  }
}

output "clickstream_topic_id" {
  value = google_pubsub_topic.clickstream.id
}

output "transactions_topic_id" {
  value = google_pubsub_topic.transactions.id
}
