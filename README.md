# E-Commerce Lakehouse Platform

Enterprise data platform on GCP + Databricks showcasing:
- **Batch + Streaming ingestion** from multiple sources
- **Medallion architecture** (Bronze → Silver → Gold) with Delta Lake
- **Governance** — lineage, PII masking, DQ, active metadata, data locality
- **Data Virtualization** — Lakehouse Federation to Cloud SQL
- **Observability** — pipeline health, freshness SLAs, cost tracking
- **RAG** — Vector Search on Gold layer for conversational analytics
- **MCP Server** — tools exposing lakehouse operations to AI assistants

## Architecture

```
DATA SOURCES                         LAKEHOUSE (Databricks)
┌──────────────┐                    ┌─────────────────────────────────┐
│ GCS (CSV/    │──── Auto Loader ──▶│ Bronze (raw)                    │
│ Parquet)     │                    │   │                             │
├──────────────┤                    │   ▼ DLT + DQ Expectations       │
│ Cloud SQL    │──── JDBC ─────────▶│ Silver (cleaned, deduplicated)  │
│ (PostgreSQL) │                    │   │                             │
├──────────────┤                    │   ▼ Aggregations                │
│ Pub/Sub      │── Structured ─────▶│ Gold (business-ready)           │
│ (clickstream)│   Streaming        │                                 │
├──────────────┤                    ├─────────────────────────────────┤
│ Pub/Sub      │── Structured ─────▶│ GOVERNANCE (Unity Catalog)      │
│ (transactions)   Streaming        │ Tags│Lineage│PII│DQ│Audit      │
└──────────────┘                    ├─────────────────────────────────┤
                                    │ VIRTUALIZATION (Federation)      │
                                    │ Foreign Catalog → Cloud SQL     │
                                    ├─────────────────────────────────┤
                                    │ AI LAYER                        │
                                    │ RAG (Vector Search) + MCP Server│
                                    └─────────────────────────────────┘
```

## Project Structure

```
├── terraform/              # GCP infrastructure as code
│   ├── modules/
│   │   ├── networking/     # VPC, subnets, firewall, NAT, PSC
│   │   ├── storage/        # GCS buckets (landing, lakehouse)
│   │   ├── iam/            # Service accounts, roles
│   │   ├── databricks/     # Workspace provisioning
│   │   ├── pubsub/         # Topics + subscriptions
│   │   └── cloudsql/       # PostgreSQL instance
│   └── environments/dev/
├── databricks/
│   ├── notebooks/
│   │   ├── ingestion/      # Auto Loader, JDBC, Streaming
│   │   ├── dlt/            # Delta Live Tables pipelines
│   │   ├── governance/     # Tagging, masking, lineage queries
│   │   └── dq/            # Business + technical DQ rules
│   └── workflows/          # Job definitions
├── data-generator/         # Synthetic e-commerce data producer
├── mcp-server/             # MCP tools for lakehouse access
├── rag/                    # RAG pipeline setup
└── docs/                   # Architecture diagrams, decisions
```

## Versions

- **V1 (current):** Delta Lake, fully Databricks-native
- **V2 (planned):** UniForm (Delta + Iceberg), Trino, BigQuery interop

## Prerequisites

- GCP project with billing enabled (trial account works)
- Databricks workspace on GCP (trial account works)
- Terraform >= 1.5
- Python >= 3.10
- gcloud CLI configured
