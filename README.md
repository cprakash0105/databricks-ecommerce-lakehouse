# E-Commerce Lakehouse Platform

Enterprise data platform on **GCP + Databricks** demonstrating end-to-end lakehouse architecture with governance, data virtualization, streaming ingestion, and AI capabilities.

## What This Demonstrates

| Capability | Implementation |
|---|---|
| Multi-source ingestion (batch + streaming) | GCS Auto Loader + Confluent Kafka |
| Medallion architecture | Bronze → Silver → Gold via Delta Live Tables |
| Governance | Active metadata, lineage, PII masking, business DQ, freshness SLAs |
| Data Virtualization | Virtual views joining lakehouse Gold + operational data |
| Streaming | Real-time Kafka → DLT with watermarking, dedup, DQ expectations |
| GCP Infrastructure | Terraform IaC: VPC, GCS, Cloud SQL, Pub/Sub, IAM |
| Security | Private networking, least-privilege IAM, PII classification, role-based masking |

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                                      │
├─────────────────┬─────────────────┬─────────────────┬───────────────────┤
│ GCS (Batch)     │ Cloud SQL       │ Kafka           │ Kafka             │
│ JSON files:     │ PostgreSQL 15:  │ (Confluent)     │ (Confluent)       │
│ - customers/    │ - customers     │ ecommerce.      │ ecommerce.        │
│ - orders/       │ - inventory     │ clickstream     │ transactions      │
│                 │ - sessions      │                 │                   │
└────────┬────────┴────────┬────────┴────────┬────────┴────────┬──────────┘
         │                 │                 │                  │
         ▼                 ▼                 ▼                  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│               GCP INFRASTRUCTURE (Terraform)                             │
│                                                                          │
│  VPC: lakehouse-vpc-dev (10.0.0.0/20)                                   │
│  ├── databricks-subnet-dev (+ pod/service secondary ranges)             │
│  ├── private-services-subnet-dev (Cloud SQL)                            │
│  ├── Cloud NAT (outbound internet)                                      │
│  ├── Private Service Connect (GCS, Pub/Sub)                             │
│  └── Firewall: allow-internal (10.0.0.0/8)                             │
│                                                                          │
│  GCS Buckets (europe-west2, uniform access):                            │
│  ├── cp-ecomm-demo-landing-dev (raw files, lifecycle → NEARLINE)       │
│  ├── cp-ecomm-demo-lakehouse-dev (Delta tables)                        │
│  └── cp-ecomm-demo-checkpoints-dev (streaming state)                   │
│                                                                          │
│  Cloud SQL: ecommerce-operational-dev (PostgreSQL 15, private IP)       │
└─────────────────────────────────┬───────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    DATABRICKS LAKEHOUSE                                   │
│                                                                          │
│  Workspace: Serverless (DLT + SQL Warehouse)                            │
│                                                                          │
│  Unity Catalog: ecommerce_dev                                            │
│  ├── default (pipeline tables)                                          │
│  │   ├── bronze_customers, bronze_orders (batch)                        │
│  │   ├── bronze_clickstream, bronze_transactions (streaming)            │
│  │   ├── silver_customers, silver_orders (validated)                    │
│  │   ├── silver_clickstream, silver_transactions (parsed, deduped)      │
│  │   ├── gold_customer_360, gold_daily_revenue, gold_product_engagement │
│  │   ├── v_customer_360_masked, v_silver_customers_masked (PII views)   │
│  │   └── v_customer_realtime (virtual: gold + operational)              │
│  ├── operational (simulated live system)                                │
│  │   ├── active_sessions                                                │
│  │   └── inventory                                                      │
│  └── governance                                                         │
│      ├── dq_results (business rule outcomes)                            │
│      ├── freshness_metrics (SLA monitoring)                             │
│      └── table_growth_metrics                                           │
│                                                                          │
│  Pipelines:                                                              │
│  ├── ecommerce-lakehouse (batch: GCS → Bronze → Silver → Gold)         │
│  └── ecommerce-streaming (Kafka → Bronze → Silver → Gold)              │
└─────────────────────────────────────────────────────────────────────────┘
```

## Streaming Architecture

```
┌──────────────┐     ┌─────────────────────┐     ┌───────────────────────────┐
│ Kafka        │     │ Confluent Cloud      │     │ Databricks DLT            │
│ Producer     │────▶│ pkc-619z3.us-east1   │────▶│ (Serverless)              │
│ (Python)     │     │ .gcp.confluent.cloud │     │                           │
│              │     │                      │     │ Bronze: raw JSON + meta   │
│ 5 events/sec │     │ Topics:              │     │ Silver: parsed, validated │
│ 70% clicks   │     │ - ecommerce.         │     │ Gold: aggregated metrics  │
│ 30% txns     │     │   clickstream        │     │                           │
│              │     │ - ecommerce.         │     │ DQ: expect_or_drop,       │
│              │     │   transactions       │     │     expect_or_fail        │
└──────────────┘     └─────────────────────┘     └───────────────────────────┘
                     Auth: SASL_SSL/PLAIN          Auth: spark.kafka.api.*
```

## Governance

| Capability | Implementation |
|---|---|
| Active Metadata | Tags on catalogs, schemas, tables, columns (domain, owner, SLA, PII) |
| Lineage | Auto-captured by Unity Catalog (visual graph with pipeline hops) |
| Technical DQ | DLT expectations: expect_or_drop, expect_or_fail |
| Business DQ | 5 rules → governance.dq_results (integrity, completeness, uniqueness, bounds) |
| PII Management | Column classification + dynamic masking views (role-based) |
| Data Locality | GCS buckets pinned to europe-west2 (Terraform enforced) |
| Freshness SLAs | Monitored: OK / WARNING / BREACHED → governance.freshness_metrics |

## Project Structure

```
├── terraform/                      # GCP Infrastructure as Code
│   ├── modules/
│   │   ├── networking/             # VPC, subnets, NAT, firewall, PSC
│   │   ├── storage/                # GCS buckets (region-pinned)
│   │   ├── iam/                    # Service accounts + roles
│   │   ├── pubsub/                 # Topics + subscriptions
│   │   ├── cloudsql/               # PostgreSQL instance
│   │   └── databricks/             # Unity Catalog setup
│   └── environments/dev/
├── databricks/
│   ├── notebooks/
│   │   ├── dlt/                    # DLT pipeline definitions
│   │   ├── dq/                     # Business DQ rules
│   │   └── governance/             # Tags, masking, lineage, observability
│   └── workflows/                  # Job orchestration
├── data-generator/
│   ├── generator.py                # Batch: GCS files
│   ├── kafka_producer.py           # Streaming: Confluent Kafka
│   └── seed_cloudsql.sql           # Cloud SQL operational tables
├── mcp-server/                     # MCP tools for AI assistants
├── rag/                            # Vector Search + RAG pipeline
└── docs/
    ├── DESIGN.md                   # Full design document
    └── DEPLOYMENT.md               # Step-by-step deployment guide
```

## Versions

- **V1 (current):** Delta Lake, Databricks-native, GCS + Kafka
- **V2 (planned):** UniForm (Delta + Iceberg), Trino, BigQuery interop, DBeaver access

## Prerequisites

- GCP project with billing ($300 free credit sufficient)
- Databricks workspace on GCP (trial)
- Confluent Cloud account (free tier)
- Terraform >= 1.5
- Python >= 3.10

## Quick Start

See [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) for full step-by-step guide.

```bash
# 1. Deploy GCP infra
cd terraform/environments/dev
terraform init && terraform apply -var="db_password=YourPassword"

# 2. Generate batch data
cd data-generator
python generator.py --mode batch

# 3. Stream to Kafka
export KAFKA_API_KEY="..." KAFKA_API_SECRET="..."
python kafka_producer.py --duration 120 --rate 5

# 4. Deploy DLT pipelines in Databricks UI
# 5. Run governance script in SQL Editor
```
