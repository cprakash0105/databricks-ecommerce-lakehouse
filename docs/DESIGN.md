# E-Commerce Lakehouse Platform — Design Document

## 1. Overview

Enterprise data platform on **GCP + Databricks** demonstrating end-to-end lakehouse architecture with governance, data virtualization, observability, and AI capabilities.

**Cloud:** GCP (project: `cp-ecomm-demo`)  
**Compute:** Databricks on GCP (serverless)  
**Region:** europe-west2 (GCP infra), us-east1 (Databricks workspace)  
**Table Format:** Delta Lake (V1), UniForm/Iceberg planned (V2)

---

## 2. Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                                      │
├─────────────────┬─────────────────┬─────────────────┬───────────────────┤
│ GCS (Batch)     │ Cloud SQL       │ Pub/Sub         │ Pub/Sub           │
│ JSON files:     │ PostgreSQL 15:  │ Clickstream     │ Transactions      │
│ - customers/    │ - customers     │ events          │ events            │
│ - orders/       │ - inventory     │                 │                   │
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
│  ├── Cloud NAT (outbound internet for compute nodes)                    │
│  ├── Private Service Connect (GCS, Pub/Sub via private network)         │
│  └── Firewall: allow-internal (10.0.0.0/8)                             │
│                                                                          │
│  GCS Buckets (all europe-west2, uniform access):                        │
│  ├── cp-ecomm-demo-landing-dev (raw files, 30d → NEARLINE lifecycle)   │
│  ├── cp-ecomm-demo-lakehouse-dev (Delta tables)                        │
│  └── cp-ecomm-demo-checkpoints-dev (streaming state)                   │
│                                                                          │
│  IAM (least-privilege):                                                  │
│  ├── databricks-lakehouse-dev (storage.objectAdmin, pubsub.subscriber) │
│  └── data-generator-dev (storage.objectCreator, pubsub.publisher)      │
│                                                                          │
│  Cloud SQL: ecommerce-operational-dev                                    │
│  ├── Private IP: 10.238.0.3                                            │
│  ├── PostgreSQL 15, db-f1-micro, ZONAL                                 │
│  └── Database: ecommerce (user: ecommerce_app)                         │
└─────────────────────────────────┬───────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    DATABRICKS LAKEHOUSE                                   │
│                                                                          │
│  Workspace: 8259562581476498.8.gcp.databricks.com                       │
│  Compute: Serverless (DLT + SQL Warehouse)                              │
│                                                                          │
│  ┌────────────────────────────────────────────────────────────────┐     │
│  │ UNITY CATALOG                                                   │     │
│  │ Catalog: ecommerce_dev                                          │     │
│  │ ├── bronze (raw ingested data)                                  │     │
│  │ ├── silver (cleaned, deduplicated)                              │     │
│  │ ├── gold (business aggregations)                                │     │
│  │ ├── governance (DQ results, metrics, observability)             │     │
│  │ └── default (DLT pipeline target)                               │     │
│  └────────────────────────────────────────────────────────────────┘     │
│                                                                          │
│  ┌────────────────────────────────────────────────────────────────┐     │
│  │ DLT PIPELINE: ecommerce-lakehouse                               │     │
│  │                                                                  │     │
│  │ Bronze (Auto Loader from GCS)                                   │     │
│  │ ├── bronze_customers (1,000 records)                            │     │
│  │ └── bronze_orders (5,000 records)                               │     │
│  │                                                                  │     │
│  │ Silver (DQ expectations, dedup, type casting)                   │     │
│  │ ├── silver_customers (expect_or_drop: valid_id, expect: email)  │     │
│  │ └── silver_orders (expect_or_drop: valid_id,                    │     │
│  │ │                   expect_or_fail: positive_amount)             │     │
│  │                                                                  │     │
│  │ Gold (Aggregations)                                             │     │
│  │ ├── gold_customer_360 (lifetime value, order count)             │     │
│  │ └── gold_daily_revenue (by country, segment)                    │     │
│  └────────────────────────────────────────────────────────────────┘     │
│                                                                          │
│  External Locations:                                                     │
│  ├── lakehouse-dev → gs://cp-ecomm-demo-lakehouse-dev                  │
│  ├── landing-dev → gs://cp-ecomm-demo-landing-dev                      │
│  └── checkpoints-dev → gs://cp-ecomm-demo-checkpoints-dev              │
│                                                                          │
│  Storage Credential:                                                     │
│  └── db-uc-credential-06f8gb5884-wa@uc-useast1.iam.gserviceaccount.com │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Implementation Steps

### Phase 1: GCP Infrastructure ✅ COMPLETE

| Step | Action | Outcome |
|------|--------|---------|
| 1.1 | Create GCP project `cp-ecomm-demo` | Project with billing enabled |
| 1.2 | Enable APIs (compute, storage, pubsub, sqladmin, servicenetworking, iam) | All APIs active |
| 1.3 | Write Terraform modules (networking, storage, iam, pubsub, cloudsql) | IaC in `terraform/modules/` |
| 1.4 | `terraform apply` from Cloud Shell | 24 resources created |
| 1.5 | Verify outputs (VPC, buckets, topics, Cloud SQL private IP) | All operational |

### Phase 2: Databricks Workspace ✅ COMPLETE

| Step | Action | Outcome |
|------|--------|---------|
| 2.1 | Create Databricks workspace via GCP Marketplace | Workspace live |
| 2.2 | Create storage credential (GCP SA) | `db-uc-credential-06f8gb5884-wa` |
| 2.3 | Grant SA `roles/storage.admin` on GCP project | GCS access verified |
| 2.4 | Create external locations (lakehouse, landing, checkpoints) | Read/Write/Delete all pass |
| 2.5 | Create Unity Catalog: `ecommerce_dev` with schemas | Catalog structure ready |

### Phase 3: Data Generation ✅ COMPLETE

| Step | Action | Outcome |
|------|--------|---------|
| 3.1 | Seed Cloud SQL operational tables | 3 tables (active_sessions, inventory, customers) |
| 3.2 | Run batch generator → GCS | 1,000 customers + 5,000 orders in landing bucket |
| 3.3 | Run streaming generator → Pub/Sub | ~600 events in clickstream + transactions topics |

### Phase 4: DLT Pipeline (Medallion) ✅ COMPLETE

| Step | Action | Outcome |
|------|--------|---------|
| 4.1 | Create DLT notebook with Bronze/Silver/Gold layers | `batch_pipeline_new` |
| 4.2 | Configure DLT pipeline (serverless, catalog: ecommerce_dev) | Pipeline `6ac4389a` |
| 4.3 | Run pipeline | All 6 tables created, data flowing Bronze→Silver→Gold |
| 4.4 | Verify Gold tables via SQL queries | `gold_customer_360`, `gold_daily_revenue` populated |

### Phase 5: Governance 🔲 NEXT

| Step | Action | Outcome |
|------|--------|---------|
| 5.1 | Apply active metadata tags (domain, owner, SLA, PII classification) | Tables discoverable and classified |
| 5.2 | Create dynamic PII masking views | Analysts see masked data, PII readers see raw |
| 5.3 | Verify automatic lineage in Unity Catalog | Table + column lineage visible |
| 5.4 | Deploy business DQ rules job | Results written to `governance.dq_results` |
| 5.5 | Data locality validation | Assert all data in correct region |
| 5.6 | Audit log queries | Track who accessed what |

### Phase 6: Data Virtualization 🔲 PLANNED

| Step | Action | Outcome |
|------|--------|---------|
| 6.1 | Create foreign connection to Cloud SQL | Lakehouse Federation configured |
| 6.2 | Create foreign catalog (`cloudsql_live`) | Live operational data queryable |
| 6.3 | Create virtual views (customer_realtime, product_engagement_live) | Unified access layer |
| 6.4 | Demo: join Gold lakehouse + live Cloud SQL in single query | Zero-copy virtualization |

### Phase 7: Observability 🔲 PLANNED

| Step | Action | Outcome |
|------|--------|---------|
| 7.1 | Deploy freshness monitoring job | SLA breach detection |
| 7.2 | DLT event log dashboards | Pipeline health visible |
| 7.3 | Cost tracking via system.billing.usage | DBU consumption tracked |
| 7.4 | Table growth metrics | Storage trends monitored |

### Phase 8: Streaming Ingestion 🔲 PLANNED

| Step | Action | Outcome |
|------|--------|---------|
| 8.1 | Add Pub/Sub connector to DLT pipeline | Real-time clickstream + transactions |
| 8.2 | Configure watermarking and dedup | Late-arriving data handled |
| 8.3 | Add streaming Gold tables (real-time engagement) | Sub-minute analytics |

### Phase 9: RAG Pipeline 🔲 PLANNED

| Step | Action | Outcome |
|------|--------|---------|
| 9.1 | Create Vector Search index on Gold tables | Embeddings on customer/product data |
| 9.2 | Configure Foundation Model endpoint | LLM for retrieval-augmented generation |
| 9.3 | Build retrieval chain | Question → search → context → answer |
| 9.4 | Demo: "Show me high-value customers at risk of churn" | Natural language analytics |

### Phase 10: MCP Server 🔲 PLANNED

| Step | Action | Outcome |
|------|--------|---------|
| 10.1 | Deploy MCP server with lakehouse tools | 6 tools exposed |
| 10.2 | Connect AI assistant (Claude) via MCP | Conversational lakehouse access |
| 10.3 | Demo: AI queries lineage, DQ, freshness via tools | Platform is AI-native |

### Phase 11: V2 — Apache Iceberg (UniForm) 🔲 FUTURE

| Step | Action | Outcome |
|------|--------|---------|
| 11.1 | Enable UniForm on Gold tables | Delta + Iceberg metadata generated |
| 11.2 | Deploy Trino (GCE or Cloud Run) | Federated query engine |
| 11.3 | Connect DBeaver → Trino → Iceberg tables | SQL client access without Databricks license |
| 11.4 | BigQuery reads Iceberg via BigLake | Three-engine lakehouse |

---

## 4. Data Flow

```
                    BATCH PATH
GCS (JSON files) ──── Auto Loader ────► Bronze ──► Silver ──► Gold
                                         (raw)    (clean)    (agg)

                    STREAMING PATH (planned)
Pub/Sub ──── Structured Streaming ────► Bronze ──► Silver ──► Gold
                                         (raw)    (dedup)    (real-time)

                    VIRTUALIZATION PATH (planned)
Cloud SQL ◄──── Lakehouse Federation ────► Virtual Views
(live ops)         (no data copy)          (join Gold + live)
```

---

## 5. Governance Model

| Capability | Implementation |
|---|---|
| **Active Metadata** | Unity Catalog tags on catalogs, schemas, tables, columns |
| **Lineage** | Auto-captured by Unity Catalog (table + column level) |
| **Technical DQ** | DLT expectations (expect_or_drop, expect_or_fail) |
| **Business DQ** | Scheduled job validating rules → governance.dq_results |
| **PII Management** | Column tags (classification=pii) + dynamic masking views |
| **Data Locality** | GCS buckets pinned to europe-west2 + runtime validation |
| **Access Control** | Groups (analysts, pii_readers) + column/table grants |
| **Audit** | system.access.audit logs + query history |
| **Observability** | Freshness SLAs, DLT event logs, billing/cost, table growth |

---

## 6. Security

| Layer | Control |
|---|---|
| Network | Private VPC, no public IPs (Cloud SQL), Private Service Connect |
| Identity | Least-privilege IAM SAs, Databricks storage credentials |
| Data | PII masking, column-level grants, encryption at rest (GCS default) |
| Secrets | Databricks secret scopes (Cloud SQL credentials) |
| Audit | Unity Catalog audit logs, GCP Cloud Audit Logs |

---

## 7. Cost Profile (Trial/Dev)

| Resource | Monthly Estimate |
|---|---|
| Cloud SQL (db-f1-micro) | ~$7 |
| GCS (< 1 GB) | ~$0.02 |
| Pub/Sub (low volume) | Free tier |
| VPC/NAT | ~$1 |
| Databricks (trial → serverless) | Free 14 days, then pay-per-query |
| **Total (post-trial)** | **~$10-15/month** |

---

## 8. Repository Structure

```
databricks-ecommerce-lakehouse/
├── terraform/                          # GCP Infrastructure as Code
│   ├── modules/
│   │   ├── networking/main.tf          # VPC, subnets, NAT, firewall, PSC
│   │   ├── storage/main.tf             # GCS buckets (region-pinned)
│   │   ├── iam/main.tf                 # Service accounts + roles
│   │   ├── pubsub/main.tf             # Topics + subscriptions
│   │   ├── cloudsql/main.tf           # PostgreSQL instance
│   │   └── databricks/main.tf         # Unity Catalog (Phase B)
│   └── environments/dev/
│       ├── main.tf                     # Module composition
│       ├── variables.tf                # Input variables
│       └── terraform.tfvars            # Environment values
├── databricks/
│   ├── notebooks/
│   │   ├── dlt/                        # DLT pipeline definitions
│   │   ├── dq/                         # Business DQ rules
│   │   └── governance/                 # Tags, masking, lineage, observability
│   └── workflows/                      # Job orchestration
├── data-generator/                     # Synthetic data (batch + streaming)
├── mcp-server/                         # MCP tools for AI assistants
├── rag/                                # Vector Search + RAG pipeline
└── docs/                               # Design, deployment, architecture
```

---

## 9. Key Decisions

| Decision | Rationale |
|---|---|
| Serverless compute | Trial-friendly, no cluster management, pay-per-use |
| Delta Lake (V1) | Native to Databricks, best DLT integration |
| Medallion architecture | Industry standard, clear data quality boundaries |
| Terraform for IaC | Reproducible, version-controlled infrastructure |
| Private networking | Enterprise security pattern, no public endpoints |
| Unity Catalog | Single governance layer across all assets |
| Schema: default for DLT | Avoids "cross-catalog" errors in serverless DLT |
| GCS in europe-west2 | Data locality compliance demonstration |
