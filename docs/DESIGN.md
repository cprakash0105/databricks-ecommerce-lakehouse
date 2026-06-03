# E-Commerce Lakehouse Platform — Design Document

## 1. Overview

Enterprise data platform on **GCP + Databricks** demonstrating end-to-end lakehouse architecture with governance, data virtualization, observability, and AI capabilities.

**Cloud:** GCP (project: `cp-ecomm-demo`)  
**Compute:** Databricks on GCP (serverless)  
**Region:** europe-west2 (GCP infra), us-east1 (Databricks workspace)  
**Table Format:** Delta Lake (V1), UniForm/Iceberg planned (V2)  
**Streaming:** Confluent Cloud Kafka (us-east1)

---

## 2. Architecture

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
│  ├── Private IP: 10.238.0.3 | Public IP: 35.246.67.159                 │
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
│  │ ├── default (DLT pipeline tables)                               │     │
│  │ │   ├── bronze_customers (batch, Auto Loader)                   │     │
│  │ │   ├── bronze_orders (batch, Auto Loader)                      │     │
│  │ │   ├── bronze_clickstream (streaming, Kafka)                   │     │
│  │ │   ├── bronze_transactions (streaming, Kafka)                  │     │
│  │ │   ├── silver_customers (validated, PII tagged)                │     │
│  │ │   ├── silver_orders (validated, deduped)                      │     │
│  │ │   ├── silver_clickstream (parsed, deduped, watermarked)       │     │
│  │ │   ├── silver_transactions (validated, deduped)                │     │
│  │ │   ├── gold_customer_360 (lifetime metrics)                    │     │
│  │ │   ├── gold_daily_revenue (by country/segment)                 │     │
│  │ │   ├── gold_product_engagement (real-time from clickstream)    │     │
│  │ │   ├── v_customer_360_masked (PII masked view)                 │     │
│  │ │   ├── v_silver_customers_masked (PII masked view)             │     │
│  │ │   └── v_customer_realtime (virtual: gold + operational)       │     │
│  │ ├── operational (simulated live Cloud SQL)                      │     │
│  │ │   ├── active_sessions                                         │     │
│  │ │   └── inventory                                               │     │
│  │ └── governance (observability)                                  │     │
│  │     ├── dq_results                                              │     │
│  │     ├── freshness_metrics                                       │     │
│  │     └── table_growth_metrics                                    │     │
│  └────────────────────────────────────────────────────────────────┘     │
│                                                                          │
│  ┌────────────────────────────────────────────────────────────────┐     │
│  │ PIPELINES                                                       │     │
│  │ ├── ecommerce-lakehouse (batch: GCS → Bronze → Silver → Gold)  │     │
│  │ └── ecommerce-streaming (Kafka → Bronze → Silver → Gold)       │     │
│  └────────────────────────────────────────────────────────────────┘     │
│                                                                          │
│  ┌────────────────────────────────────────────────────────────────┐     │
│  │ GOVERNANCE                                                      │     │
│  │ ├── Active metadata tags (domain, owner, SLA, PII, locality)   │     │
│  │ ├── Column-level PII classification                             │     │
│  │ ├── Dynamic masking views                                       │     │
│  │ ├── Auto-captured lineage (visual graph with pipeline hops)    │     │
│  │ ├── Technical DQ (DLT expectations)                             │     │
│  │ ├── Business DQ (5 rules → governance.dq_results)              │     │
│  │ └── Freshness SLA monitoring                                    │     │
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

## 3. Data Flow

```
                    BATCH PATH (working)
GCS (JSON files) ──── Auto Loader ────► Bronze ──► Silver ──► Gold
 customers/                            (raw)     (clean)    (customer_360,
 orders/                                                     daily_revenue)

                    STREAMING PATH (working)
Confluent Kafka ──── Structured ──────► Bronze ──► Silver ──► Gold
 ecommerce.          Streaming          (raw     (parsed,   (product_
 clickstream                             JSON)    deduped)   engagement)
 ecommerce.
 transactions

                    VIRTUALIZATION PATH (working)
ecommerce_dev.operational ──────────► Virtual Views
(simulating Cloud SQL)                 (v_customer_realtime: gold + live)
```

---

## 4. Implementation Steps

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
| 3.3 | Run Kafka producer → Confluent Cloud | 422 clickstream + 178 transaction events |

### Phase 4: DLT Batch Pipeline (Medallion) ✅ COMPLETE

| Step | Action | Outcome |
|------|--------|---------|
| 4.1 | Create DLT notebook with Bronze/Silver/Gold layers | `batch_pipeline_new` |
| 4.2 | Configure DLT pipeline (serverless, catalog: ecommerce_dev) | Pipeline `6ac4389a` |
| 4.3 | Run pipeline | 6 tables: bronze(2) + silver(2) + gold(2) |
| 4.4 | Verify Gold tables | `gold_customer_360` (1K), `gold_daily_revenue` populated |

### Phase 5: Governance ✅ COMPLETE

| Step | Action | Outcome |
|------|--------|---------|
| 5.1 | Apply active metadata tags (domain, owner, SLA, PII, locality) | Tags visible on all tables/columns in Catalog UI |
| 5.2 | Classify PII columns (email, phone, first_name, last_name) | Column tags with classification=pii |
| 5.3 | Create dynamic PII masking views | `v_customer_360_masked`, `v_silver_customers_masked` |
| 5.4 | Verify automatic lineage in Unity Catalog | Visual lineage graph with pipeline hops |
| 5.5 | Deploy business DQ rules (5 rules) | Results in `governance.dq_results` (all passing) |
| 5.6 | Freshness monitoring | Results in `governance.freshness_metrics` |
| 5.7 | Create governance tables (dq_results, freshness_metrics, table_growth) | Observability foundation |

### Phase 6: Data Virtualization ✅ COMPLETE

| Step | Action | Outcome |
|------|--------|---------|
| 6.1 | Create `operational` schema (simulating Cloud SQL) | Tables: active_sessions, inventory |
| 6.2 | Populate operational tables with realistic data | 3 sessions, 5 inventory items |
| 6.3 | Create virtual view `v_customer_realtime` | Joins Gold + operational sessions |
| 6.4 | Create virtual view `v_product_stock_status` | Joins revenue + inventory |
| 6.5 | Demo queries: active high-value customers, low-stock products | Cross-source analytics working |
| 6.6 | Note: Lakehouse Federation (foreign catalog) attempted but blocked by serverless networking | Simulated pattern instead |

### Phase 7: Observability 🔲 NEXT

| Step | Action | Outcome |
|------|--------|---------|
| 7.1 | Instrument data generator with OpenTelemetry | Traces + metrics on event production |
| 7.2 | Instrument MCP server with OpenTelemetry | Traces per tool call, latency metrics |
| 7.3 | Extract DLT pipeline metrics from event logs | Pipeline health metrics |
| 7.4 | Export to GCP Cloud Trace + Cloud Monitoring | Centralized observability |
| 7.5 | Create monitoring dashboard | All signals in one view |
| 7.6 | Set up alerts (freshness SLA breach, DQ failures) | Proactive monitoring |

### Phase 8: Streaming Ingestion (Kafka) ✅ COMPLETE

| Step | Action | Outcome |
|------|--------|---------|
| 8.1 | Create Confluent Cloud Kafka cluster (us-east1) | Cluster with API key auth |
| 8.2 | Create topics: `ecommerce.clickstream`, `ecommerce.transactions` | 2 topics, 1 partition each |
| 8.3 | Build Kafka producer (`kafka_producer.py`) | Sends events at configurable rate |
| 8.4 | Create DLT streaming pipeline reading from Kafka | `streaming_pipeline` notebook |
| 8.5 | Run pipeline with Kafka credentials in config | All tables populated |
| 8.6 | Verify end-to-end: Kafka → Bronze(422+178) → Silver(422+178) → Gold(156) | Streaming medallion working |

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

## 5. Streaming Architecture Detail

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
                     API Key + Secret              in pipeline configuration
```

---

## 6. Governance Model

| Capability | Implementation | Status |
|---|---|---|
| **Active Metadata** | Unity Catalog tags on catalogs, schemas, tables, columns | ✅ |
| **Lineage** | Auto-captured by Unity Catalog (visual graph with pipeline hops) | ✅ |
| **Technical DQ** | DLT expectations: expect_or_drop, expect_or_fail, expect | ✅ |
| **Business DQ** | 5 SQL rules → governance.dq_results (revenue bounds, integrity, completeness, uniqueness) | ✅ |
| **PII Management** | Column tags (classification=pii, pii_type) + dynamic masking views | ✅ |
| **Data Locality** | GCS buckets pinned to europe-west2 (Terraform enforced) | ✅ |
| **Access Control** | Groups (analysts, pii_readers) + masked views for role-based access | ✅ |
| **Freshness SLAs** | Monitored via governance.freshness_metrics (OK/WARNING/BREACHED) | ✅ |
| **Observability** | DQ results, freshness, table growth tables in governance schema | ✅ |
| **Audit** | Unity Catalog audit logs (system.access.audit) | Available |

---

## 7. Security

| Layer | Control |
|---|---|
| Network | Private VPC, Cloud SQL private IP, Private Service Connect, NAT |
| Identity | Least-privilege IAM SAs, Databricks storage credentials |
| Data | PII masking, column-level classification, role-based views |
| Secrets | Kafka credentials in DLT pipeline config (spark.kafka.*) |
| Streaming | SASL_SSL authentication to Confluent Cloud |
| Audit | Unity Catalog audit logs, GCP Cloud Audit Logs |

---

## 8. Cost Profile

### Actual Costs (May 4 - Jun 2, 2026): ₹196.29 (~$2.35 USD)

| Service | Cost (₹) | Notes |
|---|---|---|
| Cloud Run | 98.29 | Not from this project |
| Compute Engine | 66.08 | NAT gateway + VMs |
| Cloud SQL | 15.03 | PostgreSQL (paused when not in use) |
| Gemini API | 11.69 | Not from this project |
| Networking | 2.55 | NAT egress |
| VM Manager | 2.55 | Auto-enabled |
| Cloud Storage | 0.01 | All GCS buckets |
| **Total** | **196.29** | **Well within $300 free credit** |

Additional:
- Databricks: Free trial (14 days), then serverless pay-per-query
- Confluent Kafka: Free tier (sufficient for demo)

---

## 9. Repository Structure

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
│   │   │   ├── batch_pipeline.py       # GCS → Bronze → Silver → Gold
│   │   │   └── ecommerce_pipeline.py   # Full pipeline (batch + streaming)
│   │   ├── dq/                         # Business DQ rules
│   │   │   └── business_dq_rules.py
│   │   └── governance/                 # Tags, masking, lineage, observability
│   │       ├── governance_setup.sql    # Complete governance script
│   │       ├── setup_governance.py
│   │       ├── data_virtualization.py
│   │       └── observability.py
│   └── workflows/                      # Job orchestration
│       └── ecommerce_workflow.json
├── data-generator/                     # Synthetic data producers
│   ├── generator.py                    # Batch (GCS) + Pub/Sub streaming
│   ├── kafka_producer.py              # Kafka streaming producer
│   ├── seed_cloudsql.sql              # Cloud SQL operational tables
│   └── requirements.txt
├── mcp-server/                         # MCP tools for AI assistants
│   ├── server.py
│   └── requirements.txt
├── rag/                                # Vector Search + RAG pipeline
├── docs/
│   ├── DESIGN.md                       # This document
│   └── DEPLOYMENT.md                   # Step-by-step deployment guide
├── .gitignore
└── README.md
```

---

## 10. Key Decisions

| Decision | Rationale |
|---|---|
| Serverless compute | Trial-friendly, no cluster management, pay-per-use |
| Delta Lake (V1) | Native to Databricks, best DLT integration |
| Medallion architecture | Industry standard, clear data quality boundaries |
| Terraform for IaC | Reproducible, version-controlled infrastructure |
| Private networking | Enterprise security pattern, no public endpoints |
| Unity Catalog | Single governance layer across all assets |
| Confluent Kafka over Pub/Sub | Native Databricks support, no connector needed, industry standard |
| Schema: default for DLT | Avoids "cross-catalog" errors in serverless DLT |
| GCS in europe-west2 | Data locality compliance demonstration |
| Simulated virtualization | Serverless can't reach Cloud SQL; same pattern demonstrated in-platform |
| Separate batch + streaming pipelines | Isolation of concerns, independent scaling and monitoring |

---

## 11. Demo Narrative

> "This is an enterprise e-commerce data platform running on GCP and Databricks.
>
> **Ingestion:** Data arrives via two paths — batch files from GCS processed by Auto Loader, and real-time events from Confluent Kafka. Both flow through a medallion architecture using Delta Live Tables.
>
> **Governance:** Every table is tagged with active metadata — domain, owner, SLA, PII classification. The lineage graph shows data flowing from source through bronze, silver, gold with pipeline hops. PII columns are classified and dynamically masked based on user group membership. Five business DQ rules validate data integrity continuously.
>
> **Virtualization:** Virtual views join the Gold lakehouse with operational data, enabling queries like 'show me high-value customers currently browsing with items in cart' — without copying data between systems.
>
> **Streaming:** Real-time clickstream and transaction events from Kafka are processed with watermarking, deduplication, and DQ expectations, producing live product engagement metrics in the Gold layer.
>
> **What's next:** OpenTelemetry observability on all components, RAG for natural language analytics, and an MCP server making the entire platform conversationally queryable by AI assistants."
