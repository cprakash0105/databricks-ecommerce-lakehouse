# Enhancement Backlog — Enterprise Maturity

Tracking document for remaining enhancements to make the platform production-realistic.

---

## Priority Legend

- 🔴 **Do First** — highest demo/interview impact
- 🟡 **Do Next** — strong differentiators
- 🟢 **Nice to Have** — polish and completeness

---

## Backlog

| # | Enhancement | Priority | Effort | Status | Notes |
|---|---|---|---|---|---|
| 1 | **Dead Letter Queue + Data Contracts** | 🔴 | 30 min | 🔲 TODO | Quarantine table for malformed Kafka events. Schema validation before processing. Stream never crashes. |
| 2 | **Incremental Ingestion + MERGE INTO (SCD Type 1)** | 🔴 | 30 min | 🔲 TODO | Re-run generator with updated customers → Auto Loader picks up new files → MERGE INTO for upserts. Demonstrates CDC pattern. |
| 3 | **Delta Sharing (Data as a Product)** | 🟡 | 20 min | 🔲 TODO | Create a Share from `ecommerce_dev.default.gold_customer_360`. Add recipient. Shows peer-to-peer data mesh sharing without data duplication. |
| 4 | **Reconciliation (Source vs Gold)** | 🟡 | 15 min | 🔲 TODO | After each pipeline run, compare source count → bronze count → silver count → gold count. Write to `governance.reconciliation`. Proves no data loss. |
| 5 | **Hub-and-Spoke / Data Sovereignty Views** | 🟡 | 20 min | 🔲 TODO | Create `ecommerce_dev.global_analytics` schema with pre-aggregated views that strip all PII. Simulates multi-region compliance (GDPR). |
| 6 | **FinOps / Cost Chargeback by Domain** | 🟢 | 15 min | 🔲 TODO | Tag tables by domain, query `system.billing.usage` grouped by domain tags. Show which team's pipelines cost what. |
| 7 | **SQL Alerts on DQ/Freshness Breach** | 🟢 | 10 min | 🔲 TODO | Create Databricks SQL Alert: if `governance.freshness_metrics.freshness_status = 'BREACHED'` → trigger notification. |
| 8 | **Schema Evolution Handling** | 🟢 | 20 min | 🔲 TODO | Add new columns to generated data. Show Auto Loader's `cloudFiles.schemaEvolutionMode = "addNewColumns"` handling it gracefully. |
| 9 | **Multi-Environment Promotion Pattern** | 🟢 | 15 min | 🔲 TODO | Create `ecommerce_staging` catalog. Document promotion flow: dev → staging → prod. Same Terraform, different tfvars. |
| 10 | **Observability (OpenTelemetry)** | 🟢 | 45 min | 🔲 TODO | Instrument Kafka producer + MCP server with OTel SDK. Export traces to GCP Cloud Trace, metrics to Cloud Monitoring. |

---

## Implementation Details

### 1. Dead Letter Queue + Data Contracts

**What:** Add try/catch parsing in the streaming DLT pipeline. Malformed events route to `ecommerce_dev.default.quarantine` with error reason.

**Demo narrative:** *"Malformed events don't crash the pipeline — they route to a quarantine table with the error reason and raw payload. The stream never stops. Ops team investigates async."*

**Implementation:**
- Add `quarantine` table to streaming pipeline
- Parse JSON inside a try/catch equivalent (use `from_json` with `PERMISSIVE` mode + `_corrupt_record`)
- Route corrupt records to quarantine, valid records to silver
- Add DQ rule: "quarantine count should be < 1% of total"

---

### 2. Incremental Ingestion + MERGE INTO (SCD Type 1)

**What:** Run generator again with modified customer data (address/segment changes). Show Auto Loader picks up ONLY new files. Use MERGE INTO for upserts.

**Demo narrative:** *"Customer profiles change over time. We handle updates via MERGE INTO — SCD Type 1 upserts. Same pattern works for CDC from operational databases."*

**Implementation:**
- Modify `generator.py` to produce "update" files (same customer_id, new segment/city)
- Auto Loader picks up new file (already works)
- Add MERGE INTO logic in Silver layer: match on `customer_id`, update changed fields
- Gold layer automatically reflects latest state

---

### 3. Delta Sharing (Data as a Product)

**What:** Create a Share in Unity Catalog exposing Gold tables. Add a recipient (can be the same account for demo).

**Demo narrative:** *"Marketing consumes Billing's data product via Delta Sharing — zero data duplication, full audit trail, instantly revocable."*

**Implementation:**
```sql
CREATE SHARE ecommerce_gold_share;
ALTER SHARE ecommerce_gold_share ADD TABLE ecommerce_dev.default.gold_customer_360;
ALTER SHARE ecommerce_gold_share ADD TABLE ecommerce_dev.default.gold_daily_revenue;
CREATE RECIPIENT marketing_team;
GRANT SELECT ON SHARE ecommerce_gold_share TO RECIPIENT marketing_team;
```

---

### 4. Reconciliation

**What:** After pipeline run, compare counts at each layer. Write to `governance.reconciliation`.

**Demo narrative:** *"We can prove no data was lost or duplicated in transit. Source → Bronze → Silver → Gold counts reconcile within tolerance."*

**Implementation:**
```sql
CREATE TABLE ecommerce_dev.governance.reconciliation (
  run_timestamp TIMESTAMP,
  pipeline STRING,
  source_count LONG,
  bronze_count LONG,
  silver_count LONG,
  gold_count LONG,
  records_dropped LONG,
  drop_reason STRING,
  reconciliation_status STRING  -- PASS / FAIL / WARNING
);
```

---

### 5. Hub-and-Spoke / Data Sovereignty

**What:** Create `global_analytics` schema with views that aggregate and strip PII. Simulates what a "global HQ" analyst would see.

**Demo narrative:** *"EU customer PII never leaves the regional bucket. Global analysts see pre-aggregated, anonymized views — GDPR compliant by design."*

**Implementation:**
```sql
CREATE SCHEMA ecommerce_dev.global_analytics;

CREATE VIEW ecommerce_dev.global_analytics.revenue_by_region AS
SELECT country AS region, segment, SUM(revenue) AS total_revenue, 
       SUM(order_count) AS total_orders
FROM ecommerce_dev.default.gold_daily_revenue
GROUP BY country, segment;
-- No PII columns exposed. No row-level customer data.
```

---

### 6. FinOps / Cost Chargeback

**What:** Query system billing tables grouped by domain tags.

**Demo narrative:** *"Each domain's compute and storage costs are tagged and tracked. Finance sees exactly what each team spent."*

**Implementation:**
```sql
-- Requires system tables access
SELECT 
  usage_metadata.job_name,
  sku_name,
  SUM(usage_quantity) AS total_dbus,
  ROUND(SUM(usage_quantity) * 0.07, 2) AS estimated_cost_usd
FROM system.billing.usage
WHERE usage_date >= current_date() - INTERVAL 7 DAYS
GROUP BY 1, 2
ORDER BY total_dbus DESC;
```

---

### 7. SQL Alerts

**What:** Create alert that fires when freshness breaches SLA.

**Demo narrative:** *"The platform is proactive. When data staleness exceeds the SLA tag, alerts fire automatically."*

**Implementation:**
- In Databricks SQL → Alerts → Create Alert
- Query: `SELECT COUNT(*) FROM ecommerce_dev.governance.freshness_metrics WHERE freshness_status = 'BREACHED' AND check_timestamp > current_timestamp() - INTERVAL 1 HOUR`
- Trigger: when value > 0
- Destination: email or webhook

---

### 8. Schema Evolution

**What:** Add new columns to source data, show Auto Loader handles it without failure.

**Demo narrative:** *"When upstream adds a new field, Auto Loader's schema evolution absorbs it gracefully — no pipeline failures, no manual intervention."*

**Implementation:**
- Modify generator to add `loyalty_tier` column to customer JSON
- Set Auto Loader option: `cloudFiles.schemaEvolutionMode = "addNewColumns"`
- Re-run pipeline → new column appears automatically in Bronze

---

### 9. Multi-Environment Promotion

**What:** Document (or implement) dev → staging → prod catalog promotion pattern.

**Demo narrative:** *"Code and data promote through environments. Same Terraform modules, different variable files. Unity Catalog isolates dev from prod."*

**Implementation:**
```sql
CREATE CATALOG ecommerce_staging;
-- In production: same DLT notebook runs against staging catalog
-- Promotion = re-run pipeline pointing to next catalog
```

---

### 10. Observability (OpenTelemetry)

**What:** Instrument Kafka producer and MCP server with OTel. Export to GCP.

**Demo narrative:** *"Every component emits traces and metrics. I can trace a single event from Kafka through the entire medallion flow."*

**Implementation:**
- Add `opentelemetry-sdk`, `opentelemetry-exporter-gcp-trace` to producer
- Wrap `produce()` calls in spans
- Export to GCP Cloud Trace
- Create Cloud Monitoring dashboard

---

---

## Unhappy Path / Failure Scenarios

| # | Scenario | Priority | Effort | Status | Notes |
|---|---|---|---|---|---|
| 11 | **Kafka Broker Unavailable** | 🔴 | 20 min | 🔲 TODO | Simulate Kafka outage. Show pipeline retries with backoff, doesn't crash. Checkpoints preserve progress. On recovery, resumes from last committed offset. |
| 12 | **Late-Arriving Data** | 🔴 | 20 min | 🔲 TODO | Send events with timestamps 2 hours in the past. Show watermarking handles them correctly — either processed or dropped with audit trail. |
| 13 | **Duplicate Events (At-Least-Once Delivery)** | 🔴 | 15 min | 🔲 TODO | Send same event_id twice from producer. Show `dropDuplicates` in Silver catches it. Reconciliation proves exact count. |
| 14 | **Partial Batch Failure (Poison Pill)** | 🔴 | 25 min | 🔲 TODO | Mix valid + invalid JSON in same Kafka batch. Valid records proceed to Silver, invalid route to quarantine. Pipeline continues uninterrupted. |
| 15 | **Source Schema Breaking Change** | 🟡 | 20 min | 🔲 TODO | Upstream renames `total_amount` → `order_total`. Pipeline fails gracefully, logs schema mismatch, alerts. No silent data loss. |
| 16 | **DQ Expectation Failure (expect_or_fail)** | 🟡 | 15 min | 🔲 TODO | Inject orders with negative amounts. Show `expect_or_fail` halts that table's update, other tables continue. DQ alert fires. |
| 17 | **Storage Credential Expiry / Permission Revoked** | 🟡 | 15 min | 🔲 TODO | Temporarily revoke GCS access from Databricks SA. Show pipeline fails with clear error. Restore → pipeline auto-recovers on next run. |
| 18 | **Network Partition (GCS Unreachable)** | 🟡 | 15 min | 🔲 TODO | Simulate by pointing Auto Loader to non-existent bucket. Show error handling, retry logic, and graceful failure without corrupting existing tables. |
| 19 | **Concurrent Pipeline Conflict** | 🟢 | 20 min | 🔲 TODO | Run batch and streaming pipelines simultaneously writing to overlapping tables. Show Delta's ACID transactions prevent corruption. |
| 20 | **Checkpoint Corruption Recovery** | 🟢 | 20 min | 🔲 TODO | Delete/corrupt streaming checkpoint. Show how to recover: reset to earliest offset, reprocess with dedup ensuring no duplicates in Silver/Gold. |
| 21 | **Backpressure / Volume Spike** | 🟢 | 20 min | 🔲 TODO | Crank producer to 100+ events/sec. Show DLT handles backpressure via micro-batch sizing. No data loss, just increased latency. |
| 22 | **Reject Handling with Business Rules** | 🔴 | 25 min | 🔲 TODO | Orders from sanctioned countries, amounts exceeding fraud threshold, invalid product IDs → separate reject table with rejection reason code, timestamp, source record. Reprocessable. |

---

### 11. Kafka Broker Unavailable

**What:** Simulate Kafka connectivity loss. Pipeline retries gracefully, resumes on recovery.

**Demo narrative:** *"When Kafka goes down, the streaming pipeline doesn't crash — it retries with exponential backoff. When connectivity restores, it resumes from the exact last committed offset. Zero data loss."*

**Implementation:**
- Pause/revoke Kafka API key temporarily in Confluent console
- Pipeline logs connection errors but keeps retrying
- Re-enable key → pipeline resumes from checkpoint
- Show event log: retry attempts + successful recovery

---

### 12. Late-Arriving Data

**What:** Produce events with timestamps significantly in the past. Show watermark behavior.

**Demo narrative:** *"In real-time systems, events arrive late — network delays, mobile app buffering. Our watermark is set to 30 minutes for clickstream. Events older than that are dropped with an audit trail. Events within the window are processed normally."*

**Implementation:**
- Modify Kafka producer to send events with `timestamp = now() - 2 hours`
- Silver's `withWatermark("event_ts", "30 minutes")` drops them
- Add a `late_arrivals` audit table that captures dropped-due-to-watermark counts
- DQ rule: "late arrival rate should be < 5%"

---

### 13. Duplicate Events

**What:** Intentionally produce same `event_id` / `txn_id` multiple times. Show dedup works.

**Demo narrative:** *"Kafka guarantees at-least-once delivery. Our Silver layer uses `dropDuplicates` on business keys to ensure exactly-once semantics. Reconciliation proves bronze_count >= silver_count, and the difference equals detected duplicates."*

**Implementation:**
- Modify producer: send 10% of events twice with same `event_id`
- Silver's `dropDuplicates(["event_id"])` catches them
- Reconciliation: `bronze_count - silver_count = duplicate_count`
- Log duplicates detected per run in governance table

---

### 14. Partial Batch Failure (Poison Pill)

**What:** Mix valid JSON + malformed JSON + valid JSON in same topic. Only bad records quarantined.

**Demo narrative:** *"A single bad record doesn't take down the pipeline. We use PERMISSIVE parsing — valid records flow through, malformed records route to quarantine with the raw payload and parse error. The stream never stops."*

**Implementation:**
- Modify producer to inject ~5% malformed JSON (missing brackets, wrong types)
- In streaming DLT, use `from_json` with `columnNameOfCorruptRecord = "_corrupt_record"`
- Split: `_corrupt_record IS NULL` → Silver, otherwise → quarantine table
- Quarantine schema: `raw_payload, error_reason, kafka_timestamp, ingestion_ts`

---

### 15. Source Schema Breaking Change

**What:** Upstream renames a column. Pipeline detects mismatch and fails gracefully.

**Demo narrative:** *"When a producer makes a breaking schema change without updating the data contract, our pipeline fails fast with a clear error — not silently dropping data. The alert fires, and the team negotiates the contract change."*

**Implementation:**
- Produce events with `order_total` instead of `amount`
- Silver's `from_json` with strict schema returns NULL for missing `amount`
- `expect_or_fail("positive_amount", "amount > 0")` triggers failure
- DLT event log shows exactly which expectation failed and why
- Alert fires on pipeline failure

---

### 16. DQ Expectation Failure (expect_or_fail)

**What:** Inject data that violates a critical DQ rule. Show partial pipeline success.

**Demo narrative:** *"Critical DQ rules use expect_or_fail — if violated, that specific table's update halts. But other independent tables continue processing. You get targeted failure, not total pipeline collapse."*

**Implementation:**
- Produce transactions with `amount = -500`
- `expect_or_fail("positive_amount", "amount > 0")` halts `silver_transactions`
- `silver_clickstream` and downstream Gold tables continue processing
- Event log clearly shows which expectation failed

---

### 17. Storage Credential Expiry

**What:** Temporarily revoke GCS permissions. Show clear failure and recovery.

**Demo narrative:** *"If the storage credential expires or permissions are revoked, the pipeline fails with a clear 403 error — not silent data corruption. Once permissions are restored, the next pipeline trigger auto-recovers."*

**Implementation:**
- In GCP IAM, temporarily remove `storage.admin` from Databricks SA
- Pipeline run fails with permission denied
- Restore permission → next run succeeds
- Show: no partial writes, no corrupted tables (Delta's atomic commits)

---

### 18. Network Partition (GCS Unreachable)

**What:** Point Auto Loader to a non-existent path. Show graceful failure.

**Demo narrative:** *"Network issues don't corrupt the lakehouse. Delta's ACID transactions mean either a commit fully succeeds or it doesn't — no half-written data."*

**Implementation:**
- Temporarily change Auto Loader path to `gs://non-existent-bucket/`
- Pipeline fails cleanly with IOException
- Fix path → re-run → resumes from checkpoint
- Verify: no phantom records in Bronze

---

### 19. Concurrent Pipeline Conflict

**What:** Run batch + streaming pipelines targeting same tables simultaneously.

**Demo narrative:** *"Delta Lake's ACID transactions handle concurrent writes. Two pipelines can write to the same table without corruption — conflicts are resolved via optimistic concurrency control."*

**Implementation:**
- Trigger both `ecommerce-lakehouse` and `ecommerce-streaming` at the same time
- Both write to tables in `ecommerce_dev.default`
- Verify no data corruption, no lost records
- Check Delta history: `DESCRIBE HISTORY ecommerce_dev.default.gold_customer_360`

---

### 20. Checkpoint Corruption Recovery

**What:** Simulate lost checkpoint. Show recovery procedure.

**Demo narrative:** *"If a streaming checkpoint is corrupted, we reset to earliest offset and reprocess. Our dedup logic in Silver ensures no duplicates appear even after full reprocessing."*

**Implementation:**
- Delete checkpoint directory in GCS
- Restart pipeline with `startingOffsets = "earliest"`
- Bronze gets re-ingested (duplicates)
- Silver's `dropDuplicates` prevents downstream duplicates
- Reconciliation confirms Gold counts unchanged

---

### 21. Backpressure / Volume Spike

**What:** Spike producer rate from 5/sec to 100+/sec. Show DLT handles gracefully.

**Demo narrative:** *"During Black Friday-style spikes, the pipeline doesn't drop data — it increases micro-batch sizes and processes more per cycle. Latency grows slightly but no data loss."*

**Implementation:**
- Run: `python kafka_producer.py --duration 60 --rate 100`
- Pipeline processes larger batches
- Check DLT event logs: batch sizes increase, processing time increases
- Verify: all records eventually land in Gold

---

### 22. Reject Handling with Business Rules

**What:** Implement a business-rule reject table separate from DQ (DQ = technical, rejects = business logic).

**Demo narrative:** *"Technical DQ catches malformed data. Business rejects catch valid-but-unacceptable data — fraud-risk transactions, sanctioned countries, exceeded thresholds. Both are auditable and reprocessable."*

**Implementation:**
```sql
CREATE TABLE ecommerce_dev.governance.business_rejects (
  reject_timestamp TIMESTAMP,
  source_table STRING,
  record_key STRING,
  reject_reason_code STRING,
  reject_reason_desc STRING,
  raw_payload STRING,
  reprocessed BOOLEAN DEFAULT FALSE,
  reprocessed_at TIMESTAMP
);
```

Business rules:
- `amount > 50000` → reject (fraud threshold)
- `country IN ('XX','YY')` → reject (sanctioned)
- `product_id NOT IN inventory` → reject (invalid reference)
- `customer_id IS NULL AND amount > 1000` → reject (anonymous high-value)

Rejects are stored with full payload for manual review and reprocessing.

---

---

## Multi-Cloud Integration

| # | Scenario | Priority | Effort | Status | Notes |
|---|---|---|---|---|---|
| 23 | **Azure Blob as Data Source (M&A scenario)** | 🔴 | 20 min | 🔲 TODO | "Acquired company" runs on Azure. Ingest their product/inventory data from Azure Blob into the GCP lakehouse via Auto Loader + ABFS connector. |
| 24 | **AWS S3 as DR / Consumption Layer** | 🔴 | 30 min | 🔲 TODO | Replicate Gold tables to S3. AWS Athena queries Delta/Parquet files directly. Shows cross-cloud DR and vendor-agnostic consumption. |
| 25 | **Azure Event Hubs as Streaming Source** | 🟡 | 25 min | 🔲 TODO | Azure Event Hubs exposes Kafka-compatible API. Existing streaming pipeline reads from both Confluent Kafka AND Azure Event Hubs — same DLT code. |
| 26 | **Multi-Cloud Iceberg (UniForm + Athena + Synapse)** | 🟡 | 30 min | 🔲 TODO | Enable UniForm on Gold tables. AWS Athena reads via Iceberg. Azure Synapse reads via Iceberg. Three clouds, one table, zero duplication. Ties into V2 plan. |
| 27 | **Cross-Cloud Metadata Sync (Purview)** | 🟢 | 30 min | 🔲 TODO | Push Unity Catalog metadata to Microsoft Purview via REST API. Enterprise-wide single pane of glass across all clouds. |
| 28 | **Delta Sharing Cross-Cloud** | 🟡 | 15 min | 🔲 TODO | Share Gold tables to an AWS-based recipient via Delta Sharing. Recipient reads from S3 without Databricks. Shows data mesh across cloud boundaries. |

---

### 23. Azure Blob as Data Source (M&A Scenario)

**What:** Simulate acquired company data on Azure. Databricks Auto Loader ingests from Azure Blob alongside GCS.

**Demo narrative:** *"The company acquired a business running on Azure. We ingest their product catalog from Azure Blob into our GCP lakehouse — same medallion pipeline, same governance, no migration required."*

**Implementation:**
- Create Azure free account ($200 credit)
- Create Storage Account + container: `acquired-company-data`
- Upload product catalog JSON files
- Create Databricks external location pointing to `abfss://container@account.dfs.core.windows.net/`
- Add Azure source to batch DLT pipeline:
```python
@dlt.table(name="bronze_products_azure")
def bronze_products_azure():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("abfss://acquired-company-data@storageaccount.dfs.core.windows.net/products/")
        .withColumn("ingestion_ts", current_timestamp())
        .withColumn("source_cloud", lit("azure"))
    )
```

---

### 24. AWS S3 as DR / Consumption Layer

**What:** Replicate Gold tables to S3. Query from AWS Athena without Databricks.

**Demo narrative:** *"Gold tables are replicated to AWS S3 for disaster recovery and for teams that run on AWS. They query via Athena — no Databricks license needed. Same data, multiple clouds."*

**Implementation:**
- Create AWS free tier account
- Create S3 bucket: `ecommerce-lakehouse-dr`
- Use GCP Cloud Storage Transfer to replicate `cp-ecomm-demo-lakehouse-dev/gold/` → S3
- OR: Export Gold tables as Parquet via notebook → upload to S3
- Create AWS Glue Crawler → catalogs the Parquet/Delta files
- Query in Athena:
```sql
SELECT segment, COUNT(*), AVG(lifetime_value)
FROM ecommerce_gold.customer_360
GROUP BY segment;
```

---

### 25. Azure Event Hubs as Streaming Source

**What:** Azure Event Hubs provides Kafka-compatible API. Same streaming DLT pipeline reads from both sources.

**Demo narrative:** *"The acquired Azure-based system emits events to Event Hubs. Because Event Hubs exposes a Kafka API, our existing streaming pipeline ingests from it with zero code changes — just a different bootstrap server."*

**Implementation:**
- Create Event Hubs namespace (free tier: 1 TU)
- Create topic: `azure.orders`
- Connection string acts as Kafka bootstrap + SASL credentials
- Add to streaming DLT:
```python
azure_kafka_config = {
    "kafka.bootstrap.servers": "your-namespace.servicebus.windows.net:9093",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": "...EventHubs connection string...",
}

@dlt.table(name="bronze_orders_azure")
def bronze_orders_azure():
    return (
        spark.readStream.format("kafka")
        .options(**azure_kafka_config)
        .option("subscribe", "azure.orders")
        .option("startingOffsets", "earliest")
        .load()
    )
```

---

### 26. Multi-Cloud Iceberg (UniForm + Athena + Synapse)

**What:** Enable UniForm on Gold tables so they emit both Delta and Iceberg metadata. AWS Athena and Azure Synapse read natively.

**Demo narrative:** *"We write Delta natively in Databricks, but UniForm auto-generates Iceberg metadata. AWS reads via Athena Iceberg connector, Azure reads via Synapse Iceberg connector. One write, three clouds, zero duplication."*

**Implementation:**
```sql
ALTER TABLE ecommerce_dev.default.gold_customer_360
SET TBLPROPERTIES ('delta.universalFormat.enabledFormats' = 'iceberg');

ALTER TABLE ecommerce_dev.default.gold_daily_revenue
SET TBLPROPERTIES ('delta.universalFormat.enabledFormats' = 'iceberg');
```
Then:
- AWS Athena: register Iceberg table pointing to GCS (or replicated S3)
- Azure Synapse: OPENROWSET against Iceberg metadata
- Trino (V2): connects via Iceberg REST Catalog

---

### 27. Cross-Cloud Metadata Sync (Purview)

**What:** Sync Unity Catalog metadata to Microsoft Purview for enterprise-wide discovery.

**Demo narrative:** *"Unity Catalog governs the Databricks platform. Metadata is synced to Microsoft Purview so the enterprise has a single catalog across all data assets — GCP, Azure, AWS — regardless of where the data lives."*

**Implementation:**
- Use Databricks Unity Catalog REST API to export table/column metadata
- Transform to Purview-compatible format (Apache Atlas entities)
- Push via Purview REST API (`/api/atlas/v2/entity`)
- Script runs on schedule to keep catalogs in sync

---

### 28. Delta Sharing Cross-Cloud

**What:** Share Gold tables to a recipient on AWS. They consume without Databricks.

**Demo narrative:** *"Marketing team runs on AWS. They consume our Gold data product via Delta Sharing — open protocol, no Databricks required on their side. They use the delta-sharing Python library or Spark connector to read directly from our GCS storage via pre-signed URLs."*

**Implementation:**
```sql
CREATE SHARE cross_cloud_share;
ALTER SHARE cross_cloud_share ADD TABLE ecommerce_dev.default.gold_customer_360;
ALTER SHARE cross_cloud_share ADD TABLE ecommerce_dev.default.gold_daily_revenue;
CREATE RECIPIENT aws_marketing_team;
-- Share the activation link with the AWS-side consumer
```
AWS consumer:
```python
import delta_sharing
profile = "path/to/share_profile.json"
df = delta_sharing.load_as_pandas(f"{profile}#cross_cloud_share.default.gold_customer_360")
```

---

## Updated Architecture (Multi-Cloud)

```
┌──────────────┐  ┌──────────────┐  ┌──────────────────────────────────────┐
│   AZURE      │  │   GCP        │  │   AWS                                │
│              │  │ (PRIMARY)    │  │                                      │
│ Blob Storage │  │ GCS Buckets  │  │ S3 (DR/Consumption)                  │
│ (M&A data)   │  │ Cloud SQL    │  │ Athena (queries Gold)                │
│              │  │ Pub/Sub      │  │                                      │
│ Event Hubs   │  │ Kafka        │  │ Delta Sharing recipient              │
│ (streaming)  │  │ (Confluent)  │  │                                      │
│              │  │              │  │                                      │
│ Purview      │  │ DATABRICKS   │  │                                      │
│ (metadata    │  │ (lakehouse)  │  │                                      │
│  catalog)    │  │              │  │                                      │
└──────┬───────┘  └──────┬───────┘  └───────────────────┬──────────────────┘
       │                 │                              │
       └─────────────────┼──────────────────────────────┘
                         │
                         ▼
              ┌──────────────────────┐
              │   DATABRICKS (GCP)   │
              │   Unity Catalog      │
              │   Single governance  │
              │   across all clouds  │
              └──────────────────────┘
```

---

## Completed Phases (for reference)

- [x] Phase 1: GCP Infrastructure (Terraform)
- [x] Phase 2: Databricks Workspace + Unity Catalog
- [x] Phase 3: Data Generation (batch + streaming)
- [x] Phase 4: DLT Batch Pipeline (Medallion)
- [x] Phase 5: Governance (tags, PII, masking, DQ, lineage, freshness)
- [x] Phase 6: Data Virtualization (virtual views)
- [x] Phase 8: Streaming Ingestion (Confluent Kafka → DLT)
