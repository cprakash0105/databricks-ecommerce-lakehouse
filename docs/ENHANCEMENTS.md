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

## Completed Phases (for reference)

- [x] Phase 1: GCP Infrastructure (Terraform)
- [x] Phase 2: Databricks Workspace + Unity Catalog
- [x] Phase 3: Data Generation (batch + streaming)
- [x] Phase 4: DLT Batch Pipeline (Medallion)
- [x] Phase 5: Governance (tags, PII, masking, DQ, lineage, freshness)
- [x] Phase 6: Data Virtualization (virtual views)
- [x] Phase 8: Streaming Ingestion (Confluent Kafka → DLT)
