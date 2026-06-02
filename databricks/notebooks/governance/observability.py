# Databricks notebook source
# MAGIC %md
# MAGIC # Observability — Pipeline Health, Freshness, Cost
# MAGIC Scheduled job that monitors platform health and writes metrics to governance schema.

# COMMAND ----------

from pyspark.sql.functions import *
from datetime import datetime

run_ts = datetime.utcnow()

# =============================================================================
# 1. PIPELINE HEALTH — DLT Event Logs
# =============================================================================

# COMMAND ----------
# MAGIC %sql
# MAGIC -- DLT pipeline run summary (last 24h)
# MAGIC SELECT
# MAGIC   pipeline_id,
# MAGIC   origin.update_id,
# MAGIC   timestamp,
# MAGIC   level,
# MAGIC   message
# MAGIC FROM event_log(TABLE(ecommerce_dev.bronze.bronze_orders))
# MAGIC WHERE timestamp > current_timestamp() - INTERVAL 24 HOURS
# MAGIC   AND level IN ('ERROR', 'WARN')
# MAGIC ORDER BY timestamp DESC;

# COMMAND ----------
# MAGIC %sql
# MAGIC -- DLT data quality metrics
# MAGIC SELECT
# MAGIC   row_expectations.dataset AS table_name,
# MAGIC   row_expectations.name AS expectation_name,
# MAGIC   SUM(row_expectations.passed_records) AS passed,
# MAGIC   SUM(row_expectations.failed_records) AS failed,
# MAGIC   ROUND(SUM(row_expectations.passed_records) /
# MAGIC     (SUM(row_expectations.passed_records) + SUM(row_expectations.failed_records)) * 100, 2
# MAGIC   ) AS pass_rate_pct
# MAGIC FROM event_log(TABLE(ecommerce_dev.bronze.bronze_orders)),
# MAGIC   LATERAL VARIANT_EXPLODE(expectations) AS row_expectations
# MAGIC WHERE timestamp > current_timestamp() - INTERVAL 24 HOURS
# MAGIC GROUP BY 1, 2
# MAGIC ORDER BY pass_rate_pct ASC;

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. FRESHNESS MONITORING

# COMMAND ----------

# Check freshness against SLA tags
freshness_checks = spark.sql("""
    WITH table_freshness AS (
        SELECT 'silver_orders' AS table_name, MAX(ingestion_ts) AS last_ingestion FROM ecommerce_dev.silver.silver_orders
        UNION ALL
        SELECT 'silver_clickstream', MAX(ingestion_ts) FROM ecommerce_dev.silver.silver_clickstream
        UNION ALL
        SELECT 'silver_transactions', MAX(ingestion_ts) FROM ecommerce_dev.silver.silver_transactions
    )
    SELECT
        table_name,
        last_ingestion,
        TIMESTAMPDIFF(MINUTE, last_ingestion, current_timestamp()) AS staleness_minutes,
        CASE
            WHEN TIMESTAMPDIFF(MINUTE, last_ingestion, current_timestamp()) > 60 THEN 'BREACHED'
            WHEN TIMESTAMPDIFF(MINUTE, last_ingestion, current_timestamp()) > 45 THEN 'WARNING'
            ELSE 'OK'
        END AS freshness_status
    FROM table_freshness
""")

freshness_checks.show()

# Write to governance
(freshness_checks
    .withColumn("check_timestamp", lit(run_ts))
    .write.mode("append")
    .saveAsTable("ecommerce_dev.governance.freshness_metrics"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. COST TRACKING (System Tables)

# COMMAND ----------
# MAGIC %sql
# MAGIC -- DBU consumption by workspace and SKU (last 7 days)
# MAGIC SELECT
# MAGIC   usage_date,
# MAGIC   sku_name,
# MAGIC   usage_unit,
# MAGIC   SUM(usage_quantity) AS total_dbus
# MAGIC FROM system.billing.usage
# MAGIC WHERE usage_date >= current_date() - INTERVAL 7 DAYS
# MAGIC GROUP BY 1, 2, 3
# MAGIC ORDER BY usage_date DESC, total_dbus DESC;

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Cost by job/pipeline
# MAGIC SELECT
# MAGIC   usage_metadata.job_id,
# MAGIC   usage_metadata.job_name,
# MAGIC   sku_name,
# MAGIC   SUM(usage_quantity) AS total_dbus,
# MAGIC   ROUND(SUM(usage_quantity) * 0.07, 2) AS estimated_cost_usd  -- approximate rate
# MAGIC FROM system.billing.usage
# MAGIC WHERE usage_date >= current_date() - INTERVAL 7 DAYS
# MAGIC   AND usage_metadata.job_id IS NOT NULL
# MAGIC GROUP BY 1, 2, 3
# MAGIC ORDER BY total_dbus DESC;

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. ACCESS AUDIT — Who's Querying What

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Top table consumers (last 7 days)
# MAGIC SELECT
# MAGIC   user_identity.email,
# MAGIC   request_params.full_name_arg AS table_name,
# MAGIC   COUNT(*) AS access_count,
# MAGIC   MAX(event_date) AS last_access
# MAGIC FROM system.access.audit
# MAGIC WHERE action_name = 'commandSubmit'
# MAGIC   AND event_date >= current_date() - INTERVAL 7 DAYS
# MAGIC   AND request_params.full_name_arg LIKE 'ecommerce_dev.%'
# MAGIC GROUP BY 1, 2
# MAGIC ORDER BY access_count DESC
# MAGIC LIMIT 20;

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. VOLUME & GROWTH TRACKING

# COMMAND ----------

# Track table sizes over time
table_stats = spark.sql("""
    SELECT
        table_catalog, table_schema, table_name,
        data_source_format,
        ROUND(bytes / (1024*1024*1024), 3) AS size_gb,
        rows AS row_count
    FROM system.information_schema.tables
    WHERE table_catalog = 'ecommerce_dev'
      AND table_schema IN ('bronze', 'silver', 'gold')
    ORDER BY bytes DESC
""")

table_stats.show()

(table_stats
    .withColumn("snapshot_timestamp", lit(run_ts))
    .write.mode("append")
    .saveAsTable("ecommerce_dev.governance.table_growth_metrics"))
