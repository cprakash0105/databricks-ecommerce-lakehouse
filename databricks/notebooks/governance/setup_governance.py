# Databricks notebook source
# MAGIC %md
# MAGIC # Governance Setup — Active Metadata, PII Masking, Lineage
# MAGIC Run once to configure governance controls on the lakehouse.

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Active Metadata — Tags

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Domain and ownership tags
# MAGIC ALTER CATALOG ecommerce_dev SET TAGS ('domain' = 'ecommerce', 'owner' = 'data-platform-team');

# MAGIC ALTER SCHEMA ecommerce_dev.bronze SET TAGS ('layer' = 'bronze', 'quality' = 'raw');
# MAGIC ALTER SCHEMA ecommerce_dev.silver SET TAGS ('layer' = 'silver', 'quality' = 'validated');
# MAGIC ALTER SCHEMA ecommerce_dev.gold SET TAGS ('layer' = 'gold', 'quality' = 'business-ready');

# MAGIC -- Table-level metadata
# MAGIC ALTER TABLE ecommerce_dev.gold.gold_customer_360 SET TAGS (
# MAGIC   'domain' = 'customer',
# MAGIC   'owner' = 'customer-analytics-team',
# MAGIC   'freshness_sla' = '1h',
# MAGIC   'contains_pii' = 'true',
# MAGIC   'data_locality' = 'europe-west2'
# MAGIC );

# MAGIC ALTER TABLE ecommerce_dev.gold.gold_daily_revenue SET TAGS (
# MAGIC   'domain' = 'finance',
# MAGIC   'owner' = 'revenue-team',
# MAGIC   'freshness_sla' = '24h',
# MAGIC   'contains_pii' = 'false'
# MAGIC );

# MAGIC ALTER TABLE ecommerce_dev.gold.gold_product_engagement SET TAGS (
# MAGIC   'domain' = 'product',
# MAGIC   'owner' = 'product-analytics-team',
# MAGIC   'freshness_sla' = '1h',
# MAGIC   'contains_pii' = 'false'
# MAGIC );

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. PII Column Classification

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Tag PII columns in silver_customers
# MAGIC ALTER TABLE ecommerce_dev.silver.silver_customers
# MAGIC   ALTER COLUMN email SET TAGS ('classification' = 'pii', 'pii_type' = 'email');
# MAGIC ALTER TABLE ecommerce_dev.silver.silver_customers
# MAGIC   ALTER COLUMN phone SET TAGS ('classification' = 'pii', 'pii_type' = 'phone');
# MAGIC ALTER TABLE ecommerce_dev.silver.silver_customers
# MAGIC   ALTER COLUMN first_name SET TAGS ('classification' = 'pii', 'pii_type' = 'name');
# MAGIC ALTER TABLE ecommerce_dev.silver.silver_customers
# MAGIC   ALTER COLUMN last_name SET TAGS ('classification' = 'pii', 'pii_type' = 'name');

# MAGIC -- Tag PII in gold
# MAGIC ALTER TABLE ecommerce_dev.gold.gold_customer_360
# MAGIC   ALTER COLUMN email SET TAGS ('classification' = 'pii', 'masking_policy' = 'hash');

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Dynamic PII Masking Views

# COMMAND ----------
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW ecommerce_dev.gold.customer_360_masked AS
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   CASE
# MAGIC     WHEN is_account_group_member('pii_readers') THEN email
# MAGIC     ELSE CONCAT(LEFT(email, 2), '***@', SPLIT(email, '@')[1])
# MAGIC   END AS email,
# MAGIC   city,
# MAGIC   country,
# MAGIC   segment,
# MAGIC   signup_date,
# MAGIC   total_orders,
# MAGIC   lifetime_value,
# MAGIC   last_order_date,
# MAGIC   first_order_date,
# MAGIC   payment_methods_used,
# MAGIC   avg_transaction_amount
# MAGIC FROM ecommerce_dev.gold.gold_customer_360;

# MAGIC -- Grant analysts access to masked view only
# MAGIC GRANT SELECT ON VIEW ecommerce_dev.gold.customer_360_masked TO `analysts`;
# MAGIC -- PII readers get the raw table
# MAGIC GRANT SELECT ON TABLE ecommerce_dev.gold.gold_customer_360 TO `pii_readers`;

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Lineage Queries (System Tables)

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Table-level lineage: what feeds gold_customer_360?
# MAGIC SELECT
# MAGIC   source_table_full_name,
# MAGIC   target_table_full_name,
# MAGIC   event_time
# MAGIC FROM system.access.table_lineage
# MAGIC WHERE target_table_full_name = 'ecommerce_dev.gold.gold_customer_360'
# MAGIC ORDER BY event_time DESC
# MAGIC LIMIT 20;

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Column-level lineage: where does 'lifetime_value' come from?
# MAGIC SELECT
# MAGIC   source_table_full_name,
# MAGIC   source_column_name,
# MAGIC   target_table_full_name,
# MAGIC   target_column_name
# MAGIC FROM system.access.column_lineage
# MAGIC WHERE target_table_full_name = 'ecommerce_dev.gold.gold_customer_360'
# MAGIC   AND target_column_name = 'lifetime_value';

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Data Locality Validation

# COMMAND ----------

from pyspark.sql.functions import *

# Verify all lakehouse data resides in expected region
tables_with_locality = spark.sql("""
    SELECT 
        table_name,
        table_catalog,
        table_schema,
        comment
    FROM system.information_schema.tables
    WHERE table_catalog = 'ecommerce_dev'
""")

# Check GCS file locations for Gold tables
gold_tables = ["gold_customer_360", "gold_daily_revenue", "gold_product_engagement"]
for table in gold_tables:
    files = spark.sql(f"DESCRIBE DETAIL ecommerce_dev.gold.{table}").select("location").collect()
    location = files[0]["location"]
    assert "europe-west2" in location or "eu" in location.lower(), \
        f"DATA LOCALITY VIOLATION: {table} is not in europe-west2! Location: {location}"
    print(f"✓ {table}: {location}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Audit Log Queries

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Who accessed PII tables in the last 7 days?
# MAGIC SELECT
# MAGIC   user_identity.email AS user_email,
# MAGIC   request_params.full_name_arg AS table_accessed,
# MAGIC   action_name,
# MAGIC   event_date,
# MAGIC   source_ip_address
# MAGIC FROM system.access.audit
# MAGIC WHERE action_name IN ('getTable', 'commandSubmit')
# MAGIC   AND request_params.full_name_arg LIKE '%customer%'
# MAGIC   AND event_date >= current_date() - INTERVAL 7 DAYS
# MAGIC ORDER BY event_date DESC;
