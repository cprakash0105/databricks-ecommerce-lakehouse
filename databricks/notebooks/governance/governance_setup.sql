-- =============================================================================
-- GOVERNANCE SETUP — Run in Databricks SQL Editor
-- =============================================================================
-- Execute each section one at a time (select the block and run)

-- =============================================================================
-- 1. ACTIVE METADATA — Catalog & Schema Tags
-- =============================================================================

ALTER CATALOG ecommerce_dev SET TAGS ('domain' = 'ecommerce', 'owner' = 'data-platform-team', 'environment' = 'dev');

ALTER SCHEMA ecommerce_dev.default SET TAGS ('layer' = 'all', 'pipeline' = 'ecommerce-lakehouse');
ALTER SCHEMA ecommerce_dev.governance SET TAGS ('purpose' = 'observability', 'owner' = 'platform-team');

-- =============================================================================
-- 2. ACTIVE METADATA — Table Tags
-- =============================================================================

-- Bronze tables
ALTER TABLE ecommerce_dev.default.bronze_customers SET TAGS (
  'layer' = 'bronze',
  'domain' = 'customer',
  'quality' = 'raw',
  'source' = 'gcs-landing-zone',
  'ingestion_pattern' = 'auto-loader',
  'contains_pii' = 'true'
);

ALTER TABLE ecommerce_dev.default.bronze_orders SET TAGS (
  'layer' = 'bronze',
  'domain' = 'orders',
  'quality' = 'raw',
  'source' = 'gcs-landing-zone',
  'ingestion_pattern' = 'auto-loader',
  'contains_pii' = 'false'
);

-- Silver tables
ALTER TABLE ecommerce_dev.default.silver_customers SET TAGS (
  'layer' = 'silver',
  'domain' = 'customer',
  'quality' = 'validated',
  'owner' = 'customer-analytics-team',
  'contains_pii' = 'true',
  'freshness_sla' = '1h',
  'data_locality' = 'europe-west2'
);

ALTER TABLE ecommerce_dev.default.silver_orders SET TAGS (
  'layer' = 'silver',
  'domain' = 'orders',
  'quality' = 'validated',
  'owner' = 'order-management-team',
  'contains_pii' = 'false',
  'freshness_sla' = '1h'
);

-- Gold tables
ALTER TABLE ecommerce_dev.default.gold_customer_360 SET TAGS (
  'layer' = 'gold',
  'domain' = 'customer',
  'quality' = 'business-ready',
  'owner' = 'customer-analytics-team',
  'contains_pii' = 'true',
  'freshness_sla' = '1h',
  'data_locality' = 'europe-west2',
  'consumers' = 'bi-team,marketing-team'
);

ALTER TABLE ecommerce_dev.default.gold_daily_revenue SET TAGS (
  'layer' = 'gold',
  'domain' = 'finance',
  'quality' = 'business-ready',
  'owner' = 'revenue-team',
  'contains_pii' = 'false',
  'freshness_sla' = '24h',
  'consumers' = 'finance-team,exec-dashboard'
);

-- =============================================================================
-- 3. PII COLUMN CLASSIFICATION
-- =============================================================================

-- Bronze customers PII
ALTER TABLE ecommerce_dev.default.bronze_customers ALTER COLUMN email SET TAGS ('classification' = 'pii', 'pii_type' = 'email');
ALTER TABLE ecommerce_dev.default.bronze_customers ALTER COLUMN phone SET TAGS ('classification' = 'pii', 'pii_type' = 'phone');
ALTER TABLE ecommerce_dev.default.bronze_customers ALTER COLUMN first_name SET TAGS ('classification' = 'pii', 'pii_type' = 'name');
ALTER TABLE ecommerce_dev.default.bronze_customers ALTER COLUMN last_name SET TAGS ('classification' = 'pii', 'pii_type' = 'name');

-- Silver customers PII
ALTER TABLE ecommerce_dev.default.silver_customers ALTER COLUMN email SET TAGS ('classification' = 'pii', 'pii_type' = 'email');
ALTER TABLE ecommerce_dev.default.silver_customers ALTER COLUMN phone SET TAGS ('classification' = 'pii', 'pii_type' = 'phone');
ALTER TABLE ecommerce_dev.default.silver_customers ALTER COLUMN first_name SET TAGS ('classification' = 'pii', 'pii_type' = 'name');
ALTER TABLE ecommerce_dev.default.silver_customers ALTER COLUMN last_name SET TAGS ('classification' = 'pii', 'pii_type' = 'name');

-- Gold customer_360 PII
ALTER TABLE ecommerce_dev.default.gold_customer_360 ALTER COLUMN email SET TAGS ('classification' = 'pii', 'pii_type' = 'email', 'masking_policy' = 'partial_mask');

-- =============================================================================
-- 4. PII MASKING VIEWS
-- =============================================================================

CREATE OR REPLACE VIEW ecommerce_dev.default.v_customer_360_masked AS
SELECT
  customer_id,
  CASE
    WHEN is_account_group_member('pii_readers') THEN email
    ELSE CONCAT(LEFT(email, 2), '***@', SUBSTRING_INDEX(email, '@', -1))
  END AS email,
  city,
  country,
  segment,
  signup_date,
  total_orders,
  lifetime_value,
  last_order_date,
  first_order_date
FROM ecommerce_dev.default.gold_customer_360;

CREATE OR REPLACE VIEW ecommerce_dev.default.v_silver_customers_masked AS
SELECT
  customer_id,
  CASE
    WHEN is_account_group_member('pii_readers') THEN email
    ELSE CONCAT(LEFT(email, 2), '***@', SUBSTRING_INDEX(email, '@', -1))
  END AS email,
  CASE
    WHEN is_account_group_member('pii_readers') THEN phone
    ELSE CONCAT('***-', RIGHT(phone, 4))
  END AS phone,
  CASE
    WHEN is_account_group_member('pii_readers') THEN first_name
    ELSE CONCAT(LEFT(first_name, 1), '***')
  END AS first_name,
  CASE
    WHEN is_account_group_member('pii_readers') THEN last_name
    ELSE CONCAT(LEFT(last_name, 1), '***')
  END AS last_name,
  city,
  country,
  segment,
  signup_date,
  ingestion_ts
FROM ecommerce_dev.default.silver_customers;

-- =============================================================================
-- 5. ACCESS CONTROL GROUPS
-- =============================================================================

-- Create groups (may already exist)
-- Run these one at a time, ignore errors if groups exist

CREATE GROUP IF NOT EXISTS analysts;
CREATE GROUP IF NOT EXISTS pii_readers;
CREATE GROUP IF NOT EXISTS data_engineers;

-- Analysts get masked views only
GRANT USE CATALOG ON CATALOG ecommerce_dev TO analysts;
GRANT USE SCHEMA ON SCHEMA ecommerce_dev.default TO analysts;
GRANT SELECT ON VIEW ecommerce_dev.default.v_customer_360_masked TO analysts;
GRANT SELECT ON VIEW ecommerce_dev.default.v_silver_customers_masked TO analysts;
GRANT SELECT ON TABLE ecommerce_dev.default.gold_daily_revenue TO analysts;

-- PII readers get raw tables
GRANT USE CATALOG ON CATALOG ecommerce_dev TO pii_readers;
GRANT USE SCHEMA ON SCHEMA ecommerce_dev.default TO pii_readers;
GRANT SELECT ON TABLE ecommerce_dev.default.gold_customer_360 TO pii_readers;
GRANT SELECT ON TABLE ecommerce_dev.default.silver_customers TO pii_readers;

-- =============================================================================
-- 6. GOVERNANCE TABLES (for DQ results and observability)
-- =============================================================================

CREATE TABLE IF NOT EXISTS ecommerce_dev.governance.dq_results (
  run_timestamp TIMESTAMP,
  rule_id STRING,
  rule_name STRING,
  category STRING,
  severity STRING,
  table_name STRING,
  description STRING,
  threshold DOUBLE,
  actual_value DOUBLE,
  passed BOOLEAN,
  error_message STRING
);

CREATE TABLE IF NOT EXISTS ecommerce_dev.governance.freshness_metrics (
  table_name STRING,
  last_ingestion TIMESTAMP,
  staleness_minutes LONG,
  freshness_status STRING,
  check_timestamp TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ecommerce_dev.governance.table_growth_metrics (
  table_catalog STRING,
  table_schema STRING,
  table_name STRING,
  data_source_format STRING,
  size_gb DOUBLE,
  row_count LONG,
  snapshot_timestamp TIMESTAMP
);

-- =============================================================================
-- 7. BUSINESS DQ RULES — Run as a validation check
-- =============================================================================

-- Rule BIZ-001: No negative revenue
INSERT INTO ecommerce_dev.governance.dq_results
SELECT
  current_timestamp() AS run_timestamp,
  'BIZ-001' AS rule_id,
  'daily_revenue_non_negative' AS rule_name,
  'business' AS category,
  'critical' AS severity,
  'ecommerce_dev.default.gold_daily_revenue' AS table_name,
  'No negative revenue values allowed' AS description,
  0.0 AS threshold,
  CAST(COUNT(*) AS DOUBLE) AS actual_value,
  COUNT(*) = 0 AS passed,
  NULL AS error_message
FROM ecommerce_dev.default.gold_daily_revenue
WHERE revenue < 0;

-- Rule BIZ-002: No orphan orders
INSERT INTO ecommerce_dev.governance.dq_results
SELECT
  current_timestamp() AS run_timestamp,
  'BIZ-002' AS rule_id,
  'no_orphan_orders' AS rule_name,
  'integrity' AS category,
  'critical' AS severity,
  'ecommerce_dev.default.silver_orders' AS table_name,
  'All orders must have a matching customer' AS description,
  0.0 AS threshold,
  CAST(COUNT(*) AS DOUBLE) AS actual_value,
  COUNT(*) = 0 AS passed,
  NULL AS error_message
FROM ecommerce_dev.default.silver_orders o
LEFT JOIN ecommerce_dev.default.silver_customers c ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

-- Rule BIZ-003: Email completeness >= 95%
INSERT INTO ecommerce_dev.governance.dq_results
SELECT
  current_timestamp() AS run_timestamp,
  'BIZ-003' AS rule_id,
  'customer_email_completeness' AS rule_name,
  'completeness' AS category,
  'warning' AS severity,
  'ecommerce_dev.default.gold_customer_360' AS table_name,
  'Email completeness must be >= 95%' AS description,
  95.0 AS threshold,
  ROUND((COUNT(email) * 100.0 / COUNT(*)), 2) AS actual_value,
  (COUNT(email) * 100.0 / COUNT(*)) >= 95.0 AS passed,
  NULL AS error_message
FROM ecommerce_dev.default.gold_customer_360;

-- Rule BIZ-004: Customer ID uniqueness
INSERT INTO ecommerce_dev.governance.dq_results
SELECT
  current_timestamp() AS run_timestamp,
  'BIZ-004' AS rule_id,
  'customer_id_uniqueness' AS rule_name,
  'uniqueness' AS category,
  'critical' AS severity,
  'ecommerce_dev.default.gold_customer_360' AS table_name,
  'customer_id must be unique in customer_360' AS description,
  0.0 AS threshold,
  CAST((COUNT(*) - COUNT(DISTINCT customer_id)) AS DOUBLE) AS actual_value,
  (COUNT(*) - COUNT(DISTINCT customer_id)) = 0 AS passed,
  NULL AS error_message
FROM ecommerce_dev.default.gold_customer_360;

-- Rule BIZ-005: Revenue upper bound check
INSERT INTO ecommerce_dev.governance.dq_results
SELECT
  current_timestamp() AS run_timestamp,
  'BIZ-005' AS rule_id,
  'daily_revenue_upper_bound' AS rule_name,
  'business' AS category,
  'warning' AS severity,
  'ecommerce_dev.default.gold_daily_revenue' AS table_name,
  'Revenue exceeding 10M per day/segment is suspicious' AS description,
  0.0 AS threshold,
  CAST(COUNT(*) AS DOUBLE) AS actual_value,
  COUNT(*) = 0 AS passed,
  NULL AS error_message
FROM ecommerce_dev.default.gold_daily_revenue
WHERE revenue > 10000000;

-- =============================================================================
-- 8. VIEW DQ RESULTS
-- =============================================================================

SELECT * FROM ecommerce_dev.governance.dq_results ORDER BY run_timestamp DESC;

-- =============================================================================
-- 9. LINEAGE QUERIES (may require system table access)
-- =============================================================================

-- Table lineage: what feeds gold_customer_360?
-- SELECT source_table_full_name, target_table_full_name, event_time
-- FROM system.access.table_lineage
-- WHERE target_table_full_name = 'ecommerce_dev.default.gold_customer_360'
-- ORDER BY event_time DESC LIMIT 20;

-- Column lineage: where does lifetime_value come from?
-- SELECT source_table_full_name, source_column_name, target_column_name
-- FROM system.access.column_lineage
-- WHERE target_table_full_name = 'ecommerce_dev.default.gold_customer_360'
--   AND target_column_name = 'lifetime_value';

-- =============================================================================
-- 10. FRESHNESS CHECK
-- =============================================================================

INSERT INTO ecommerce_dev.governance.freshness_metrics
SELECT
  table_name,
  last_ingestion,
  staleness_minutes,
  CASE
    WHEN staleness_minutes > 60 THEN 'BREACHED'
    WHEN staleness_minutes > 45 THEN 'WARNING'
    ELSE 'OK'
  END AS freshness_status,
  current_timestamp() AS check_timestamp
FROM (
  SELECT 'silver_customers' AS table_name, MAX(ingestion_ts) AS last_ingestion,
    TIMESTAMPDIFF(MINUTE, MAX(ingestion_ts), current_timestamp()) AS staleness_minutes
  FROM ecommerce_dev.default.silver_customers
  UNION ALL
  SELECT 'silver_orders', MAX(ingestion_ts),
    TIMESTAMPDIFF(MINUTE, MAX(ingestion_ts), current_timestamp())
  FROM ecommerce_dev.default.silver_orders
);

SELECT * FROM ecommerce_dev.governance.freshness_metrics ORDER BY check_timestamp DESC;

-- =============================================================================
-- 11. VERIFY GOVERNANCE SETUP
-- =============================================================================

-- Check all tags applied
SELECT * FROM system.information_schema.table_tags WHERE catalog_name = 'ecommerce_dev';

-- Check column tags (PII)
SELECT * FROM system.information_schema.column_tags WHERE catalog_name = 'ecommerce_dev' AND tag_name = 'classification';

-- Check masked view works
SELECT * FROM ecommerce_dev.default.v_customer_360_masked LIMIT 5;
