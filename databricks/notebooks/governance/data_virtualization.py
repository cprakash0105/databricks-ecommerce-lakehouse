# Databricks notebook source
# MAGIC %md
# MAGIC # Data Virtualization — Lakehouse Federation
# MAGIC Query Cloud SQL (live operational data) alongside lakehouse tables
# MAGIC without copying data.

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Create Foreign Connection to Cloud SQL

# COMMAND ----------
# MAGIC %sql
# MAGIC CREATE CONNECTION IF NOT EXISTS cloudsql_ecommerce
# MAGIC TYPE POSTGRESQL
# MAGIC OPTIONS (
# MAGIC   host '${cloudsql_private_ip}',
# MAGIC   port '5432',
# MAGIC   user secret('ecommerce', 'cloudsql_user'),
# MAGIC   password secret('ecommerce', 'cloudsql_password')
# MAGIC );

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Create Foreign Catalog

# COMMAND ----------
# MAGIC %sql
# MAGIC CREATE FOREIGN CATALOG IF NOT EXISTS cloudsql_live
# MAGIC USING CONNECTION cloudsql_ecommerce
# MAGIC OPTIONS (database 'ecommerce');

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Virtual Views — Unified Access Layer
# MAGIC These views join lakehouse (Gold) with live operational data.

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Real-time customer view: lakehouse profile + live session data
# MAGIC CREATE OR REPLACE VIEW ecommerce_dev.gold.customer_realtime AS
# MAGIC SELECT
# MAGIC   c.customer_id,
# MAGIC   c.segment,
# MAGIC   c.lifetime_value,
# MAGIC   c.total_orders,
# MAGIC   c.last_order_date,
# MAGIC   s.session_id,
# MAGIC   s.current_page,
# MAGIC   s.cart_value,
# MAGIC   s.last_activity_ts,
# MAGIC   CASE
# MAGIC     WHEN s.session_id IS NOT NULL
# MAGIC       AND s.last_activity_ts > current_timestamp() - INTERVAL 15 MINUTES
# MAGIC     THEN 'active'
# MAGIC     ELSE 'inactive'
# MAGIC   END AS session_status
# MAGIC FROM ecommerce_dev.gold.gold_customer_360 c
# MAGIC LEFT JOIN cloudsql_live.ecommerce.active_sessions s
# MAGIC   ON c.customer_id = s.customer_id;

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Inventory-aware product engagement: lakehouse metrics + live stock
# MAGIC CREATE OR REPLACE VIEW ecommerce_dev.gold.product_engagement_live AS
# MAGIC SELECT
# MAGIC   pe.product_id,
# MAGIC   pe.date,
# MAGIC   pe.views,
# MAGIC   pe.add_to_carts,
# MAGIC   pe.checkouts,
# MAGIC   pe.conversion_rate,
# MAGIC   inv.stock_quantity,
# MAGIC   inv.warehouse_location,
# MAGIC   CASE
# MAGIC     WHEN inv.stock_quantity = 0 THEN 'out_of_stock'
# MAGIC     WHEN inv.stock_quantity < 10 THEN 'low_stock'
# MAGIC     ELSE 'in_stock'
# MAGIC   END AS stock_status
# MAGIC FROM ecommerce_dev.gold.gold_product_engagement pe
# MAGIC LEFT JOIN cloudsql_live.ecommerce.inventory inv
# MAGIC   ON pe.product_id = inv.product_id;

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Example Queries — Virtualization in Action

# COMMAND ----------
# MAGIC %sql
# MAGIC -- High-value customers currently browsing with items in cart
# MAGIC SELECT
# MAGIC   customer_id, segment, lifetime_value,
# MAGIC   cart_value, current_page, session_status
# MAGIC FROM ecommerce_dev.gold.customer_realtime
# MAGIC WHERE lifetime_value > 1000
# MAGIC   AND session_status = 'active'
# MAGIC   AND cart_value > 0
# MAGIC ORDER BY lifetime_value DESC;

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Products with high engagement but low stock (action needed)
# MAGIC SELECT
# MAGIC   product_id, views, add_to_carts, conversion_rate,
# MAGIC   stock_quantity, stock_status
# MAGIC FROM ecommerce_dev.gold.product_engagement_live
# MAGIC WHERE date = current_date()
# MAGIC   AND stock_status IN ('out_of_stock', 'low_stock')
# MAGIC   AND views > 100
# MAGIC ORDER BY views DESC;

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Governance on Federated Tables
# MAGIC Unity Catalog governs foreign tables too — same access control, audit, lineage.

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Tag the foreign catalog
# MAGIC ALTER CATALOG cloudsql_live SET TAGS ('source_type' = 'operational', 'connection' = 'cloudsql');

# MAGIC -- Grant read-only to analysts (they can't modify operational DB)
# MAGIC GRANT USE CATALOG ON CATALOG cloudsql_live TO `analysts`;
# MAGIC GRANT USE SCHEMA ON SCHEMA cloudsql_live.ecommerce TO `analysts`;
# MAGIC GRANT SELECT ON SCHEMA cloudsql_live.ecommerce TO `analysts`;
