# Databricks notebook source
# MAGIC %md
# MAGIC # E-Commerce DLT Pipeline — Medallion Architecture
# MAGIC Bronze → Silver → Gold with data quality expectations

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# =============================================================================
# BRONZE LAYER — Raw ingestion (append-only, no transformations)
# =============================================================================

@dlt.table(
    name="bronze_orders",
    comment="Raw orders from GCS landing zone",
    table_properties={"quality": "bronze", "domain": "ecommerce"}
)
def bronze_orders():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", "gs://${checkpoints_bucket}/schema/orders")
        .load("gs://${landing_bucket}/orders/")
    )


@dlt.table(
    name="bronze_clickstream",
    comment="Raw clickstream events from Pub/Sub",
    table_properties={"quality": "bronze", "domain": "ecommerce"}
)
def bronze_clickstream():
    return (
        spark.readStream
        .format("pubsub")
        .option("subscriptionId", "clickstream-databricks-dev")
        .option("projectId", "${project_id}")
        .load()
        .select(
            col("messageId").alias("event_id"),
            from_json(col("data").cast("string"), clickstream_schema()).alias("payload"),
            col("publishTime").alias("event_ts"),
            current_timestamp().alias("ingestion_ts")
        )
        .select("event_id", "payload.*", "event_ts", "ingestion_ts")
    )


@dlt.table(
    name="bronze_transactions",
    comment="Raw transaction events from Pub/Sub",
    table_properties={"quality": "bronze", "domain": "ecommerce"}
)
def bronze_transactions():
    return (
        spark.readStream
        .format("pubsub")
        .option("subscriptionId", "transactions-databricks-dev")
        .option("projectId", "${project_id}")
        .load()
        .select(
            col("messageId").alias("txn_id"),
            from_json(col("data").cast("string"), transaction_schema()).alias("payload"),
            col("publishTime").alias("event_ts"),
            current_timestamp().alias("ingestion_ts")
        )
        .select("txn_id", "payload.*", "event_ts", "ingestion_ts")
    )


@dlt.table(
    name="bronze_customers",
    comment="Raw customer data from Cloud SQL (batch)",
    table_properties={"quality": "bronze", "domain": "ecommerce"}
)
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
def bronze_customers():
    return (
        spark.read
        .format("jdbc")
        .option("url", "jdbc:postgresql://${cloudsql_ip}:5432/ecommerce")
        .option("dbtable", "customers")
        .option("user", "ecommerce_app")
        .option("password", dbutils.secrets.get("ecommerce", "cloudsql_password"))
        .load()
        .withColumn("ingestion_ts", current_timestamp())
    )


# =============================================================================
# SILVER LAYER — Cleaned, deduplicated, typed, with DQ expectations
# =============================================================================

@dlt.table(
    name="silver_orders",
    comment="Cleaned orders with DQ validation",
    table_properties={"quality": "silver", "domain": "ecommerce"}
)
@dlt.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
@dlt.expect_or_fail("positive_amount", "total_amount > 0")
@dlt.expect("valid_status", "status IN ('pending','confirmed','shipped','delivered','cancelled')")
def silver_orders():
    return (
        dlt.read_stream("bronze_orders")
        .select(
            col("order_id").cast("string"),
            col("customer_id").cast("string"),
            col("product_id").cast("string"),
            col("quantity").cast("int"),
            col("total_amount").cast("decimal(10,2)"),
            col("status").cast("string"),
            col("order_date").cast("timestamp"),
            col("ingestion_ts")
        )
        .withWatermark("ingestion_ts", "1 hour")
        .dropDuplicates(["order_id"])
    )


@dlt.table(
    name="silver_clickstream",
    comment="Deduplicated clickstream with session enrichment",
    table_properties={"quality": "silver", "domain": "ecommerce"}
)
@dlt.expect_or_drop("valid_event", "event_id IS NOT NULL AND user_id IS NOT NULL")
@dlt.expect("valid_event_type", "event_type IN ('page_view','add_to_cart','remove_from_cart','checkout','search')")
def silver_clickstream():
    return (
        dlt.read_stream("bronze_clickstream")
        .withWatermark("event_ts", "30 minutes")
        .dropDuplicates(["event_id"])
        .select(
            "event_id", "user_id", "session_id", "event_type",
            "page_url", "product_id", "event_ts", "ingestion_ts"
        )
    )


@dlt.table(
    name="silver_transactions",
    comment="Validated financial transactions",
    table_properties={"quality": "silver", "domain": "ecommerce", "contains_pii": "false"}
)
@dlt.expect_or_fail("valid_txn_id", "txn_id IS NOT NULL")
@dlt.expect_or_fail("positive_amount", "amount > 0")
@dlt.expect("valid_currency", "currency IN ('GBP','USD','EUR')")
def silver_transactions():
    return (
        dlt.read_stream("bronze_transactions")
        .withWatermark("event_ts", "1 hour")
        .dropDuplicates(["txn_id"])
        .select(
            "txn_id", "order_id", "customer_id", "amount",
            "currency", "payment_method", "status", "event_ts", "ingestion_ts"
        )
    )


@dlt.table(
    name="silver_customers",
    comment="Cleaned customer profiles (PII tagged)",
    table_properties={"quality": "silver", "domain": "ecommerce", "contains_pii": "true"}
)
@dlt.expect_or_drop("valid_customer", "customer_id IS NOT NULL")
@dlt.expect("valid_email", "email RLIKE '^[^@]+@[^@]+\\\\.[^@]+$'")
def silver_customers():
    return (
        dlt.read("bronze_customers")
        .select(
            col("customer_id").cast("string"),
            col("email"),       # PII — will be masked in views
            col("phone"),       # PII
            col("first_name"),  # PII
            col("last_name"),   # PII
            col("city"),
            col("country"),
            col("signup_date").cast("date"),
            col("segment"),
            col("ingestion_ts")
        )
    )


# =============================================================================
# GOLD LAYER — Business aggregations
# =============================================================================

@dlt.table(
    name="gold_customer_360",
    comment="Unified customer view with lifetime metrics",
    table_properties={"quality": "gold", "domain": "ecommerce"}
)
def gold_customer_360():
    customers = dlt.read("silver_customers")
    orders = dlt.read("silver_orders")
    transactions = dlt.read("silver_transactions")

    order_metrics = (
        orders.groupBy("customer_id")
        .agg(
            count("order_id").alias("total_orders"),
            sum("total_amount").alias("lifetime_value"),
            max("order_date").alias("last_order_date"),
            min("order_date").alias("first_order_date")
        )
    )

    txn_metrics = (
        transactions.groupBy("customer_id")
        .agg(
            countDistinct("payment_method").alias("payment_methods_used"),
            avg("amount").alias("avg_transaction_amount")
        )
    )

    return (
        customers
        .join(order_metrics, "customer_id", "left")
        .join(txn_metrics, "customer_id", "left")
        .select(
            customers.customer_id,
            customers.email,
            customers.city,
            customers.country,
            customers.segment,
            customers.signup_date,
            coalesce(order_metrics.total_orders, lit(0)).alias("total_orders"),
            coalesce(order_metrics.lifetime_value, lit(0)).alias("lifetime_value"),
            order_metrics.last_order_date,
            order_metrics.first_order_date,
            txn_metrics.payment_methods_used,
            txn_metrics.avg_transaction_amount
        )
    )


@dlt.table(
    name="gold_daily_revenue",
    comment="Daily revenue aggregation by segment and country",
    table_properties={"quality": "gold", "domain": "ecommerce"}
)
def gold_daily_revenue():
    orders = dlt.read("silver_orders")
    customers = dlt.read("silver_customers")

    return (
        orders
        .join(customers, "customer_id", "left")
        .groupBy(
            orders.order_date.cast("date").alias("date"),
            customers.country,
            customers.segment
        )
        .agg(
            sum("total_amount").alias("revenue"),
            count("order_id").alias("order_count"),
            countDistinct("customer_id").alias("unique_customers")
        )
    )


@dlt.table(
    name="gold_product_engagement",
    comment="Product engagement metrics from clickstream",
    table_properties={"quality": "gold", "domain": "ecommerce"}
)
def gold_product_engagement():
    clicks = dlt.read("silver_clickstream")

    return (
        clicks
        .filter(col("product_id").isNotNull())
        .groupBy("product_id", col("event_ts").cast("date").alias("date"))
        .agg(
            count(when(col("event_type") == "page_view", 1)).alias("views"),
            count(when(col("event_type") == "add_to_cart", 1)).alias("add_to_carts"),
            count(when(col("event_type") == "checkout", 1)).alias("checkouts"),
            countDistinct("user_id").alias("unique_users")
        )
        .withColumn("conversion_rate",
            col("checkouts") / when(col("views") > 0, col("views")).otherwise(1)
        )
    )


# =============================================================================
# SCHEMAS
# =============================================================================

def clickstream_schema():
    return StructType([
        StructField("user_id", StringType()),
        StructField("session_id", StringType()),
        StructField("event_type", StringType()),
        StructField("page_url", StringType()),
        StructField("product_id", StringType()),
    ])

def transaction_schema():
    return StructType([
        StructField("order_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("amount", DoubleType()),
        StructField("currency", StringType()),
        StructField("payment_method", StringType()),
        StructField("status", StringType()),
    ])
