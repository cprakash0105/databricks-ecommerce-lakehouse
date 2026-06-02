# Databricks notebook source
# MAGIC %md
# MAGIC # E-Commerce DLT Pipeline — Batch (V1)
# MAGIC Bronze → Silver → Gold using Auto Loader from GCS

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# =============================================================================
# BRONZE LAYER
# =============================================================================

@dlt.table(
    name="bronze_customers",
    comment="Raw customers from GCS landing zone",
    table_properties={"quality": "bronze", "domain": "ecommerce"}
)
def bronze_customers():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", "gs://cp-ecomm-demo-checkpoints-dev/schema/customers")
        .load("gs://cp-ecomm-demo-landing-dev/customers/")
        .withColumn("ingestion_ts", current_timestamp())
    )


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
        .option("cloudFiles.schemaLocation", "gs://cp-ecomm-demo-checkpoints-dev/schema/orders")
        .load("gs://cp-ecomm-demo-landing-dev/orders/")
        .withColumn("ingestion_ts", current_timestamp())
    )


# =============================================================================
# SILVER LAYER
# =============================================================================

@dlt.table(
    name="silver_customers",
    comment="Cleaned customer profiles",
    table_properties={"quality": "silver", "domain": "ecommerce", "contains_pii": "true"}
)
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect("valid_email", "email RLIKE '^[^@]+@[^@]+\\\\.[^@]+$'")
def silver_customers():
    return (
        dlt.read_stream("bronze_customers")
        .select(
            col("customer_id").cast("string"),
            col("email").cast("string"),
            col("phone").cast("string"),
            col("first_name").cast("string"),
            col("last_name").cast("string"),
            col("city").cast("string"),
            col("country").cast("string"),
            col("segment").cast("string"),
            col("signup_date").cast("date"),
            col("ingestion_ts")
        )
    )


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
        .dropDuplicates(["order_id"])
    )


# =============================================================================
# GOLD LAYER
# =============================================================================

@dlt.table(
    name="gold_customer_360",
    comment="Unified customer view with lifetime metrics",
    table_properties={"quality": "gold", "domain": "ecommerce"}
)
def gold_customer_360():
    customers = dlt.read("silver_customers")
    orders = dlt.read("silver_orders")

    order_metrics = (
        orders.groupBy("customer_id")
        .agg(
            count("order_id").alias("total_orders"),
            sum("total_amount").alias("lifetime_value"),
            max("order_date").alias("last_order_date"),
            min("order_date").alias("first_order_date")
        )
    )

    return (
        customers
        .join(order_metrics, "customer_id", "left")
        .select(
            customers.customer_id,
            customers.email,
            customers.city,
            customers.country,
            customers.segment,
            customers.signup_date,
            coalesce(order_metrics.total_orders, lit(0)).alias("total_orders"),
            coalesce(order_metrics.lifetime_value, lit(0.0)).alias("lifetime_value"),
            order_metrics.last_order_date,
            order_metrics.first_order_date
        )
    )


@dlt.table(
    name="gold_daily_revenue",
    comment="Daily revenue by country and segment",
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
