# Databricks notebook source
# MAGIC %md
# MAGIC # Data Quality — Business Rules Validation
# MAGIC Scheduled job that validates Gold layer against business rules
# MAGIC and writes results to governance.dq_results for observability.

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

# =============================================================================
# DQ RULES DEFINITION
# =============================================================================

business_rules = [
    # Revenue sanity checks
    {
        "rule_id": "BIZ-001",
        "rule_name": "daily_revenue_non_negative",
        "category": "business",
        "severity": "critical",
        "table": "ecommerce_dev.gold.gold_daily_revenue",
        "sql": "SELECT COUNT(*) FROM ecommerce_dev.gold.gold_daily_revenue WHERE revenue < 0",
        "threshold": 0,
        "description": "No negative revenue values allowed"
    },
    {
        "rule_id": "BIZ-002",
        "rule_name": "daily_revenue_upper_bound",
        "category": "business",
        "severity": "warning",
        "table": "ecommerce_dev.gold.gold_daily_revenue",
        "sql": "SELECT COUNT(*) FROM ecommerce_dev.gold.gold_daily_revenue WHERE revenue > 10000000",
        "threshold": 0,
        "description": "Revenue exceeding 10M per day/segment is suspicious"
    },
    # Referential integrity
    {
        "rule_id": "BIZ-003",
        "rule_name": "no_orphan_orders",
        "category": "integrity",
        "severity": "critical",
        "table": "ecommerce_dev.silver.silver_orders",
        "sql": """
            SELECT COUNT(*) FROM ecommerce_dev.silver.silver_orders o
            LEFT JOIN ecommerce_dev.silver.silver_customers c ON o.customer_id = c.customer_id
            WHERE c.customer_id IS NULL
        """,
        "threshold": 0,
        "description": "All orders must have a matching customer"
    },
    # Completeness
    {
        "rule_id": "BIZ-004",
        "rule_name": "customer_email_completeness",
        "category": "completeness",
        "severity": "warning",
        "table": "ecommerce_dev.gold.gold_customer_360",
        "sql": """
            SELECT ROUND((1 - COUNT(CASE WHEN email IS NULL THEN 1 END) / COUNT(*)) * 100, 2)
            FROM ecommerce_dev.gold.gold_customer_360
        """,
        "threshold": 95,  # At least 95% emails present
        "comparison": "gte",
        "description": "Email completeness must be >= 95%"
    },
    # Freshness
    {
        "rule_id": "BIZ-005",
        "rule_name": "orders_freshness",
        "category": "freshness",
        "severity": "critical",
        "table": "ecommerce_dev.silver.silver_orders",
        "sql": """
            SELECT TIMESTAMPDIFF(MINUTE, MAX(ingestion_ts), current_timestamp())
            FROM ecommerce_dev.silver.silver_orders
        """,
        "threshold": 60,  # Must be fresher than 60 minutes
        "comparison": "lte",
        "description": "Orders data must be no older than 60 minutes"
    },
    # Uniqueness
    {
        "rule_id": "BIZ-006",
        "rule_name": "customer_id_uniqueness",
        "category": "uniqueness",
        "severity": "critical",
        "table": "ecommerce_dev.gold.gold_customer_360",
        "sql": """
            SELECT COUNT(*) - COUNT(DISTINCT customer_id)
            FROM ecommerce_dev.gold.gold_customer_360
        """,
        "threshold": 0,
        "description": "customer_id must be unique in customer_360"
    },
    # Conversion rate sanity
    {
        "rule_id": "BIZ-007",
        "rule_name": "conversion_rate_bounds",
        "category": "business",
        "severity": "warning",
        "table": "ecommerce_dev.gold.gold_product_engagement",
        "sql": """
            SELECT COUNT(*) FROM ecommerce_dev.gold.gold_product_engagement
            WHERE conversion_rate > 1.0 OR conversion_rate < 0
        """,
        "threshold": 0,
        "description": "Conversion rate must be between 0 and 1"
    },
]

# =============================================================================
# EXECUTE DQ CHECKS
# =============================================================================

results = []
run_ts = datetime.utcnow()

for rule in business_rules:
    try:
        result_value = spark.sql(rule["sql"]).collect()[0][0]
        comparison = rule.get("comparison", "lte")  # default: result <= threshold = pass

        if comparison == "lte":
            passed = result_value <= rule["threshold"]
        elif comparison == "gte":
            passed = result_value >= rule["threshold"]
        else:
            passed = result_value == rule["threshold"]

        results.append({
            "run_timestamp": run_ts,
            "rule_id": rule["rule_id"],
            "rule_name": rule["rule_name"],
            "category": rule["category"],
            "severity": rule["severity"],
            "table_name": rule["table"],
            "description": rule["description"],
            "threshold": float(rule["threshold"]),
            "actual_value": float(result_value) if result_value else 0.0,
            "passed": passed,
            "error_message": None
        })
    except Exception as e:
        results.append({
            "run_timestamp": run_ts,
            "rule_id": rule["rule_id"],
            "rule_name": rule["rule_name"],
            "category": rule["category"],
            "severity": rule["severity"],
            "table_name": rule["table"],
            "description": rule["description"],
            "threshold": float(rule["threshold"]),
            "actual_value": -1.0,
            "passed": False,
            "error_message": str(e)[:500]
        })

# =============================================================================
# WRITE RESULTS TO GOVERNANCE TABLE
# =============================================================================

results_df = spark.createDataFrame(results)

results_df.write.mode("append").saveAsTable("ecommerce_dev.governance.dq_results")

# =============================================================================
# SUMMARY + ALERTING
# =============================================================================

failed_critical = [r for r in results if not r["passed"] and r["severity"] == "critical"]
failed_warning = [r for r in results if not r["passed"] and r["severity"] == "warning"]

print(f"DQ Run Complete: {run_ts}")
print(f"  Total rules: {len(results)}")
print(f"  Passed: {sum(1 for r in results if r['passed'])}")
print(f"  Failed (critical): {len(failed_critical)}")
print(f"  Failed (warning): {len(failed_warning)}")

if failed_critical:
    print("\n⚠️ CRITICAL FAILURES:")
    for r in failed_critical:
        print(f"  [{r['rule_id']}] {r['rule_name']}: expected {r['threshold']}, got {r['actual_value']}")
    # In production: trigger alert via webhook/Pub/Sub
    # requests.post(alert_webhook_url, json={"failures": failed_critical})
