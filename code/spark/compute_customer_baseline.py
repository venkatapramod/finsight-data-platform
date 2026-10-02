"""
FinSight — Customer Historical Baseline (Batch)
Section 7.2 support job

The churn streaming job compares each customer's live 24-step window
against two per-customer historical numbers:

  hist_avg_txn_per_12   -> average transaction count per 12-step period
  hist_avg_amount       -> average transaction amount, all-time

Run this once (or nightly) before starting churn_streaming.py; the
stream reads the output as a static reference table.

Input : /finsight/raw/transactions   (Parquet, partitioned by step)
Output: /finsight/reference/customer_baseline (Parquet)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, max as spark_max, min as spark_min

INPUT_PATH = "/finsight/raw/transactions"
INPUT_FORMAT = "parquet"
OUTPUT_PATH = "/finsight/reference/customer_baseline"

spark = (
    SparkSession.builder
    .appName("FinSight-Customer-Baseline")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

if INPUT_FORMAT == "json":
    df = spark.read.json(INPUT_PATH)
else:
    df = spark.read.parquet(INPUT_PATH)

agg = (
    df.groupBy(col("nameOrig").alias("customerId"))
    .agg(
        count("*").alias("txn_count"),
        avg("amount").alias("hist_avg_amount"),
        spark_max("step").alias("max_step"),
        spark_min("step").alias("min_step"),
    )
)

baseline = (
    agg
    .withColumn("step_span", (col("max_step") - col("min_step") + 1))
    .withColumn(
        "hist_avg_txn_per_12",
        col("txn_count") / (col("step_span") / 12.0)
    )
    .select("customerId", "hist_avg_txn_per_12", "hist_avg_amount")
)

baseline.write.mode("overwrite").parquet(OUTPUT_PATH)

print("=" * 60)
print("FINSIGHT CUSTOMER BASELINE — BATCH JOB COMPLETE")
print("=" * 60)
print("Customers with baseline:", baseline.count())
baseline.orderBy(col("customerId")).show(30, truncate=False)

spark.stop()
