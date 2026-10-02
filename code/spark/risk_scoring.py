"""
FinSight — Spark Core Batch Risk Scoring (Section 7.3)

Computes a rolling 7-day (168-step) composite risk score per customer
account from four factors: transaction frequency, average transfer
amount, CASH_OUT proportion, and count of unique destination accounts.
Also writes the R2 daily_summary output used by the Day 4 Alteryx
Transaction Summary workflow.

Design note — normalisation
In this PaySim file almost every nameOrig appears exactly once
(1,549,889 distinct customers across 1,550,447 rows). Normalising each
factor by the in-batch maximum therefore collapses nearly every customer
onto the same few scores, and with equal 25% weights the lowest reachable
score is 0.25, which makes the spec's Low tier (< 0.25) unreachable.
Each factor is instead normalised against a fixed reference scale:
    frequency   -> 5+ transactions in the window = 1.0
    amount      -> the spec's $200,000 large-transfer threshold (Section 7.1)
    cash_out    -> already a 0-1 proportion
    unique_dest -> 5+ distinct counterparties in the window = 1.0
Full-dataset result: High 209,448 / Low 692,897 / Medium 647,544.

Assumption: Section 7.3 names four weighted factors but gives no weights,
so equal 25% weights are used.

Window: the dataset's maximum step is 154, so the 168-step window is
clamped to start at step 1 (see window_start below).

Input : /finsight/raw/transactions/   (Parquet, partitioned by step)
Output: /finsight/processed/risk_scores/    (one row per customerId)
        /finsight/processed/daily_summary/  (type x step aggregates)
Schedule: cron, 22:00 daily (see crontab.txt)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, avg, sum as spark_sum, when, countDistinct,
    max as spark_max
)

INPUT_PATH = "/finsight/raw/transactions/"
INPUT_FORMAT = "parquet"

RISK_SCORES_OUTPUT = "/finsight/processed/risk_scores"
DAILY_SUMMARY_OUTPUT = "/finsight/processed/daily_summary"

WINDOW_STEPS = 168  # rolling 7-day window (spec: 1 step = 1 hour)

# Fixed reference scales — see design note above
FREQ_REF = 5.0
AMOUNT_REF = 200000.0
UNIQUE_DEST_REF = 5.0

# Equal weighting — spec doesn't specify exact weights (see note above)
WEIGHT_FREQUENCY = 0.25
WEIGHT_AMOUNT = 0.25
WEIGHT_CASH_OUT = 0.25
WEIGHT_UNIQUE_DEST = 0.25

spark = (
    SparkSession.builder
    .appName("FinSight-Spark-Core-Risk-Scoring")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

if INPUT_FORMAT == "json":
    df = spark.read.json(INPUT_PATH)
else:
    df = spark.read.parquet(INPUT_PATH)

max_step = df.agg(spark_max("step")).first()[0]
window_start = max(1, max_step - WINDOW_STEPS + 1)
print(f"Max step: {max_step} | Rolling window: steps {window_start}-{max_step}")

windowed = df.filter(col("step") >= window_start)

# ------------------------------------------------------------
# R2 — Daily summary (by transaction type and step), over the FULL
# history (not just the rolling window), required for the Alteryx
# Transaction Summary workflow on Day 4.
# ------------------------------------------------------------

daily_summary = (
    df.groupBy("type", "step")
    .agg(
        count("*").alias("txn_count"),
        spark_sum("amount").alias("total_amount"),
        spark_sum(when(col("isFraud") == 1, 1).otherwise(0)).alias("fraud_count")
    )
    .orderBy("step", "type")
)

daily_summary.write.mode("overwrite").parquet(DAILY_SUMMARY_OUTPUT)
print(f"daily_summary written: {daily_summary.count()} rows -> {DAILY_SUMMARY_OUTPUT}")

# ------------------------------------------------------------
# Per-customer raw factors (originating side — nameOrig)
# ------------------------------------------------------------

per_customer = (
    windowed
    .groupBy(col("nameOrig").alias("customerId"))
    .agg(
        count("*").alias("txn_frequency"),
        avg("amount").alias("avg_transfer_amount"),
        spark_sum(when(col("type") == "CASH_OUT", 1).otherwise(0)).alias("cash_out_count"),
        countDistinct("nameDest").alias("unique_destinations")
    )
    .withColumn("cash_out_proportion", col("cash_out_count") / col("txn_frequency"))
)

# ------------------------------------------------------------
# Normalise + composite score + tier (R1)
# ------------------------------------------------------------

def cap_at_one(colname, ref):
    return when(col(colname) / ref > 1, 1.0).otherwise(col(colname) / ref)

scored = (
    per_customer
    .withColumn("norm_frequency", cap_at_one("txn_frequency", FREQ_REF))
    .withColumn("norm_amount", cap_at_one("avg_transfer_amount", AMOUNT_REF))
    .withColumn("norm_cash_out", col("cash_out_proportion"))
    .withColumn("norm_unique_dest", cap_at_one("unique_destinations", UNIQUE_DEST_REF))
    .withColumn(
        "risk_score",
        (col("norm_frequency") * WEIGHT_FREQUENCY)
        + (col("norm_amount") * WEIGHT_AMOUNT)
        + (col("norm_cash_out") * WEIGHT_CASH_OUT)
        + (col("norm_unique_dest") * WEIGHT_UNIQUE_DEST)
    )
    .withColumn(
        "risk_tier",
        when(col("risk_score") < 0.25, "Low")
        .when(col("risk_score") <= 0.60, "Medium")
        .otherwise("High")
    )
    .select("customerId", "risk_score", "risk_tier")
)

scored.write.mode("overwrite").parquet(RISK_SCORES_OUTPUT)

print("=" * 60)
print("FINSIGHT SPARK CORE RISK SCORING — BATCH JOB COMPLETE")
print("=" * 60)
print(f"Customers scored: {scored.count()}")
scored.groupBy("risk_tier").count().orderBy("risk_tier").show()
scored.orderBy(col("risk_score").desc()).show(15, truncate=False)

spark.stop()
