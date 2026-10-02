from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    sum as spark_sum,
    avg,
    max as spark_max,
    min as spark_min,
    array,
    array_compact,
    lit,
    when,
    expr,
    current_timestamp,
    broadcast,
    coalesce
)
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    DoubleType,
    StringType
)

# ============================================================
# FinSight - Real-Time Customer Churn Detection
# Section 7.2  (fixed version)
#
# Fixes vs. previous version:
#   1. signal_low_frequency and signal_low_amount were dead code
#      (compared txn_count < 1 and avg_amount < 0, which can never
#      be true). Both now compare against a per-customer historical
#      baseline computed by compute_customer_baseline.py, matching
#      the spec's "relative to their own historical average" wording.
#   2. signal_low_balance now requires >=2 transactions in the window
#      at/below $500, instead of just the single lowest balance seen
#      (closer to the spec's "two or more" wording; true consecutive-
#      order tracking is a documented simplification — see README note
#      at the bottom of this file).
#   3. Null-filtering added to the human-readable "signals" array via
#      array_compact so untriggered signals don't show up as null.
#
# IMPORTANT: because the aggregation schema changed, you MUST clear
# the old checkpoints before running this version, or Spark will
# refuse to start with a state-schema mismatch error:
#
#   hdfs dfs -rm -r -f /finsight/checkpoints/churn-kafka
#   hdfs dfs -rm -r -f /finsight/checkpoints/churn-hdfs
#   hdfs dfs -rm -r -f /finsight/processed/churn_alerts
#
# PREREQUISITE: run compute_customer_baseline.py at least once before
# starting this job, so /finsight/reference/customer_baseline exists.
# ============================================================

spark = (
    SparkSession.builder
    .appName("FinSight-Customer-Churn-Streaming")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ------------------------------------------------------------
# Load static historical baseline (stream-static join source)
# ------------------------------------------------------------

baseline_df = spark.read.parquet("/finsight/reference/customer_baseline")

# ------------------------------------------------------------
# Kafka transaction schema
# ------------------------------------------------------------

schema = StructType([
    StructField("step", LongType(), True),
    StructField("type", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("nameOrig", StringType(), True),
    StructField("oldbalanceOrg", DoubleType(), True),
    StructField("newbalanceOrig", DoubleType(), True),
    StructField("nameDest", StringType(), True),
    StructField("oldbalanceDest", DoubleType(), True),
    StructField("newbalanceDest", DoubleType(), True),
    StructField("isFraud", LongType(), True),
    StructField("isFlaggedFraud", LongType(), True)
])

# ------------------------------------------------------------
# Read txn-raw from Kafka
# ------------------------------------------------------------

raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "txn-raw")
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

# producer.py publishes a Kafka Connect envelope {"schema": ..., "payload": {...}}
# (required by the HDFS Sink's Parquet converter), while hand-typed test records
# from kafka-console-producer are flat JSON. Parse both and use whichever exists.
TXN_FIELDS = [
    ("step", "BIGINT"), ("type", "STRING"), ("amount", "DOUBLE"),
    ("nameOrig", "STRING"), ("oldbalanceOrg", "DOUBLE"),
    ("newbalanceOrig", "DOUBLE"), ("nameDest", "STRING"),
    ("oldbalanceDest", "DOUBLE"), ("newbalanceDest", "DOUBLE"),
    ("isFraud", "BIGINT"), ("isFlaggedFraud", "BIGINT"),
]
flat_ddl = ", ".join(f"{n} {t}" for n, t in TXN_FIELDS)
payload_ddl = ", ".join(f"{n}: {t}" for n, t in TXN_FIELDS)
envelope_ddl = f"{flat_ddl}, payload STRUCT<{payload_ddl}>"

transactions = (
    raw_stream
    .selectExpr("CAST(value AS STRING) AS json_value")
    .select(expr(f"from_json(json_value, '{envelope_ddl}')").alias("m"))
    .select(*[
        coalesce(col(f"m.payload.{n}"), col(f"m.{n}")).alias(n)
        for n, _ in TXN_FIELDS
    ])
    .withColumnRenamed("nameOrig", "customerId")
)

# ------------------------------------------------------------
# Convert step to event time.
#
# Dataset steps represent hourly activity.
# We create a timestamp based on the step so that Spark's
# Structured Streaming windowing can be demonstrated.
# ------------------------------------------------------------

transactions = transactions.withColumn(
    "event_time",
    expr("timestampadd(HOUR, step, timestamp('2026-01-01 00:00:00'))")
)

# ------------------------------------------------------------
# Customer behavioural aggregation
#
# 24-step / 24-hour rolling window
# ------------------------------------------------------------

customer_window = (
    transactions
    .withWatermark("event_time", "2 hours")
    .groupBy(
        "customerId",
        expr("window(event_time, '24 hours', '1 hour')")
    )
    .agg(
        count("*").alias("txn_count"),
        avg("amount").alias("avg_amount"),
        spark_sum("amount").alias("total_amount"),
        spark_max("step").alias("last_step"),
        spark_min("step").alias("first_step"),
        spark_sum(
            when(col("type") == "CASH_OUT", 1).otherwise(0)
        ).alias("cash_out_count"),
        spark_sum(
            when(col("type") == "PAYMENT", 1).otherwise(0)
        ).alias("payment_count"),
        spark_sum(
            when(col("type") == "DEBIT", 1).otherwise(0)
        ).alias("debit_count"),
        spark_min("newbalanceOrig").alias("min_balance"),
        spark_sum(
            when(col("newbalanceOrig") <= 500, 1).otherwise(0)
        ).alias("low_balance_txn_count")
    )
)

# ------------------------------------------------------------
# Join with historical baseline (static, broadcast)
# ------------------------------------------------------------

customer_window = customer_window.join(
    broadcast(baseline_df),
    on="customerId",
    how="left"
)

# ------------------------------------------------------------
# Churn signals
#
# The specification requires any TWO or more signals.
# ------------------------------------------------------------

signals = (
    customer_window
    .withColumn(
        # spec: freq drops below 1/12 steps, for a customer whose
        # historical average is above 3/12 steps
        "window_txn_rate_per_12",
        col("txn_count") / (24.0 / 12.0)
    )
    .withColumn(
        "signal_low_frequency",
        when(
            (col("window_txn_rate_per_12") < 1) &
            (col("hist_avg_txn_per_12") > 3),
            lit(1)
        ).otherwise(lit(0))
    )
    .withColumn(
        # spec: window avg amount falls below 20% of all-time avg
        "signal_low_amount",
        when(
            col("avg_amount") < (0.2 * col("hist_avg_amount")),
            lit(1)
        ).otherwise(lit(0))
    )
    .withColumn(
        "signal_cashout_only",
        when(
            (col("cash_out_count") > 0) &
            (col("payment_count") == 0) &
            (col("debit_count") == 0),
            lit(1)
        ).otherwise(lit(0))
    )
    .withColumn(
        # simplification: >=2 low-balance transactions in the window,
        # rather than strictly consecutive (see note at bottom of file)
        "signal_low_balance",
        when(col("low_balance_txn_count") >= 2, lit(1)).otherwise(lit(0))
    )
)

# ------------------------------------------------------------
# Count triggered signals
# ------------------------------------------------------------

signals = signals.withColumn(
    "signal_count",
    col("signal_low_frequency")
    + col("signal_low_amount")
    + col("signal_cashout_only")
    + col("signal_low_balance")
)

# ------------------------------------------------------------
# Build human-readable signal list (nulls removed)
# ------------------------------------------------------------

signals = signals.withColumn(
    "signals",
    array_compact(
        array(
            when(col("signal_low_frequency") == 1, lit("low_transaction_frequency")),
            when(col("signal_low_amount") == 1, lit("transaction_amount_decline")),
            when(col("signal_cashout_only") == 1, lit("cash_out_only")),
            when(col("signal_low_balance") == 1, lit("low_balance"))
        )
    )
)

churn_alerts = (
    signals
    .filter(col("signal_count") >= 2)
    .select(
        col("customerId"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("signals"),
        current_timestamp().alias("alert_timestamp")
    )
)

# ------------------------------------------------------------
# Write churn alerts to Kafka
# ------------------------------------------------------------

kafka_output = (
    churn_alerts
    .selectExpr(
        "CAST(customerId AS STRING) AS key",
        """to_json(named_struct(
            'customerId', customerId,
            'window_start', window_start,
            'window_end', window_end,
            'signals', signals,
            'alert_timestamp', alert_timestamp
        )) AS value"""
    )
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "txn-churn")
    .option("checkpointLocation", "/finsight/checkpoints/churn-kafka")
    .outputMode("update")
    .start()
)

# ------------------------------------------------------------
# Persist churn alerts to HDFS (Parquet)
#
# NOTE: file sinks only support append mode, and append mode for a
# watermarked aggregation only emits a window once the watermark has
# passed the window's end. In practice this means: after sending your
# test churn transactions, you must also send a LATER event (any
# customer) with a step far enough ahead to push the watermark past
# the window end + 2h, or this sink will look empty even though the
# alert already fired on Kafka.
# ------------------------------------------------------------

hdfs_output = (
    churn_alerts
    .writeStream
    .format("parquet")
    .option("path", "/finsight/processed/churn_alerts")
    .option("checkpointLocation", "/finsight/checkpoints/churn-hdfs")
    .outputMode("append")
    .start()
)

print("=" * 60)
print("FINSIGHT CUSTOMER CHURN STREAMING (fixed)")
print("=" * 60)
print("Kafka input : txn-raw")
print("Kafka output: txn-churn")
print("HDFS output : /finsight/processed/churn_alerts")
print("Baseline    : /finsight/reference/customer_baseline")
print("Window      : 24 hours, slide 1 hour, watermark 2 hours")
print("Threshold   : 2+ churn signals")
print("=" * 60)

spark.streams.awaitAnyTermination()

# ------------------------------------------------------------
# README NOTE — signal_low_balance simplification
#
# The spec's literal wording is: "newbalanceOrig reaches zero or
# drops below $500 for two or more CONSECUTIVE transactions." True
# consecutive-order enforcement needs per-customer sequence state
# (e.g. a session window with an ordered scan, or a custom
# flatMapGroupsWithState), which is disproportionate complexity for
# a 5-day solo build. This implementation instead requires >=2
# transactions at/below $500 anywhere within the 24h window, which
# is a defensible, easily-explained approximation of the same intent
# (sustained low balance rather than a single momentary dip). If
# asked in review/demo, describe it exactly this way rather than
# claiming strict consecutiveness.
# ------------------------------------------------------------
