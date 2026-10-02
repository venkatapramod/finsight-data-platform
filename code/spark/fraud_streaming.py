import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_json, struct, lit, current_timestamp, coalesce
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    DoubleType,
    StringType
)

# Make the shared rule module importable when run with spark-submit.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from fraud_rules import RULE_VERSION, fraud_condition, fraud_reason  # noqa: E402

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

spark = (
    SparkSession.builder
    .appName("FinSight-Fraud-Streaming")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print(f"Fraud rule in use: {RULE_VERSION}")

# Schema of transactions arriving from txn-raw
transaction_schema = StructType([
    StructField("step", IntegerType(), True),
    StructField("type", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("nameOrig", StringType(), True),
    StructField("oldbalanceOrg", DoubleType(), True),
    StructField("newbalanceOrig", DoubleType(), True),
    StructField("nameDest", StringType(), True),
    StructField("oldbalanceDest", DoubleType(), True),
    StructField("newbalanceDest", DoubleType(), True),
    StructField("isFraud", IntegerType(), True),
    StructField("isFlaggedFraud", IntegerType(), True)
])

# Read transaction events from Kafka
raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", "txn-raw")
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

# Convert Kafka value from binary -> JSON -> structured columns.
# producer.py publishes a Kafka Connect envelope {"schema": ..., "payload": {...}}
# (required by the HDFS Sink's Parquet converter). Hand-typed test messages from
# kafka-console-producer are flat JSON. Parse both shapes and take whichever is present.
envelope_schema = StructType(
    transaction_schema.fields + [StructField("payload", transaction_schema, True)]
)
transactions = (
    raw_stream
    .selectExpr("CAST(value AS STRING) AS json_value")
    .select(from_json(col("json_value"), envelope_schema).alias("m"))
    .select(*[
        coalesce(col(f"m.payload.{f.name}"), col(f"m.{f.name}")).alias(f.name)
        for f in transaction_schema.fields
    ])
)

# Fraud rule comes from fraud_rules.py (see that file for rule history
# and evaluation results).
flagged = transactions.filter(fraud_condition())

flagged_output = flagged.select(
    col("step"),
    col("type"),
    col("amount"),
    col("nameOrig"),
    col("oldbalanceOrg"),
    col("nameDest"),
    col("newbalanceDest"),
    col("isFraud"),
    col("isFlaggedFraud"),
    fraud_reason().alias("reason"),
    lit(RULE_VERSION).alias("rule_version")
)

# Convert the structured record back to JSON for Kafka
kafka_output = (
    flagged_output
    .select(
        to_json(struct("*")).alias("value")
    )
)

# Write flagged transactions to txn-flagged
# R1: checkpointLocation guarantees exactly-once semantics and enables
# automatic recovery if the job is interrupted mid-stream.
query = (
    kafka_output
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("topic", "txn-flagged")
    .option("checkpointLocation", "/finsight/checkpoints/fraud")
    .outputMode("append")
    .start()
)

# ------------------------------------------------------------
# R2: Running alert rate per micro-batch.
# Computed as (flagged count / total count * 100) and logged to
# /finsight/processed/streaming_metrics/ on HDFS for monitoring.
# ------------------------------------------------------------

METRICS_OUTPUT = "/finsight/processed/streaming_metrics"
METRICS_CHECKPOINT = "/finsight/checkpoints/fraud_metrics"


def log_batch_metrics(batch_df, batch_id):
    total_count = batch_df.count()
    if total_count == 0:
        print(f"[Batch {batch_id}] No records in this micro-batch.")
        return

    flagged_count = batch_df.filter(fraud_condition()).count()
    fraud_rate = (flagged_count / total_count) * 100.0

    print(
        f"[Batch {batch_id}] total={total_count} flagged={flagged_count} "
        f"alert_rate={fraud_rate:.4f}% rule={RULE_VERSION}"
    )

    metrics_row = spark.createDataFrame(
        [(int(batch_id), total_count, flagged_count, float(fraud_rate), RULE_VERSION)],
        ["batch_id", "total_count", "flagged_count", "fraud_rate_pct", "rule_version"]
    ).withColumn("logged_at", current_timestamp())

    metrics_row.write.mode("append").parquet(METRICS_OUTPUT)


metrics_query = (
    transactions
    .writeStream
    .foreachBatch(log_batch_metrics)
    .option("checkpointLocation", METRICS_CHECKPOINT)
    .start()
)

spark.streams.awaitAnyTermination()
