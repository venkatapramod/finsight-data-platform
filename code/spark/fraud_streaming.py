from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_json, struct, lit, current_timestamp, coalesce
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    DoubleType,
    StringType
)
spark = (
    SparkSession.builder
    .appName("FinSight-Fraud-Streaming")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
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
    .option("kafka.bootstrap.servers", "localhost:9092")
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
# FinSight fraud rule from the project specification:
#
# (TRANSFER OR CASH_OUT)
# AND amount > 200,000
# AND newbalanceDest = 0
#
flagged = transactions.filter(
    (
        col("type").isin("TRANSFER", "CASH_OUT")
    )
    & (col("amount") > 200000)
    & (col("newbalanceDest") == 0)
)
# Add a reason for the alert
flagged_output = flagged.select(
    col("step"),
    col("type"),
    col("amount"),
    col("nameOrig"),
    col("nameDest"),
    col("newbalanceDest"),
    col("isFraud"),
    col("isFlaggedFraud")
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
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "txn-flagged")
    .option("checkpointLocation", "/finsight/checkpoints/fraud")
    .outputMode("append")
    .start()
)

# ------------------------------------------------------------
# R2 — Running fraud rate metric per micro-batch.
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

    flagged_count = batch_df.filter(
        (col("type").isin("TRANSFER", "CASH_OUT"))
        & (col("amount") > 200000)
        & (col("newbalanceDest") == 0)
    ).count()

    fraud_rate = (flagged_count / total_count) * 100.0

    print(
        f"[Batch {batch_id}] total={total_count} flagged={flagged_count} "
        f"fraud_rate={fraud_rate:.4f}%"
    )

    metrics_row = spark.createDataFrame(
        [(int(batch_id), total_count, flagged_count, float(fraud_rate))],
        ["batch_id", "total_count", "flagged_count", "fraud_rate_pct"]
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
