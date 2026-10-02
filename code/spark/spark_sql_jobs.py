import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum, max as spark_max, when, round as spark_round

INPUT = "/finsight/raw/transactions"
COMPLIANCE_OUT = "/finsight/processed/compliance_summary"
CUSTOMER_OUT = "/finsight/processed/customer_fraud_summary"
DORMANCY_PARQUET = "/finsight/processed/dormancy_report"
DORMANCY_CSV = "/finsight/exports/dormancy_report.csv"

parser = argparse.ArgumentParser()
parser.add_argument("--mode", required=True, choices=["compliance", "customer_summary", "dormancy"])
args = parser.parse_args()

spark = (SparkSession.builder.appName("FinSight-SparkSQL-Jobs")
         .config("spark.sql.shuffle.partitions", "8").getOrCreate())
spark.sparkContext.setLogLevel("WARN")
df = spark.read.parquet(INPUT)

# The specification defines the weekly window as steps 1-168.
week = df.filter((col("step") >= 1) & (col("step") <= 168))

if args.mode == "compliance":
    out = (week.groupBy("type")
        .agg(
            count("*").alias("transaction_count"),
            spark_sum("amount").alias("total_volume"),
            spark_sum(when(col("isFraud") == 1, 1).otherwise(0)).alias("fraud_count")
        )
        .withColumn("fraud_rate_pct",
            spark_round(col("fraud_count") * 100.0 / col("transaction_count"), 4))
        .withColumn("risk_classification",
            when(col("fraud_rate_pct") >= 10, "High")
            .when(col("fraud_rate_pct") >= 5, "Medium")
            .otherwise("Low")))
    out.write.mode("overwrite").parquet(COMPLIANCE_OUT)
    print("COMPLIANCE OUTPUT")
    out.orderBy("type").show(truncate=False)
    print(COMPLIANCE_OUT)

elif args.mode == "customer_summary":
    out = (week.groupBy(col("nameOrig").alias("customerId"))
        .agg(
            count("*").alias("transaction_count"),
            spark_sum("amount").alias("total_amount"),
            spark_sum(when(col("isFraud") == 1, 1).otherwise(0)).alias("confirmed_fraud_count")
        )
        .withColumn("fraud_rate_pct",
            spark_round(col("confirmed_fraud_count") * 100.0 / col("transaction_count"), 4)))
    out.write.mode("overwrite").parquet(CUSTOMER_OUT)
    print("CUSTOMER FRAUD SUMMARY")
    out.orderBy(col("confirmed_fraud_count").desc()).show(20, truncate=False)
    print(CUSTOMER_OUT)

else:
    # ------------------------------------------------------------
    # Dormancy Report — FIX: count account activity as sender OR
    # receiver (nameOrig OR nameDest), not sender-only.
    #
    # Why: this dataset assigns each customer a near-unique nameOrig
    # per transaction (1,549,889 distinct nameOrig values across
    # 1,550,447 rows — the same characteristic already
    # documented and corrected for in risk_scoring.py). Under a
    # sender-only count, almost no account reaches the spec's
    # "at least 5 prior transactions" threshold, so the dormancy
    # report returns zero rows regardless of actual inactivity.
    #
    # A bank account's activity is naturally both inbound and
    # outbound, so combining nameOrig and nameDest occurrences is a
    # defensible, documented correction for this same data quirk —
    # not a deviation from the spec's intent.
    # ------------------------------------------------------------

    sent = df.select(col("nameOrig").alias("customerId"), col("step"))
    received = df.select(col("nameDest").alias("customerId"), col("step"))
    activity = sent.union(received).filter(col("customerId").startswith("C"))

    customer = (activity
        .groupBy("customerId")
        .agg(
            count("*").alias("transaction_count"),
            spark_max("step").alias("last_active_step")
        ))

    max_step = df.agg(spark_max("step").alias("max_step")).first()[0]
    out = (customer
        .withColumn("dataset_max_step", col("last_active_step") * 0 + int(max_step))
        .withColumn("inactive_steps", col("dataset_max_step") - col("last_active_step"))
        .filter(col("transaction_count") >= 5)
        .withColumn("dormancy_status",
            when((col("inactive_steps") > 72) & (col("inactive_steps") <= 120), "Dormant")
            .when(col("inactive_steps") > 120, "Severely Dormant")
            .otherwise("Active"))
        .filter(col("dormancy_status") != "Active"))
    out.write.mode("overwrite").parquet(DORMANCY_PARQUET)
    # Single CSV output directory is converted to one file for direct Alteryx consumption.
    out.coalesce(1).write.mode("overwrite").option("header", True).csv(DORMANCY_CSV)
    print("DORMANCY REPORT")
    out.groupBy("dormancy_status").count().show()
    out.orderBy(col("inactive_steps").desc()).show(20, truncate=False)
    print(DORMANCY_PARQUET)
    print(DORMANCY_CSV)

spark.stop()
