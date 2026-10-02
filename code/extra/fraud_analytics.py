from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    sum,
    round,
    when,
    desc
)

spark = (
    SparkSession.builder
    .appName("FinSight-Fraud-Analytics")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ============================================================
# READ TRANSACTIONS FROM HDFS
# ============================================================

transactions = (
    spark.read
    .json("/topics/txn-raw/partition=*/*.json")
)

print("\n========================================")
print("FINSIGHT FRAUD ANALYTICS")
print("========================================")

print("\nSchema:")
transactions.printSchema()

total_transactions = transactions.count()

print("\nTotal transactions:", total_transactions)

# ============================================================
# 1. OVERALL FRAUD SUMMARY
# ============================================================

print("\n----------------------------------------")
print("OVERALL FRAUD SUMMARY")
print("----------------------------------------")

fraud_summary = transactions.select(
    count("*").alias("total_transactions"),
    round(sum("amount"), 2).alias("total_amount"),
    sum(
        when(col("isFraud") == 1, 1).otherwise(0)
    ).alias("fraud_transactions"),
    round(
        sum(
            when(col("isFraud") == 1, col("amount"))
            .otherwise(0)
        ),
        2
    ).alias("fraud_amount")
)

fraud_summary.show(truncate=False)

# Save to HDFS
fraud_summary.write.mode("overwrite").json(
    "/finsight/analytics/fraud_summary"
)

# ============================================================
# 2. FRAUD BY TRANSACTION TYPE
# ============================================================

print("\n----------------------------------------")
print("FRAUD BY TRANSACTION TYPE")
print("----------------------------------------")

fraud_by_type = (
    transactions
    .groupBy("type")
    .agg(
        count("*").alias("transaction_count"),
        round(sum("amount"), 2).alias("total_amount"),
        sum(
            when(col("isFraud") == 1, 1).otherwise(0)
        ).alias("fraud_count"),
        round(
            sum(
                when(col("isFraud") == 1, col("amount"))
                .otherwise(0)
            ),
            2
        ).alias("fraud_amount")
    )
    .orderBy(desc("fraud_count"))
)

fraud_by_type.show(truncate=False)

# Save to HDFS
fraud_by_type.write.mode("overwrite").json(
    "/finsight/analytics/fraud_by_type"
)

# ============================================================
# 3. HIGH-VALUE SUSPICIOUS TRANSACTIONS
# ============================================================

print("\n----------------------------------------")
print("HIGH-VALUE SUSPICIOUS TRANSACTIONS")
print("----------------------------------------")

high_value_suspicious = (
    transactions
    .filter(
        (col("type").isin("TRANSFER", "CASH_OUT"))
        & (col("amount") > 200000)
        & (col("newbalanceDest") == 0)
    )
    .select(
        "step",
        "type",
        "amount",
        "nameOrig",
        "nameDest",
        "isFraud"
    )
    .orderBy(desc("amount"))
)

high_value_suspicious.show(20, truncate=False)

# Save to HDFS
high_value_suspicious.write.mode("overwrite").json(
    "/finsight/analytics/high_value_suspicious"
)

# ============================================================
# 4. SAVE COMPLETE TRANSACTION DATASET
# ============================================================

transactions.write.mode("overwrite").parquet(
    "/finsight/analytics/transactions"
)

print("\n========================================")
print("ANALYTICS OUTPUT SAVED TO HDFS")
print("========================================")
print("/finsight/analytics/fraud_summary")
print("/finsight/analytics/fraud_by_type")
print("/finsight/analytics/high_value_suspicious")
print("/finsight/analytics/transactions")
print("========================================\n")

spark.stop()
