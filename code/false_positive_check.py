from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("FalsePositiveCheck").getOrCreate()

txn = spark.read.parquet("/finsight/raw/transactions/")

# Apply the exact streaming fraud-flagging rule from spec Section 7.1
flagged_by_rule = txn.filter(
    (col("type").isin("TRANSFER", "CASH_OUT")) &
    (col("amount") > 200000) &
    (col("newbalanceDest") == 0)
)

total_flagged = flagged_by_rule.count()
true_positives = flagged_by_rule.filter(col("isFraud") == 1).count()
false_positives = flagged_by_rule.filter(col("isFraud") == 0).count()

false_positive_rate = (false_positives / total_flagged) * 100 if total_flagged > 0 else 0

print(f"Total transactions flagged by rule: {total_flagged}")
print(f"True positives (actually fraud): {true_positives}")
print(f"False positives (not actually fraud): {false_positives}")
print(f"False Positive Rate: {false_positive_rate:.2f}%")

spark.stop()
