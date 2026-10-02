"""
FinSight — Re-land Transactions.csv to HDFS as Parquet, partitioned by step.
Fallback path per spec Section 11: direct file load to HDFS.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DoubleType
)

spark = SparkSession.builder.appName("FinSight-Load-Transactions").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
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
    StructField("isFlaggedFraud", IntegerType(), True),
])

print("Reading Transactions.csv...")
df = spark.read.csv(
    "file:///home/pramo/finsight/data/Transactions.csv",
    header=True,
    schema=schema,
)

count = df.count()
print(f"Rows read: {count}")

print("Writing to HDFS as Parquet, partitioned by step...")
df.write.mode("overwrite").partitionBy("step").parquet("/finsight/raw/transactions")

print("Done. Verifying...")
check = spark.read.parquet("/finsight/raw/transactions")
print(f"Rows in HDFS after write: {check.count()}")
check.printSchema()

spark.stop()
