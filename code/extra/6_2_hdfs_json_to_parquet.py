from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import *

RAW_JSON = "/topics/txn-raw"
OUT = "/finsight/raw/transactions"

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
    StructField("isFlaggedFraud", LongType(), True),
])

spark = (SparkSession.builder.appName("FinSight-6.2-HDFS-Parquet-Recovery")
         .config("spark.sql.shuffle.partitions", "8").getOrCreate())
spark.sparkContext.setLogLevel("WARN")

# The current connector landing contains JSON records. Read JSON and enforce the canonical schema.
df = spark.read.schema(schema).json(RAW_JSON).filter(col("step").isNotNull())

# Keep one canonical Parquet lake for all downstream Spark/Hive jobs.
df.write.mode("overwrite").partitionBy("step").parquet(OUT)

print("6.2/landing recovery complete")
print("Rows:", df.count())
print("Output:", OUT)
print("Min step:", df.agg({"step":"min"}).first()[0])
print("Max step:", df.agg({"step":"max"}).first()[0])

spark.stop()
