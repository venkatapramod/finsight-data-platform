from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("FinSight-Export-Current-Transactions")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

input_path = "hdfs:///finsight/analytics/transactions"
output_path = "file:///home/pramo/finsight/powerbi/current_transactions_tmp"

df = spark.read.parquet(input_path)

print("CURRENT HDFS TRANSACTION COUNT:", df.count())

df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)

spark.stop()
