from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ExportFraudView") \
    .enableHiveSupport() \
    .getOrCreate()

df = spark.sql("SELECT * FROM finsight.vw_fraud_transactions")
df.coalesce(1).write.mode("overwrite").option("header", "true").csv("/finsight/exports/flagged_transactions_csv")

spark.stop()
