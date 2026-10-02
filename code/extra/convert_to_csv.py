from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, col

spark = SparkSession.builder.appName("ConvertToCSV").getOrCreate()

df2 = spark.read.parquet("/finsight/processed/churn_alerts/")
df2 = df2.withColumn("signals", concat_ws(", ", col("signals")))
df2.coalesce(1).write.mode("overwrite").option("header", "true").csv("/finsight/exports/churn_alerts_csv")

spark.stop()
