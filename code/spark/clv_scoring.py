from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct, lit, max as spark_max, sum as spark_sum, when, greatest
from pyspark.sql.types import DoubleType

INPUT = "/finsight/raw/transactions"
OUTPUT = "/finsight/processed/clv_scores"

spark = (SparkSession.builder
    .appName("FinSight-CLV-Scoring")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

# Independent application/session and independent output path, as required by 7.4.
df = spark.read.parquet(INPUT).select(
    col("step").cast("long"), col("type"), col("amount").cast("double"), col("nameOrig").alias("customerId")
).filter(col("customerId").isNotNull())

max_step = df.agg(spark_max("step").alias("max_step")).first()[0]
max_step = int(max_step or 0)

agg = (df.groupBy("customerId")
    .agg(
        spark_sum("amount").alias("total_amount"),
        count("*").alias("txn_count"),
        countDistinct("type").alias("distinct_txn_types"),
        spark_max("step").alias("last_active_step")
    ))

# Four required components.
# Percentile normalisation: rank each customer against the population rather
# than against the single largest account, so scores span the full 0-1 range.
from pyspark.sql.window import Window
from pyspark.sql.functions import percent_rank

w_volume = Window.orderBy(col("total_amount"))
w_frequency = Window.orderBy(col("txn_count"))

result = (agg
    .withColumn("transaction_volume_score", percent_rank().over(w_volume))
    .withColumn("transaction_frequency_score", percent_rank().over(w_frequency))
    .withColumn("product_diversity_score",
        (col("distinct_txn_types") / lit(5.0)).cast("double"))
    .withColumn("inactive_steps", lit(max_step) - col("last_active_step"))
    # Bounded inverse recency: 0 for >=48 inactive steps; otherwise more recent = higher.
    .withColumn("recency_score",
        when(col("inactive_steps") >= 48, lit(0.0))
        .otherwise((lit(1.0) / (lit(1.0) + greatest(col("inactive_steps"), lit(0)))).cast("double")))
    .withColumn("clv_score",
        col("transaction_volume_score") * lit(0.30) +
        col("transaction_frequency_score") * lit(0.25) +
        col("product_diversity_score") * lit(0.25) +
        col("recency_score") * lit(0.20))
    .withColumn("clv_tier",
        when(col("clv_score") > 0.70, lit("High Value"))
        .when(col("clv_score") >= 0.40, lit("Growth Potential"))
        .otherwise(lit("At Risk")))
    .select("customerId", "total_amount", "txn_count", "distinct_txn_types",
            "last_active_step", "inactive_steps", "transaction_volume_score",
            "transaction_frequency_score", "product_diversity_score", "recency_score",
            "clv_score", "clv_tier"))

result.write.mode("overwrite").parquet(OUTPUT)
print("CLV complete")
print("Customers:", result.count())
print("Output:", OUTPUT)
result.groupBy("clv_tier").count().orderBy("clv_tier").show(truncate=False)

spark.stop()
