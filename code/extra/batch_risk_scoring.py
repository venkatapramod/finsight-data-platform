import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as _sum, when, lit, current_timestamp

def main():
    spark = SparkSession.builder \
        .appName("FinSight-Batch-Risk-Scoring") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    hdfs_input_path = "/finsight/raw/transactions"
    hdfs_output_path = "/finsight/processed/risk_scores"

    print(f"Reading transaction data from {hdfs_input_path}...")
    df = spark.read.json(hdfs_input_path)

    # Determine maximum step in dataset
    max_step_row = df.selectExpr("max(step) as max_step").collect()
    max_step = max_step_row[0]["max_step"]

    if max_step is None:
        print("No transactions found. Exiting.")
        sys.exit(1)

    print(f"Max step in dataset: {max_step}")

    # 7-day rolling window = 168 steps/hours back from max_step
    min_step = max(1, max_step - 168)
    df_7d = df.filter((col("step") >= min_step) & (col("step") <= max_step))

    # Calculate Aggregates per Customer (using step-based schema fields)
    risk_df = df_7d.groupBy("nameOrig").agg(
        _sum("amount").alias("total_7d_amount"),
        count("step").alias("total_7d_count"),
        _sum(when(col("type") == "CASH_OUT", col("amount")).otherwise(0)).alias("cash_out_amount"),
        _sum(when(col("isFraud") == 1, 1).otherwise(0)).alias("flagged_fraud_count")
    ).withColumnRenamed("nameOrig", "customerId")

    # Compute Risk Score
    risk_scored_df = risk_df.withColumn(
        "cash_out_ratio", 
        when(col("total_7d_amount") > 0, col("cash_out_amount") / col("total_7d_amount")).otherwise(0)
    ).withColumn(
        "risk_score",
        (col("cash_out_ratio") * 40) + 
        (when(col("total_7d_count") > 15, 30).otherwise(col("total_7d_count") * 2)) +
        (when(col("flagged_fraud_count") > 0, 30).otherwise(0))
    ).withColumn(
        "risk_tier",
        when(col("risk_score") >= 75, "High")
        .when(col("risk_score") >= 45, "Medium")
        .otherwise("Low")
    ).withColumn("batch_processed_at", current_timestamp())

    # Write output to HDFS in Parquet format
    print(f"Writing risk scores to {hdfs_output_path}...")
    risk_scored_df.write \
        .mode("overwrite") \
        .parquet(hdfs_output_path)

    print("Batch Risk Scoring completed successfully!")
    spark.stop()

if __name__ == "__main__":
    main()
