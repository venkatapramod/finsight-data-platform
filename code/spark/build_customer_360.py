"""
Build the FinSight Customer 360 table.

The synthetic customer profiles (data/novacrest_customers.json) and the
PaySim transactions were generated independently, so their IDs never
match: only 2 of 10,000 profiles joined in the original run.

Banks model this with a customer -> account crosswalk, so this job builds one:

  1. Activity per account: transactions sent and received across the
     whole dataset (receiving accounts carry most of the history).
  2. Rank accounts by total activity (ties broken by account ID) and
     profiles by customerId; pair them rank-for-rank. Deterministic:
     the same inputs always give the same mapping.
  3. Join profile + account activity into customer_360.

Outputs (HDFS):
  /finsight/reference/customer_account_map   customerId <-> accountId
  /finsight/processed/customer_360           one row per customer
  /finsight/exports/customer_360_csv         CSV for Power BI / Alteryx

Usage:
    spark-submit code/spark/build_customer_360.py 2>/dev/null
"""

import os
import sys

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

TRANSACTIONS = os.environ.get(
    "FINSIGHT_TRANSACTIONS_PATH", "hdfs://localhost:9000/finsight/raw/transactions")
PROFILES = os.environ.get(
    "FINSIGHT_PROFILES_PATH", "data/novacrest_customers.json")
OUT_BASE = os.environ.get("FINSIGHT_OUTPUT_BASE", "hdfs://localhost:9000/finsight")


def local_uri(path: str) -> str:
    """Spark resolves scheme-less paths against the default FS (HDFS);
    make local files explicit with file://."""
    return path if "://" in path else "file://" + os.path.abspath(path)


def account_activity(txns: DataFrame) -> DataFrame:
    """One row per customer account (IDs starting with 'C'), sent + received."""
    sent = txns.select(
        F.col("nameOrig").alias("accountId"),
        F.lit(1).alias("sent"), F.lit(0).alias("received"),
        F.col("amount").alias("amount_sent"), F.lit(0.0).alias("amount_received"),
        F.col("isFraud"), F.col("step"),
    )
    received = txns.where(F.col("nameDest").startswith("C")).select(
        F.col("nameDest").alias("accountId"),
        F.lit(0).alias("sent"), F.lit(1).alias("received"),
        F.lit(0.0).alias("amount_sent"), F.col("amount").alias("amount_received"),
        F.col("isFraud"), F.col("step"),
    )
    return (
        sent.unionByName(received)
        .groupBy("accountId")
        .agg(
            F.sum("sent").alias("txn_sent"),
            F.sum("received").alias("txn_received"),
            F.round(F.sum("amount_sent"), 2).alias("amount_sent"),
            F.round(F.sum("amount_received"), 2).alias("amount_received"),
            F.sum("isFraud").alias("fraud_txn_count"),
            F.min("step").alias("first_step"),
            F.max("step").alias("last_step"),
        )
        .withColumn("txn_total", F.col("txn_sent") + F.col("txn_received"))
    )


def map_profiles_to_accounts(profiles: DataFrame, activity: DataFrame) -> DataFrame:
    """Pair profiles with the most active accounts, rank for rank (deterministic)."""
    order = [F.col("txn_total").desc(), F.col("accountId")]
    n_profiles = profiles.count()
    p = profiles.select("customerId").withColumn(
        "rank", F.row_number().over(Window.orderBy("customerId")))
    # Keep only the top accounts before ranking, so the window sorts
    # n_profiles rows instead of every account in the dataset.
    a = (activity.select("accountId", "txn_total").orderBy(*order).limit(n_profiles)
         .withColumn("rank", F.row_number().over(Window.orderBy(*order))))
    return p.join(a, "rank").select("customerId", "accountId")


def build_customer_360(profiles: DataFrame, activity: DataFrame) -> DataFrame:
    mapping = map_profiles_to_accounts(profiles, activity)
    return (
        profiles.join(mapping, "customerId", "left")
        .join(activity, "accountId", "left")
        .withColumn("fraud_involved", F.coalesce(F.col("fraud_txn_count"), F.lit(0)) > 0)
    )


def main():
    spark = SparkSession.builder.appName("finsight-customer-360").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    profiles = spark.read.option("multiLine", True).json(local_uri(PROFILES))
    txns = spark.read.parquet(TRANSACTIONS)

    activity = account_activity(txns).cache()
    c360 = build_customer_360(profiles, activity).cache()

    c360.select("customerId", "accountId").write.mode("overwrite").parquet(
        f"{OUT_BASE}/reference/customer_account_map")
    c360.write.mode("overwrite").parquet(f"{OUT_BASE}/processed/customer_360")

    # CSV cannot hold arrays: flatten products to "a|b|c".
    (c360.withColumn("products", F.concat_ws("|", "products"))
         .coalesce(1).write.mode("overwrite").option("header", True)
         .csv(f"{OUT_BASE}/exports/customer_360_csv"))

    s = c360.agg(
        F.count("*").alias("profiles"),
        F.count("accountId").alias("mapped"),
        F.sum((F.col("txn_total") >= 2).cast("int")).alias("multi_txn"),
        F.sum(F.col("fraud_involved").cast("int")).alias("fraud_customers"),
        F.min("txn_total").alias("min_txn"),
        F.max("txn_total").alias("max_txn"),
        F.round(F.avg("txn_total"), 1).alias("avg_txn"),
    ).first()

    print("=" * 70)
    print(f"Customer profiles             : {s['profiles']:,}")
    print(f"Mapped to transacting accounts: {s['mapped']:,} "
          f"({100.0 * s['mapped'] / s['profiles']:.1f}%)   [before: 2]")
    print(f"Customers with 2+ transactions: {s['multi_txn']:,}")
    print(f"Transactions per customer     : min {s['min_txn']}, "
          f"avg {s['avg_txn']}, max {s['max_txn']}")
    print(f"Customers involved in fraud   : {s['fraud_customers']:,}")
    print("=" * 70)

    spark.stop()
    sys.exit(0 if s["mapped"] == s["profiles"] else 1)


if __name__ == "__main__":
    main()
