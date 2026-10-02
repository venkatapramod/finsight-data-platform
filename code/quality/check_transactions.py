"""
Data-quality checks for the FinSight transactions dataset.

Runs a set of checks on a Parquet or CSV source and exits non-zero if any
check fails, so it can gate a pipeline run.

Usage:
    spark-submit code/quality/check_transactions.py               # HDFS Parquet
    spark-submit code/quality/check_transactions.py data/Transactions.csv
    FINSIGHT_EXPECTED_ROWS=1550448 spark-submit code/quality/check_transactions.py
"""

import os
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

DEFAULT_SOURCE = "hdfs://localhost:9000/finsight/raw/transactions"

REQUIRED_COLUMNS = {
    "step", "type", "amount", "nameOrig", "oldbalanceOrg", "newbalanceOrig",
    "nameDest", "oldbalanceDest", "newbalanceDest", "isFraud", "isFlaggedFraud",
}
KEY_COLUMNS = ["step", "type", "amount", "nameOrig", "nameDest", "isFraud"]
VALID_TYPES = {"CASH_IN", "CASH_OUT", "DEBIT", "PAYMENT", "TRANSFER"}
BALANCE_COLUMNS = ["oldbalanceOrg", "newbalanceOrig", "oldbalanceDest", "newbalanceDest"]


def run_checks(df: DataFrame, expected_rows=None):
    """Return a list of (check name, passed, detail)."""
    results = []

    missing = REQUIRED_COLUMNS - set(df.columns)
    results.append(("schema: required columns present", not missing,
                    f"missing={sorted(missing)}" if missing else "all 11 present"))
    if missing:
        return results

    total = df.count()
    results.append(("row count > 0", total > 0, f"{total:,} rows"))
    if expected_rows is not None:
        results.append(("row count matches expected", total == expected_rows,
                        f"{total:,} vs expected {expected_rows:,}"))

    nulls = df.select([
        F.sum(F.col(c).isNull().cast("int")).alias(c) for c in KEY_COLUMNS
    ]).first().asDict()
    bad = {k: v for k, v in nulls.items() if v}
    results.append(("no nulls in key columns", not bad,
                    f"nulls={bad}" if bad else "none"))

    types = {r["type"] for r in df.select("type").distinct().collect()}
    unknown = types - VALID_TYPES
    results.append(("transaction types valid", not unknown,
                    f"unknown={sorted(unknown, key=str)}" if unknown
                    else ", ".join(sorted(types))))

    neg_amount = df.filter(F.col("amount") < 0).count()
    results.append(("amount >= 0", neg_amount == 0, f"{neg_amount:,} negative"))

    neg_bal = df.filter(" OR ".join(f"{c} < 0" for c in BALANCE_COLUMNS)).count()
    results.append(("balances >= 0", neg_bal == 0,
                    f"{neg_bal:,} rows with a negative balance"))

    bad_label = df.filter(~F.col("isFraud").isin(0, 1)).count()
    results.append(("isFraud is 0 or 1", bad_label == 0, f"{bad_label:,} invalid"))

    bad_step = df.filter(F.col("step") < 1).count()
    results.append(("step >= 1", bad_step == 0, f"{bad_step:,} invalid"))

    fraud = df.filter(F.col("isFraud") == 1).count()
    rate = 100.0 * fraud / total if total else 0.0
    results.append(("fraud rate plausible (0-5%)", 0 < rate < 5,
                    f"{fraud:,} fraud = {rate:.3f}%"))

    return results


def load(spark, source):
    if source.lower().endswith(".csv"):
        return spark.read.option("header", True).option("inferSchema", True).csv(source)
    return spark.read.parquet(source)


def main():
    source = sys.argv[1] if len(sys.argv) > 1 else os.environ.get(
        "FINSIGHT_TRANSACTIONS_PATH", DEFAULT_SOURCE)
    expected = os.environ.get("FINSIGHT_EXPECTED_ROWS")
    expected = int(expected) if expected else None

    spark = SparkSession.builder.appName("finsight-data-quality").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    results = run_checks(load(spark, source), expected)
    spark.stop()

    print("=" * 90)
    print(f"Data-quality checks: {source}")
    print("-" * 90)
    for name, passed, detail in results:
        print(f"{'PASS' if passed else 'FAIL'}  {name:38s} {detail}")
    print("=" * 90)

    failed = [r for r in results if not r[1]]
    print(f"{len(results) - len(failed)}/{len(results)} checks passed")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
