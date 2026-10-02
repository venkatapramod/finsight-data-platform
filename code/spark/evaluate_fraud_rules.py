"""
Evaluate FinSight fraud rules against the PaySim ground-truth labels.

Scores each candidate rule on a time-based split:
    tune  = steps 1-100    (used to choose a rule)
    test  = steps 101-154  (held out; report this one)

Metrics per rule:
    flagged     transactions the rule flags
    TP / FP     flagged and fraud / flagged and legitimate
    precision   TP / flagged
    recall      TP / all fraud
    FDR         FP / flagged            (share of alerts that are false)
    FPR         FP / all legitimate     (true false-positive rate)

Usage (HDFS running):
    spark-submit code/spark/evaluate_fraud_rules.py 2>/dev/null

Input path can be overridden with FINSIGHT_TRANSACTIONS_PATH
(a Parquet directory, local or HDFS).
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

INPUT = os.environ.get(
    "FINSIGHT_TRANSACTIONS_PATH",
    "hdfs://localhost:9000/finsight/raw/transactions",
)
TUNE_MAX_STEP = 100

# Spark 3.5 needs an active session before building column expressions.
spark = SparkSession.builder.appName("finsight-evaluate-rules").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

c = F.col
risky_type = c("type").isin("TRANSFER", "CASH_OUT")
sender_drained = (c("oldbalanceOrg") > 0) & (F.abs(c("amount") - c("oldbalanceOrg")) < 0.01)
sender_zero_after = c("newbalanceOrig") == 0
dest_untouched = (c("oldbalanceDest") == 0) & (c("newbalanceDest") == 0)

RULES = {
    "R0 current: risky type & amount>200K & newbalanceDest=0":
        risky_type & (c("amount") > 200000) & (c("newbalanceDest") == 0),
    "R1 risky type & sender drained (amount = old balance)":
        risky_type & sender_drained,
    "R2 R1 & sender balance 0 after":
        risky_type & sender_drained & sender_zero_after,
    "R3 R1 & dest balances both 0":
        risky_type & sender_drained & dest_untouched,
    "R4 TRANSFER only & sender drained":
        (c("type") == "TRANSFER") & sender_drained,
    "R5 R1 OR R0":
        (risky_type & sender_drained)
        | (risky_type & (c("amount") > 200000) & (c("newbalanceDest") == 0)),
}

df = (
    spark.read.parquet(INPUT)
    .withColumn("split", F.when(c("step") <= TUNE_MAX_STEP, "tune").otherwise("test"))
    .cache()
)

aggs = [
    F.count("*").alias("rows"),
    F.sum(c("isFraud")).alias("fraud"),
]
for i, (name, cond) in enumerate(RULES.items()):
    flag = cond.cast("int")
    aggs += [
        F.sum(flag).alias(f"flag_{i}"),
        F.sum(flag * c("isFraud")).alias(f"tp_{i}"),
    ]

results = {r["split"]: r for r in df.groupBy("split").agg(*aggs).collect()}


def pct(n, d):
    return f"{100.0 * n / d:6.2f}%" if d else "   n/a"


for split in ("tune", "test"):
    r = results[split]
    rows, fraud = r["rows"], r["fraud"]
    legit = rows - fraud
    print("=" * 112)
    print(f"{split.upper()} split  rows={rows:,}  fraud={fraud:,}  legitimate={legit:,}")
    print(f"{'rule':58s} {'flagged':>8s} {'TP':>6s} {'FP':>7s} "
          f"{'precision':>9s} {'recall':>8s} {'FDR':>8s} {'FPR':>8s}")
    for i, name in enumerate(RULES):
        flagged = r[f"flag_{i}"] or 0
        tp = r[f"tp_{i}"] or 0
        fp = flagged - tp
        print(f"{name[:58]:58s} {flagged:8,d} {tp:6,d} {fp:7,d} "
              f"{pct(tp, flagged):>9s} {pct(tp, fraud):>8s} "
              f"{pct(fp, flagged):>8s} {pct(fp, legit):>8s}")
print("=" * 112)

spark.stop()
