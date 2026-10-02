from pyspark.sql import SparkSession
import json

spark = SparkSession.builder.appName("CheckOverlap").getOrCreate()

with open('/home/pramo/finsight/data/customers_export.json') as f:
    customers = json.load(f)
customer_ids = set(c['customerId'] for c in customers)
print(f"Total customer profiles (customers_export.json): {len(customer_ids)}")

txn = spark.read.parquet("/finsight/raw/transactions/")
orig_ids = set(row['nameOrig'] for row in txn.select("nameOrig").distinct().collect())
dest_ids = set(row['nameDest'] for row in txn.select("nameDest").distinct().collect())
all_txn_ids = orig_ids.union(dest_ids)
print(f"Distinct nameOrig: {len(orig_ids)}")
print(f"Distinct nameDest: {len(dest_ids)}")
print(f"Distinct total account IDs in transactions: {len(all_txn_ids)}")

overlap = customer_ids.intersection(all_txn_ids)
print(f"Customers matching nameOrig or nameDest: {len(overlap)}")
print(f"Sample overlap IDs: {list(overlap)[:10]}")
print(f"Sample customer IDs: {list(customer_ids)[:5]}")
print(f"Sample nameOrig IDs: {list(orig_ids)[:5]}")
