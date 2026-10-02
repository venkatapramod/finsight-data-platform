#!/bin/bash
set -e

echo "=== FinSight Pipeline Orchestration ==="

echo "[Step 1/3] Verifying HDFS Directories..."
hdfs dfs -mkdir -p /finsight/raw /finsight/models

echo "[Step 2/3] Executing ML Model Training..."
spark-submit --master local[*] spark/train_fraud_model.py

echo "[Step 3/3] Verifying Saved Artifacts in HDFS..."
hdfs dfs -ls /finsight/models/fraud_rf_model

echo "=== FinSight Pipeline Executed Successfully! ==="
