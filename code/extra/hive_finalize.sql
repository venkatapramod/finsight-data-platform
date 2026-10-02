CREATE DATABASE IF NOT EXISTS finsight;

CREATE EXTERNAL TABLE IF NOT EXISTS finsight.transactions (
  step BIGINT,
  type STRING,
  amount DOUBLE,
  nameOrig STRING,
  oldbalanceOrg DOUBLE,
  newbalanceOrig DOUBLE,
  nameDest STRING,
  oldbalanceDest DOUBLE,
  newbalanceDest DOUBLE,
  isFraud BIGINT,
  isFlaggedFraud BIGINT
)
STORED AS PARQUET
LOCATION '/finsight/raw/transactions';

CREATE OR REPLACE VIEW finsight.vw_fraud_transactions AS
SELECT * FROM finsight.transactions WHERE isFraud = 1;

DROP TABLE IF EXISTS finsight.customer_clv;
CREATE EXTERNAL TABLE finsight.customer_clv (
  customerId STRING,
  total_amount DOUBLE,
  txn_count BIGINT,
  distinct_txn_types BIGINT,
  last_active_step BIGINT,
  inactive_steps BIGINT,
  transaction_volume_score DOUBLE,
  transaction_frequency_score DOUBLE,
  product_diversity_score DOUBLE,
  recency_score DOUBLE,
  clv_score DOUBLE,
  clv_tier STRING
)
STORED AS PARQUET
LOCATION '/finsight/processed/clv_scores';

ANALYZE TABLE finsight.transactions COMPUTE STATISTICS;
