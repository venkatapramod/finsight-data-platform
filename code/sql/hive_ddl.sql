USE finsight;

DROP TABLE IF EXISTS finsight.transactions;

CREATE EXTERNAL TABLE IF NOT EXISTS finsight.transactions (
    type STRING, amount DOUBLE, nameOrig STRING,
    oldbalanceOrg DOUBLE, newbalanceOrig DOUBLE, nameDest STRING,
    oldbalanceDest DOUBLE, newbalanceDest DOUBLE,
    isFraud INT, isFlaggedFraud INT
)
PARTITIONED BY (step INT)
STORED AS PARQUET
LOCATION '/finsight/raw/transactions';

MSCK REPAIR TABLE finsight.transactions;

SELECT COUNT(*) FROM finsight.transactions;

ANALYZE TABLE finsight.transactions COMPUTE STATISTICS;

SHOW CREATE TABLE finsight.transactions;
