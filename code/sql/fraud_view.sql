USE finsight;

CREATE OR REPLACE VIEW finsight.vw_fraud_transactions AS
SELECT * FROM finsight.transactions WHERE isFraud = 1;

SELECT COUNT(*) FROM finsight.vw_fraud_transactions;

SELECT * FROM finsight.vw_fraud_transactions LIMIT 5;
