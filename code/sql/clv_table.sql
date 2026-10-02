USE finsight;

DROP TABLE IF EXISTS finsight.customer_clv;

CREATE TABLE finsight.customer_clv
USING PARQUET
LOCATION '/finsight/processed/clv_scores';

SELECT COUNT(*) AS clv_row_count FROM finsight.customer_clv;

SELECT clv_tier, COUNT(*) AS count
FROM finsight.customer_clv
GROUP BY clv_tier
ORDER BY count DESC;

SELECT customerId, clv_score, clv_tier
FROM finsight.customer_clv
ORDER BY clv_score DESC
LIMIT 10;

SHOW TABLES IN finsight;
