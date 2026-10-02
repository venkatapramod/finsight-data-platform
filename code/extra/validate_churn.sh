#!/usr/bin/env bash
set -euo pipefail

# Run the existing fixed churn_streaming.py after clearing its old state.
hdfs dfs -rm -r -f /finsight/checkpoints/churn-kafka >/dev/null 2>&1 || true
hdfs dfs -rm -r -f /finsight/checkpoints/churn-hdfs >/dev/null 2>&1 || true
hdfs dfs -rm -r -f /finsight/processed/churn_alerts >/dev/null 2>&1 || true

# Ensure txn-churn exists as a one-partition topic.
~/bigdata/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic txn-churn --partitions 1 --replication-factor 1 || true

echo 'Start the churn job in another terminal:'
echo '  spark-submit ~/finsight/spark/churn_streaming.py'
echo
echo 'Then replay/produce the churn test events used in the existing baseline test.'
echo 'IMPORTANT: after the test events, send one later event with a step at least 3 hours ahead of the test window so the 2-hour watermark closes the window.'
echo
echo 'Verify Kafka:'
echo '  ~/bigdata/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic txn-churn --from-beginning --max-messages 1'
echo
echo 'Verify HDFS:'
echo '  hdfs dfs -ls /finsight/processed/churn_alerts'
