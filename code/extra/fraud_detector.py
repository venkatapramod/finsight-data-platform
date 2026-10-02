import json
from kafka import KafkaConsumer, KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

INPUT_TOPIC = "txn-raw"
OUTPUT_TOPIC = "txn-flagged"

consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="finsight-fraud-detector",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda x: json.dumps(x).encode("utf-8")
)

print("Connected to Kafka:", KAFKA_BOOTSTRAP_SERVERS)
print("Reading from:", INPUT_TOPIC)
print("Publishing flagged transactions to:", OUTPUT_TOPIC)
print("--------------------------------")

for message in consumer:
    txn = message.value
    # producer.py wraps records in a Kafka Connect envelope; unwrap if present.
    if isinstance(txn, dict) and "payload" in txn:
        txn = txn["payload"]

    transaction_type = txn["type"]
    amount = float(txn["amount"])
    old_balance = float(txn["oldbalanceOrg"])
    new_balance = float(txn["newbalanceOrig"])

    fraud_reasons = []

    # Rule 1: suspicious TRANSFER/CASH_OUT that drains the
    # origin account completely
    if (
        transaction_type in ["TRANSFER", "CASH_OUT"]
        and old_balance == amount
        and new_balance == 0
    ):
        fraud_reasons.append("account_balance_drained")

    # Rule 2: unusually large transaction
    if amount >= 1000000:
        fraud_reasons.append("high_value_transaction")

    # Rule 3: original dataset's explicit fraud flag
    # isFlaggedFraud is a transaction-level indicator,
    # not the isFraud ground-truth label.
    if int(txn.get("isFlaggedFraud", 0)) == 1:
        fraud_reasons.append("flagged_by_source_system")

    is_suspicious = len(fraud_reasons) > 0

    result = {
        "step": txn["step"],
        "type": transaction_type,
        "amount": amount,
        "nameOrig": txn["nameOrig"],
        "nameDest": txn["nameDest"],
        "isSuspicious": int(is_suspicious),
        "fraudReasons": fraud_reasons
    }

    if is_suspicious:
        producer.send(OUTPUT_TOPIC, value=result)

        print(
            f"FRAUD ALERT | "
            f"type={transaction_type} | "
            f"amount={amount} | "
            f"reasons={fraud_reasons}"
        )
    else:
        print(
            f"Transaction checked | "
            f"type={transaction_type} | "
            f"amount={amount} | "
            f"status=normal"
        )

    producer.flush()
