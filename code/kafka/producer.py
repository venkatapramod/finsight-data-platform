import os
import csv
import json
import time
import argparse
from kafka import KafkaProducer

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "txn-raw"

# Input dataset
CSV_FILE = os.environ.get("TRANSACTIONS_CSV", "data/Transactions.csv")

parser = argparse.ArgumentParser(description="FinSight PaySim Kafka producer")
parser.add_argument("--max-records", type=int, default=0,
                    help="Records to publish; 0 = entire file")
parser.add_argument("--rate", type=int, default=1000,
                    help="Target messages per second (spec 6.2)")
parser.add_argument("--quiet", action="store_true",
                    help="Suppress per-record logging for full-file runs")
args = parser.parse_args()

# Connect schema declaration, required by the HDFS Sink Parquet converter.
TXN_SCHEMA = {
    "type": "struct",
    "name": "transaction",
    "optional": False,
    "fields": [
        {"field": "step",           "type": "int32",  "optional": False},
        {"field": "type",           "type": "string", "optional": False},
        {"field": "amount",         "type": "double", "optional": False},
        {"field": "nameOrig",       "type": "string", "optional": False},
        {"field": "oldbalanceOrg",  "type": "double", "optional": False},
        {"field": "newbalanceOrig", "type": "double", "optional": False},
        {"field": "nameDest",       "type": "string", "optional": False},
        {"field": "oldbalanceDest", "type": "double", "optional": False},
        {"field": "newbalanceDest", "type": "double", "optional": False},
        {"field": "isFraud",        "type": "int32",  "optional": False},
        {"field": "isFlaggedFraud", "type": "int32",  "optional": False},
    ],
}

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    linger_ms=20,
    batch_size=65536,
    acks=1,
)

print("Connected to Kafka:", KAFKA_BOOTSTRAP_SERVERS)
print("Reading:", CSV_FILE)
print("Publishing to topic:", KAFKA_TOPIC)
print("Target rate:", args.rate, "msg/sec")
print("Record limit:", args.max_records if args.max_records else "full file")
print("-" * 60)

records_sent = 0
start = time.time()

try:
    with open(CSV_FILE, "r", newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row in reader:
            row["step"] = int(row["step"])
            row["amount"] = float(row["amount"])
            row["oldbalanceOrg"] = float(row["oldbalanceOrg"])
            row["newbalanceOrig"] = float(row["newbalanceOrig"])
            row["oldbalanceDest"] = float(row["oldbalanceDest"])
            row["newbalanceDest"] = float(row["newbalanceDest"])
            row["isFraud"] = int(row["isFraud"])
            row["isFlaggedFraud"] = int(row["isFlaggedFraud"])

            producer.send(KAFKA_TOPIC,
                          value={"schema": TXN_SCHEMA, "payload": row})
            records_sent += 1

            if not args.quiet:
                print(f"Published transaction {records_sent}: "
                      f"type={row['type']}, amount={row['amount']}, "
                      f"isFraud={row['isFraud']}")
            elif records_sent % 100000 == 0:
                rate_now = records_sent / (time.time() - start)
                print(f"Published {records_sent:,} records | "
                      f"{rate_now:,.1f} msg/sec")

            # Pace to the target throughput of spec section 6.2.
            if args.rate > 0:
                drift = (start + records_sent / args.rate) - time.time()
                if drift > 0:
                    time.sleep(drift)

            if args.max_records and records_sent >= args.max_records:
                break

    producer.flush()

finally:
    producer.close()
    elapsed = time.time() - start
    print("-" * 60)
    print("Producer completed successfully")
    print(f"Total records sent: {records_sent:,}")
    print(f"Elapsed time      : {elapsed:,.2f} seconds")
    if elapsed > 0:
        print(f"Throughput        : {records_sent / elapsed:,.1f} messages/sec")
