import json
from kafka import KafkaConsumer
from pymongo import MongoClient

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "txn-flagged"

MONGO_URI = "mongodb://localhost:27017/"
MONGO_DATABASE = "finsight"
MONGO_COLLECTION = "fraud_alerts"

# Connect to MongoDB
client = MongoClient(MONGO_URI)
db = client[MONGO_DATABASE]
collection = db[MONGO_COLLECTION]

print("Connected to MongoDB")
print("Database:", MONGO_DATABASE)
print("Collection:", MONGO_COLLECTION)

# Connect to Kafka
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="finsight-mongodb-consumer",
    value_deserializer=lambda value: json.loads(value.decode("utf-8"))
)

print("Connected to Kafka:", KAFKA_BOOTSTRAP_SERVERS)
print("Reading topic:", KAFKA_TOPIC)
print("--------------------------------")

for message in consumer:
    alert = message.value

    # Add MongoDB metadata
    alert["source"] = "Kafka txn-flagged"
    alert["investigationStatus"] = "OPEN"

    collection.insert_one(alert)

    print(
        f"FRAUD ALERT STORED | "
        f"type={alert.get('type')} | "
        f"amount={alert.get('amount')} | "
        f"nameOrig={alert.get('nameOrig')}"
    )
