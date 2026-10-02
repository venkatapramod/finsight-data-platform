from pymongo import MongoClient
from neo4j import GraphDatabase

# ============================================================
# MongoDB Configuration
# ============================================================

MONGO_URI = "mongodb://localhost:27017/"
MONGO_DATABASE = "finsight"
MONGO_COLLECTION = "fraud_alerts"

# ============================================================
# Neo4j Configuration
# ============================================================
# Neo4j Desktop is running on Windows.
# WSL connects to the Windows host through the gateway.

NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "FinSight@123"

# ============================================================
# Connect to MongoDB
# ============================================================

mongo = MongoClient(MONGO_URI)

collection = mongo[MONGO_DATABASE][MONGO_COLLECTION]

print("Connected to MongoDB")
print("Database:", MONGO_DATABASE)
print("Collection:", MONGO_COLLECTION)

# ============================================================
# Connect to Neo4j
# ============================================================

print("Connecting to Neo4j:", NEO4J_URI)

driver = GraphDatabase.driver(
    NEO4J_URI,
    auth=(NEO4J_USER, NEO4J_PASSWORD)
)

# Verify Neo4j connection
driver.verify_connectivity()

print("Connected to Neo4j successfully")
print("--------------------------------")

# ============================================================
# Create Graph Data
# ============================================================

def create_graph(tx, alert):
    tx.run(
        """
        MERGE (o:Customer {id: $nameOrig})
        MERGE (d:Customer {id: $nameDest})

        CREATE (o)-[r:SUSPICIOUS_TRANSACTION {
            amount: $amount,
            type: $type,
            step: $step,
            reason: $reason,
            investigationStatus: $status
        }]->(d)
        """,
        nameOrig=alert.get("nameOrig"),
        nameDest=alert.get("nameDest"),
        amount=float(alert.get("amount", 0)),
        type=alert.get("type"),
        step=alert.get("step"),
        reason=", ".join(alert.get("fraudReasons", [])),
        status=alert.get("investigationStatus", "OPEN")
    )


# ============================================================
# Load MongoDB Alerts into Neo4j
# ============================================================

count = 0

with driver.session() as session:
    for alert in collection.find():
        session.execute_write(
            create_graph,
            alert
        )

        count += 1

        print(
            f"Loaded alert {count}: "
            f"{alert.get('type')} | "
            f"{alert.get('amount')} | "
            f"{alert.get('nameOrig')}"
        )

# ============================================================
# Completion
# ============================================================

print("--------------------------------")
print("MongoDB alerts loaded into Neo4j:", count)
print("Neo4j graph creation completed successfully")

# ============================================================
# Close Connections
# ============================================================

driver.close()
mongo.close()
