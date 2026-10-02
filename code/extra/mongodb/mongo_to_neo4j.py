"""
Load fraud alerts from MongoDB into Neo4j as a fraud-ring graph.

Configuration comes from environment variables (never hardcode secrets):
    MONGO_URI        default: mongodb://localhost:27017/
    MONGO_DATABASE   default: finsight
    MONGO_COLLECTION default: fraud_alerts
    NEO4J_URI        default: bolt://localhost:7687
    NEO4J_USER       default: neo4j
    NEO4J_PASSWORD   required

Usage:
    export NEO4J_PASSWORD='your-password'
    python3 code/extra/mongodb/mongo_to_neo4j.py
"""

import os
import sys

from pymongo import MongoClient
from neo4j import GraphDatabase
from neo4j.exceptions import AuthError, ServiceUnavailable

# ============================================================
# Configuration
# ============================================================

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017/")
MONGO_DATABASE = os.environ.get("MONGO_DATABASE", "finsight")
MONGO_COLLECTION = os.environ.get("MONGO_COLLECTION", "fraud_alerts")

NEO4J_URI = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.environ.get("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD")

if not NEO4J_PASSWORD:
    sys.exit("NEO4J_PASSWORD is not set. Run: export NEO4J_PASSWORD='your-password'")

# ============================================================
# Connect to MongoDB
# ============================================================

mongo = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
try:
    mongo.admin.command("ping")
except Exception as exc:
    sys.exit(f"Could not connect to MongoDB at {MONGO_URI}: {exc}")

collection = mongo[MONGO_DATABASE][MONGO_COLLECTION]

print("Connected to MongoDB")
print("Database:", MONGO_DATABASE)
print("Collection:", MONGO_COLLECTION)
print("Alerts found:", collection.count_documents({}))

# ============================================================
# Connect to Neo4j
# ============================================================

print("Connecting to Neo4j:", NEO4J_URI)

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

try:
    driver.verify_connectivity()
except AuthError:
    mongo.close()
    sys.exit("Neo4j authentication failed: check NEO4J_USER / NEO4J_PASSWORD "
             "and that NEO4J_URI points at the right Neo4j instance.")
except ServiceUnavailable as exc:
    mongo.close()
    sys.exit(f"Neo4j is not reachable at {NEO4J_URI}: {exc}")

print("Connected to Neo4j successfully")
print("--------------------------------")

# ============================================================
# Schema: unique constraints (also speed up MERGE)
# ============================================================

with driver.session() as session:
    session.run(
        "CREATE CONSTRAINT customer_id IF NOT EXISTS "
        "FOR (c:Customer) REQUIRE c.id IS UNIQUE"
    )

# ============================================================
# Create Graph Data
# ============================================================
# MERGE on alertId makes the script safe to rerun:
# running it twice will not duplicate relationships.

def create_graph(tx, alert):
    tx.run(
        """
        MERGE (o:Customer {id: $nameOrig})
        MERGE (d:Customer {id: $nameDest})
        MERGE (o)-[r:SUSPICIOUS_TRANSACTION {alertId: $alertId}]->(d)
        SET r.amount = $amount,
            r.type = $type,
            r.step = $step,
            r.reason = $reason,
            r.investigationStatus = $status
        """,
        alertId=str(alert.get("_id")),
        nameOrig=alert.get("nameOrig"),
        nameDest=alert.get("nameDest"),
        amount=float(alert.get("amount", 0) or 0),
        type=alert.get("type"),
        step=alert.get("step"),
        reason=", ".join(alert.get("fraudReasons", []) or []),
        status=alert.get("investigationStatus", "OPEN"),
    )


# ============================================================
# Load MongoDB Alerts into Neo4j
# ============================================================

count = 0
skipped = 0

try:
    with driver.session() as session:
        for alert in collection.find():
            if not alert.get("nameOrig") or not alert.get("nameDest"):
                skipped += 1
                continue

            session.execute_write(create_graph, alert)
            count += 1

            print(
                f"Loaded alert {count}: "
                f"{alert.get('type')} | "
                f"{alert.get('amount')} | "
                f"{alert.get('nameOrig')} -> {alert.get('nameDest')}"
            )

    # ========================================================
    # Verify what is in the graph
    # ========================================================

    with driver.session() as session:
        customers = session.run(
            "MATCH (c:Customer) RETURN count(c) AS n"
        ).single()["n"]
        relationships = session.run(
            "MATCH ()-[r:SUSPICIOUS_TRANSACTION]->() RETURN count(r) AS n"
        ).single()["n"]

    # ========================================================
    # Completion
    # ========================================================

    print("--------------------------------")
    print("MongoDB alerts loaded into Neo4j:", count)
    if skipped:
        print("Alerts skipped (missing nameOrig/nameDest):", skipped)
    print("Customer nodes in Neo4j:", customers)
    print("SUSPICIOUS_TRANSACTION relationships in Neo4j:", relationships)
    print("Neo4j graph creation completed successfully")

finally:
    # ========================================================
    # Close Connections
    # ========================================================
    driver.close()
    mongo.close()
