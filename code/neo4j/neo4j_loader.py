"""
FinSight — Neo4j Fraud Knowledge Graph Loader
Loads: Account nodes, Transaction nodes, SENT and RECEIVED_BY relationships
Graph model: (Account)-[:SENT]->(Transaction)-[:RECEIVED_BY]->(Account)
"""

import csv
import os
from neo4j import GraphDatabase

URI = "bolt://localhost:7687"
USER = "neo4j"
PASSWORD = os.environ.get("NEO4J_PASSWORD", "")

DATA_DIR = "/home/pramo/finsight/data"
BATCH_SIZE = 500


def read_csv(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def batched(rows, size):
    for i in range(0, len(rows), size):
        yield rows[i:i + size]


def clear_graph(tx):
    tx.run("MATCH (n) DETACH DELETE n")


def create_constraints(tx):
    tx.run("CREATE CONSTRAINT account_id IF NOT EXISTS FOR (a:Account) REQUIRE a.accountId IS UNIQUE")
    tx.run("CREATE CONSTRAINT txn_id IF NOT EXISTS FOR (t:Transaction) REQUIRE t.txnId IS UNIQUE")


def load_accounts(tx, rows):
    tx.run(
        """
        UNWIND $rows AS row
        MERGE (a:Account {accountId: row.accountId})
        SET a.accountType = row.accountType
        """,
        rows=rows,
    )


def load_transactions(tx, rows):
    tx.run(
        """
        UNWIND $rows AS row
        MERGE (t:Transaction {txnId: row.txnId})
        SET t.step = toInteger(row.step),
            t.type = row.type,
            t.amount = toFloat(row.amount),
            t.isFraud = toInteger(row.isFraud)
        """,
        rows=rows,
    )


def load_sent_rels(tx, rows):
    tx.run(
        """
        UNWIND $rows AS row
        MATCH (a:Account {accountId: row.startId})
        MATCH (t:Transaction {txnId: row.endId})
        MERGE (a)-[r:SENT]->(t)
        SET r.amount = toFloat(row.amount),
            r.step = toInteger(row.step),
            r.transactionType = row.transactionType
        """,
        rows=rows,
    )


def load_received_rels(tx, rows):
    tx.run(
        """
        UNWIND $rows AS row
        MATCH (t:Transaction {txnId: row.startId})
        MATCH (a:Account {accountId: row.endId})
        MERGE (t)-[r:RECEIVED_BY]->(a)
        SET r.newbalanceDest = toFloat(row.newbalanceDest),
            r.isFraud = toInteger(row.isFraud)
        """,
        rows=rows,
    )


def main():
    driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))

    accounts = read_csv(f"{DATA_DIR}/neo4j_accounts_nodes.csv")
    transactions = read_csv(f"{DATA_DIR}/neo4j_transaction_nodes.csv")
    sent = read_csv(f"{DATA_DIR}/neo4j_sent_rels.csv")
    received = read_csv(f"{DATA_DIR}/neo4j_received_rels.csv")

    # normalise column names from the CSV header format (accountId:ID, :START_ID, etc.)
    for row in accounts:
        row["accountId"] = row.pop("accountId:ID")
        row.pop(":LABEL", None)

    for row in transactions:
        row["txnId"] = row.pop("txnId:ID")
        row["step"] = row.pop("step:int")
        row["amount"] = row.pop("amount:float")
        row["isFraud"] = row.pop("isFraud:int")
        row.pop(":LABEL", None)

    for row in sent:
        row["startId"] = row.pop(":START_ID")
        row["endId"] = row.pop(":END_ID")
        row["amount"] = row.pop("amount:float")
        row["step"] = row.pop("step:int")
        row.pop(":TYPE", None)

    for row in received:
        row["startId"] = row.pop(":START_ID")
        row["endId"] = row.pop(":END_ID")
        row["newbalanceDest"] = row.pop("newbalanceDest:float")
        row["isFraud"] = row.pop("isFraud:int")
        row.pop(":TYPE", None)

    with driver.session() as session:
        print("Clearing existing graph...")
        session.execute_write(clear_graph)

        print("Creating constraints...")
        session.execute_write(create_constraints)

        print(f"Loading {len(accounts)} accounts...")
        for batch in batched(accounts, BATCH_SIZE):
            session.execute_write(load_accounts, batch)

        print(f"Loading {len(transactions)} transactions...")
        for batch in batched(transactions, BATCH_SIZE):
            session.execute_write(load_transactions, batch)

        print(f"Loading {len(sent)} SENT relationships...")
        for batch in batched(sent, BATCH_SIZE):
            session.execute_write(load_sent_rels, batch)

        print(f"Loading {len(received)} RECEIVED_BY relationships...")
        for batch in batched(received, BATCH_SIZE):
            session.execute_write(load_received_rels, batch)

    driver.close()
    print("Graph load complete!")


if __name__ == "__main__":
    main()
