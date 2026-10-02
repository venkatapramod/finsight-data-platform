# FinSight — Banking Data Engineering & Fraud Analytics Platform

[![tests](https://github.com/venkatapramod/finsight-data-platform/actions/workflows/tests.yml/badge.svg)](https://github.com/venkatapramod/finsight-data-platform/actions/workflows/tests.yml)

An end-to-end big data engineering project for **NovaCrest Bank**, integrating streaming ingestion, distributed storage, batch and stream processing, multiple database paradigms, data blending, and business intelligence.

FinSight explores four banking use cases:

- Real-time transaction fraud flagging
- Customer 360 and risk analytics
- Compliance reporting and account dormancy analysis
- Fraud-ring detection using graph relationships

**Built by:** Venkata Pramod

## 1. Project Overview

Financial institutions need reliable data pipelines to process large transaction volumes, integrate customer information, identify suspicious activity, and provide timely reporting.

FinSight demonstrates a data platform that combines transaction streaming, distributed processing, database integration, and Power BI reporting using a synthetic mobile-money transaction dataset and synthetic customer profiles.

### Business problems addressed

| Use case | Implementation |
|---|---|
| Transaction fraud detection | Kafka ingestion and Spark Structured Streaming scoring, with rules evaluated against labelled data |
| Customer 360 | Customer–account crosswalk joining 10,000 profiles to transaction history, plus Power BI reporting |
| Compliance reporting | Spark SQL aggregations and reporting outputs |
| Fraud-ring analysis | Neo4j account–transaction graph and Cypher queries |

**Project scope:** This is an educational portfolio project using synthetic data. Its rules and metrics are not validated for real-world banking deployment.

## 2. Key Features

- Kafka-based transaction ingestion and event streaming.
- HDFS data lake storage using Parquet.
- Spark Structured Streaming for fraud flagging and churn signals.
- Spark batch processing for risk and customer lifetime value (CLV) scoring.
- Hive-compatible tables and SQL-based reporting through Spark SQL.
- MongoDB customer profile storage.
- Neo4j graph modelling for relationship analysis.
- Alteryx workflows for data blending.
- Power BI dashboards for fraud alerts, customer analytics, and risk reporting.
- Fraud-rule evaluation on a time-based tune/test split (precision, recall, false discovery rate, false-positive rate).
- Unit tests (pytest), data-quality checks, and CI on every push (GitHub Actions).
- Docker Compose for Kafka, MongoDB and Neo4j, with a Makefile for common tasks.

## 3. Technology Stack

| Layer | Technology | Version tested | Purpose |
|---|---|---|---|
| Streaming ingestion | Apache Kafka (KRaft mode) | 3.7.2 | Transaction event ingestion and topic-based routing |
| Distributed storage | HDFS | Hadoop 3.3.6 | Data lake storage |
| File format | Apache Parquet | — | Structured, partitioned data storage |
| Stream processing | Spark Structured Streaming | Spark 3.5.7 | Fraud flagging and churn signals |
| Batch processing | Apache Spark | 3.5.7 | Risk and CLV scoring |
| SQL analytics | Spark SQL | 3.5.7 | Compliance, customer summary, and dormancy reports |
| Data warehouse | Hive tables via Spark's metastore | — | External tables, fraud views, and summary outputs |
| Document database | MongoDB | — | Customer KYC profiles |
| Graph database | Neo4j | — | Account and transaction relationships |
| Data blending | Alteryx | — | Integration and transformation workflows |
| Business intelligence | Power BI | — | Interactive dashboards |
| Language | Python | 3.10 | Producer, loaders, and Spark jobs |
| Testing and CI | pytest, GitHub Actions | — | Unit tests and data-quality checks on every push |
| Local services | Docker Compose | — | Kafka, MongoDB and Neo4j in one command |

## 4. Architecture and Data Flow

```text
          Synthetic Transactions CSV
                     |
                     v
               Apache Kafka
                     |
          +----------+-----------+
          |          |           |
          v          v           v
      Fraud       Churn       HDFS Sink
      Scoring     Signals     / Parquet
          |          |           |
          v          v           v
     Flagged      HDFS       Hive / Spark SQL
     Events                  Batch Analytics
                                 |
             +-------------------+
             |
      +------+------+
      |             |
      v             v
   MongoDB         Neo4j
   Customer        Account-
   Profiles        Transaction Graph
      |             |
      +------+------+
             |
             v
          Alteryx
             |
             v
          Power BI
       Three Dashboards
```

## 5. Dashboard Screenshots

### Fraud Alert Board

Shows the transaction fraud-flagging outputs, exposure indicators, and fraud-related metrics.

![Fraud Alert Board](images/dashboard-1-fraud-alert-board.png)

### Customer 360

Presents customer segments, risk and churn indicators, and profile-related analytics.

![Customer 360](images/dashboard-2-customer-360.png)

### Risk and Compliance

Presents transaction trends, risk indicators, compliance summaries, and dormancy reporting.

![Risk and Compliance](images/dashboard-3-risk-compliance.png)

The dashboards are based on the project outputs and should be interpreted in light of the data limitations described below.

## 6. Dataset and Data Quality

### Transaction dataset

The primary transaction dataset follows the PaySim synthetic financial transaction schema. The full transaction CSV is not included in this repository.

| Property | Value |
|---|---|
| Rows | 1,550,448 |
| Columns | 11 |
| File size | Approximately 121 MB |
| Simulated time steps | 154 |
| Labelled fraud records | 1,754 |
| Labelled fraud proportion | Approximately 0.113% |

The dataset contains transaction types, amounts, origin and destination identifiers, account balances, and fraud labels.

To reproduce the transaction-processing stages, obtain a compatible PaySim-format dataset from its authorized distribution source and place it at `data/Transactions.csv` (or set the `TRANSACTIONS_CSV` environment variable to its path). Confirm the file's schema and the right to use it before running the pipeline.

### Customer profiles

`data/novacrest_customers.json` contains 10,000 synthetic customer profile records used in the MongoDB component. The profiles include fields such as customer ID, segment, products, KYC status, risk score, churn probability, and preferred channel.

### Neo4j graph data

The `data/` directory contains CSV files for graph nodes and relationships:

- `neo4j_accounts_nodes.csv` — 499 accounts
- `neo4j_transaction_nodes.csv` — 1,554 transactions
- `neo4j_sent_rels.csv` — 1,554 relationships
- `neo4j_received_rels.csv` — 1,554 relationships

### Data quality considerations

The transaction labels are highly imbalanced, and most origin account identifiers appear only once (1,549,889 distinct senders across 1,550,448 transactions).

The synthetic customer profiles were generated independently of the transaction account identifiers, so they never matched (2 of 10,000 by coincidence). `code/spark/build_customer_360.py` fixes this with a customer → account crosswalk, the standard banking pattern: accounts are ranked by total activity (sent and received) and paired deterministically with profiles. The mapping is synthetic, so Customer 360 metrics demonstrate the pipeline rather than real customer behaviour.

`code/quality/check_transactions.py` validates schema, row counts, nulls, transaction types, value ranges and label validity, and exits non-zero on failure so it can gate a pipeline run.

## 7. Setup and Execution

### Quick start

```bash
cp .env.example .env          # set NEO4J_PASSWORD (8+ characters)
make up                       # Kafka, MongoDB, Neo4j via Docker Compose; topics + profiles loaded
pip install -r requirements.txt
make test                     # unit tests, no services needed
```

`make help` lists the other targets (`quality`, `evaluate`, `customer360`, `stream`, `produce`, `flagged`). Spark and HDFS run outside Docker; the manual setup below covers them.

### Prerequisites

- Python 3.10 or later
- Hadoop 3.x with HDFS
- Apache Kafka 3.x (KRaft mode)
- Apache Spark 3.5.x, with the Kafka connector JARs (`spark-sql-kafka-0-10_2.12`, `spark-token-provider-kafka-0-10_2.12`, `kafka-clients`) in `$SPARK_HOME/jars`, or passed with `--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7`
- MongoDB
- Neo4j
- Alteryx and Power BI Desktop for the supplied workflows and report

Install the Python dependencies (`pyspark` is pinned to 3.5.7 to match Spark; a different PySpark version fails with `'JavaPackage' object is not callable`):

```bash
pip install -r requirements.txt
```

### Environment notes

These issues came up when reproducing the pipeline and are worth checking first:

- **Hive metastore compatibility.** Spark 3.5's built-in Hive client supports metastore versions up to 3.1. It cannot talk to a Hive 4.x metastore service (`Invalid method name: 'get_table'`). Use Spark's embedded metastore instead, for example in `$SPARK_HOME/conf/spark-defaults.conf`:

  ```text
  spark.hadoop.hive.metastore.uris
  spark.hadoop.javax.jdo.option.ConnectionURL  jdbc:derby:;databaseName=/path/to/spark_metastore_db;create=true
  ```

- **Kafka storage location.** Kafka's default KRaft `log.dirs` is under `/tmp`, which is cleared on restart (including WSL restarts). Point `log.dirs` in `config/kraft/server.properties` to a persistent folder before formatting storage.
- **Credentials.** Copy `.env.example` and set values locally. Scripts read secrets such as `NEO4J_PASSWORD` from environment variables; nothing is hardcoded.

### Step 1: Clone the repository

```bash
git clone https://github.com/venkatapramod/finsight-data-platform.git
cd finsight-data-platform
```

### Step 2: Prepare the transaction data

Place the transaction CSV at `data/Transactions.csv`, or:

```bash
export TRANSACTIONS_CSV=/path/to/Transactions.csv
```

### Step 3: Start the services

```bash
start-dfs.sh                                  # HDFS
hdfs dfsadmin -safemode wait

cd $KAFKA_HOME                                # Kafka (first run only: format storage)
bin/kafka-storage.sh format -t $(bin/kafka-storage.sh random-uuid) -c config/kraft/server.properties
bin/kafka-server-start.sh config/kraft/server.properties

sudo systemctl start mongod                   # MongoDB
sudo neo4j start                              # Neo4j
```

### Step 4: Create Kafka topics

```bash
for t in txn-raw txn-flagged txn-churn; do
  kafka-topics.sh --create --if-not-exists --topic $t \
    --partitions 3 --replication-factor 1 \
    --bootstrap-server localhost:9092
done
```

### Step 5: Run streaming and ingestion

`fraud_streaming.py` reads from the latest offset, so start it **before** the producer:

```bash
# Terminal A: streaming fraud scoring (txn-raw -> txn-flagged)
spark-submit code/spark/fraud_streaming.py

# Terminal B: publish transactions (default rate 1,000 msg/sec)
python3 code/kafka/producer.py --max-records 200000 --quiet
```

Count the flagged events once streaming has caught up:

```bash
kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic txn-flagged
```

Other jobs:

```bash
spark-submit code/spark/load_transactions.py
spark-submit code/spark/compute_customer_baseline.py
spark-submit code/spark/churn_streaming.py
```

### Step 6: Run batch analytics

```bash
spark-submit code/spark/risk_scoring.py
spark-submit code/spark/clv_scoring.py
spark-submit code/spark/spark_sql_jobs.py --mode compliance
spark-submit code/spark/spark_sql_jobs.py --mode customer_summary
spark-submit code/spark/spark_sql_jobs.py --mode dormancy
```

### Step 7: Load database objects

```bash
spark-sql -f code/sql/hive_ddl.sql            # creates finsight db, external table, repairs partitions
spark-sql -f code/sql/fraud_view.sql
```

Build the Customer 360 table (customer → account crosswalk plus activity):

```bash
spark-submit code/spark/build_customer_360.py
```

Load the customer profiles into MongoDB:

```bash
mongoimport --db finsight --collection customers \
  --file data/novacrest_customers.json --jsonArray
```

Load the Neo4j graph:

```bash
export NEO4J_PASSWORD='<your-password>'
python3 code/neo4j/neo4j_loader.py
```

Optionally, load MongoDB fraud alerts into Neo4j (safe to rerun; relationships are merged on the alert ID):

```bash
python3 code/extra/mongodb/mongo_to_neo4j.py
```

### Step 8: Tests and checks

```bash
python3 -m pytest                                                        # unit tests
FINSIGHT_EXPECTED_ROWS=1550448 spark-submit code/quality/check_transactions.py   # data quality
spark-submit code/spark/evaluate_fraud_rules.py                          # fraud-rule evaluation
```

### Step 9: Open the reporting assets

- Alteryx workflows: `alteryx/`
- Report and documentation: `docs/`
- Dashboard screenshots: `images/`

## 8. Results and Validation

The table separates figures **reproduced** from the stored pipeline outputs (October 2026) from figures **reported** in the original project run (see the [project report (PDF)](https://github.com/venkatapramod/finsight-data-platform/raw/main/docs/FinSight_Report_Updated.pdf)).

| Metric | Result | Status |
|---|---:|---|
| Transactions in CSV, HDFS Parquet and Hive external table | 1,550,448 rows | Reproduced |
| HDFS landing | 154 `step` partitions | Reproduced |
| Hive fraud view (`isFraud = 1`) | 1,754 rows | Reproduced |
| Streaming run: records consumed from `txn-raw` | 200,000 | Reproduced |
| Streaming run: events flagged to `txn-flagged` (v1 rule) | 989 | Reproduced |
| Producer throughput (200K records) | 1,000.0 msg/sec over 200.01 s | Reproduced |
| Producer throughput (50K records) | ~999.8 msg/sec | Reported |
| MongoDB customer profiles | 10,000 documents | Reproduced |
| Neo4j graph input | 499 accounts, 1,554 transactions, 3,108 relationships | Reproduced (loader CSVs) |
| Fraud-ring query output | 157 accounts | Reported |
| Customer profiles joined to transaction history | 10,000 of 10,000 (was 2) | Reproduced |
| Transactions per mapped customer | min 21, avg 27.9, max 98 | Reproduced |
| Mapped customers involved in fraud | 172 | Reproduced |

**About throughput:** the producer is rate-limited (`--rate`, default 1,000 msg/sec). The throughput figures show that the pipeline sustained the target rate; they are not a measure of Kafka's maximum capacity.

### Fraud-rule evaluation

Rules are scored against the PaySim fraud labels with `code/spark/evaluate_fraud_rules.py`, using a time-based split: steps 1–100 to choose a rule, steps 101–154 held out for reporting.

| Rule | Logic | Test precision | Test recall | Test FPR |
|---|---|---:|---:|---:|
| v1 (original) | TRANSFER/CASH_OUT, amount > 200,000, receiver's new balance = 0 | 40.79% | 32.98% | 0.06% |
| **v2 (current)** | TRANSFER/CASH_OUT that moves the sender's entire opening balance | **100.00%** | **99.47%** | **0.00%** |

v2 uses only the amount and the sender's opening balance, both known when a transaction is authorised; v1 relied on a post-transaction balance. Its near-perfect score reflects how PaySim generates fraud (fraud agents empty the victim's account), so it should not be read as real-world performance. The rule lives in `code/spark/fraud_rules.py`, shared by the streaming job, the evaluation script and the tests.

### Validation before relying on results

- Confirm the input file row count and schema.
- Verify Kafka topic creation and consumption.
- Check Spark job completion and output counts.
- Confirm HDFS partitions and Hive query results.
- Check MongoDB document counts and indexes.
- Validate Neo4j nodes, relationships, and query results.
- Recalculate fraud precision, recall, and false-positive rate against ground-truth labels.
- Check that Power BI measures match their source data.

## 9. Known Limitations and Future Improvements

### Fraud-rule precision

The original project reported an "80.69% false-positive rate" for the v1 rule. That figure is the share of *alerts* that were false (the false discovery rate); the true false-positive rate, measured against all legitimate transactions, was well under 1%.

The v1 rule's low precision (15% on the tune split, 41% on the test split) was addressed by evaluating sender-balance features against the labels; see *Fraud-rule evaluation* above. Because PaySim fraud follows a single pattern, a production system would need a richer feature set and model, validated on real data.

### Customer profile joins

Only 2 of the 10,000 customer profiles originally matched the transaction identifiers, because the two datasets were generated independently. This is now handled by the customer → account crosswalk described in *Data quality considerations*, which joins all 10,000 profiles. The pairing is synthetic, so behavioural metrics show the pipeline working, not real customer behaviour.

### Churn frequency baseline

The transaction dataset contains limited repeated activity for most origin accounts, which constrains frequency-based behavioural signals. Churn outputs from different runs also differ (12 alerts in the stored Parquet output, 60 in the CSV export), so churn counts are not presented as headline results.

### Data integration

Hive ODBC was unavailable in the tested environment, so an exported CSV was used by the Alteryx workflow instead of querying the mart directly. Configuring HiveServer2 (or the Spark Thrift Server) and validating the ODBC connection would allow the integration path to be tested more directly.

### Power BI connectivity

The dashboards use Import mode rather than DirectQuery. A future iteration could evaluate refresh workflows or alternative connectivity modes.

### Additional engineering improvements

Done: unit tests and CI, data-quality checks, fraud-rule evaluation on held-out data, pinned dependency versions, and Docker Compose for the supporting services.

Next:

- Orchestrate the batch jobs with Airflow instead of cron.
- Add integration tests that run against the Compose services.
- Containerise Spark and HDFS for a fully one-command environment.
- Add pipeline failure handling, retries, and structured logging.

## 10. Repository Structure, Security, and Contact

### Repository structure

```text
finsight-data-platform/
├── alteryx/       # Alteryx workflows and outputs
├── code/          # Kafka producer, Spark jobs, SQL, quality checks, and graph loaders
├── data/          # Customer profiles and Neo4j CSV assets
├── docs/          # Project reports and documentation
├── images/        # Dashboard screenshots
├── tests/         # pytest unit tests
├── .github/       # CI workflow
├── docker-compose.yml
├── Makefile
├── .env.example
├── requirements.txt
├── CONTRIBUTING.md
├── SECURITY.md
├── LICENSE
└── README.md
```

### Security and data handling

- Do not commit passwords, API keys, personal credentials, or private configuration.
- Keep large datasets out of Git unless their licensing and repository size are appropriate.
- Store local secrets in environment variables or an untracked local configuration file.
- Review third-party dataset and software licenses before redistributing them.

See `SECURITY.md` for reporting security issues.

### License

This project is released under the [MIT License](LICENSE).

### Author

**Venkata Pramod**

Email: [pramodvchalla@gmail.com](mailto:pramodvchalla@gmail.com)

GitHub: [venkatapramod](https://github.com/venkatapramod)

---

*FinSight is an educational data engineering and analytics portfolio project. It is not a production banking system, and its fraud flags and risk scores should not be used to make real financial decisions.*
