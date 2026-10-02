# FinSight — Real-Time Banking Fraud & Customer Intelligence Platform

An end-to-end big data platform for a retail bank, integrating ten technologies across
streaming ingestion, distributed storage, real-time and batch processing, three database
paradigms, data blending and executive reporting.

Built solo. Processes **1.55 million transactions** through Kafka at ~1,000 msg/sec, scores
fraud in live micro-batches, and surfaces the results in three Power BI dashboards.

---

## Dashboards

**Fraud Alert Board** — live flagged-transaction feed, fraud rate, exposure value, and false
positive rate measured against the legacy baseline.

![Fraud Alert Board](images/dashboard-1-fraud-alert-board.png)

**Customer 360** — risk and churn scoring across 10,000 customer profiles, segment
distribution, channel heatmap, CLV tier breakdown.

![Customer 360](images/dashboard-2-customer-360.png)

**Risk & Compliance** — fraud rate trend against a target threshold, volume by transaction
type, compliance summary and dormancy reporting.

![Risk and Compliance](images/dashboard-3-risk-compliance.png)

---

## Stack

| Layer | Technology | What it does here |
|---|---|---|
| Streaming ingestion | **Apache Kafka** | 3 topics — `txn-raw` (3 partitions), `txn-flagged`, `txn-churn` |
| Distributed storage | **HDFS** | Data lake; Parquet partitioned by hourly step |
| Warehouse | **Apache Hive** | External transaction table, fraud view, pre-aggregated summary mart |
| Document store | **MongoDB** | 10,000 customer KYC profiles, compound-indexed |
| Graph database | **Neo4j** | Account–transaction graph for fraud-ring detection |
| Stream processing | **Spark Structured Streaming** | Two concurrent jobs: fraud scoring and churn detection |
| Batch processing | **Spark Core** | Nightly composite risk scoring and CLV scoring (cron-scheduled) |
| SQL analytics | **Spark SQL** | Compliance aggregation, customer fraud summary, dormancy report |
| Data blending | **Alteryx** | Joins Hive and MongoDB outputs, engineers composite features |
| Visualisation | **Power BI** | Three dashboard pages, custom DAX measures |

## Architecture

```
Transactions.csv (1.55M rows)
        │
        ▼
    Apache Kafka ──── txn-raw (3 partitions)
        │
        ├─── Spark Streaming ── fraud rule scoring ──────▶ txn-flagged
        ├─── Spark Streaming ── 24h windowed churn ──────▶ txn-churn + HDFS
        └─── Kafka Connect HDFS Sink ── Parquet by step
        │
        ▼
      HDFS  /finsight/{raw,processed,reference,exports}
        │
        ├─── Hive        external table · fraud view · summary mart
        ├─── Spark Core  risk scoring · CLV scoring
        └─── Spark SQL   compliance · fraud summary · dormancy
        │
   MongoDB (profiles)          Neo4j (fraud graph)
        │                            │
        └──────────▶ Alteryx ◀───────┘
                        │
                        ▼
                    Power BI
```

---

## Dataset

### `Transactions.csv` — primary transaction feed *(not included; see below)*

Synthetic mobile money transactions in the **PaySim** schema. PaySim is a public agent-based
simulator built on aggregated logs from a real mobile money service, widely used in fraud
research because it carries ground-truth fraud labels that production banking data cannot
share.

| Property | Value |
|---|---|
| Rows | 1,550,448 |
| Columns | 11 |
| File size | 121 MB |
| Time span | 154 steps (1 step = 1 hour, ≈ 6.4 simulated days) |
| Labelled fraud | 1,754 rows (0.113%) |
| Distinct `nameOrig` | 1,549,889 |

**Schema**

| Column | Type | Description |
|---|---|---|
| `step` | int | Hourly time step |
| `type` | string | `CASH_IN` · `CASH_OUT` · `DEBIT` · `PAYMENT` · `TRANSFER` |
| `amount` | double | Transaction amount |
| `nameOrig` | string | Originating account (`C` = customer) |
| `oldbalanceOrg` / `newbalanceOrig` | double | Sender balance before / after |
| `nameDest` | string | Destination account (`C` customer, `M` merchant) |
| `oldbalanceDest` / `newbalanceDest` | double | Recipient balance before / after |
| `isFraud` | int | Ground-truth fraud label |
| `isFlaggedFraud` | int | System-flagged large transfer |

**Distribution**

| Type | Count | Volume (USD) | Fraud | Fraud rate |
|---|---|---|---|---|
| CASH_OUT | 550,460 | 102.26 bn | 884 | 0.1606% |
| PAYMENT | 521,456 | 6.22 bn | 0 | — |
| CASH_IN | 339,514 | 58.08 bn | 0 | — |
| TRANSFER | 128,472 | 83.30 bn | 870 | 0.6772% |
| DEBIT | 10,546 | 0.06 bn | 0 | — |

### Characteristics that shaped the implementation

These four properties drove most of the engineering decisions in this repo:

1. **Fraud occurs only in TRANSFER and CASH_OUT.** The other three types carry no labelled
   fraud at all — which is why the streaming rule filters on those two types before anything
   else.

2. **Almost every sender is unique.** 1,549,889 distinct `nameOrig` values across 1,550,448
   rows means a customer typically appears once as a sender. This breaks any aggregation that
   assumes repeat behaviour per account, and it forced two deliberate design changes (see
   *Design decisions* below).

3. **Severe class imbalance at 0.113%.** Accuracy is meaningless at this ratio — a model
   predicting "never fraud" scores 99.89%. Precision, recall and false-positive rate are
   reported instead.

4. **Destination balances are frequently zero**, including for legitimate merchant payments.
   The account-emptying fraud rule keys on this field, which is the dominant source of its
   false positives.

### `novacrest_customers.json` — customer profiles *(included, `data/`)*

10,000 synthetic KYC records loaded into MongoDB: `customerId`, demographics, `segment`,
`products[]`, `kyc_status`, `risk_score`, `churn_probability`, `preferred_channel`.

Segments: Standard 4,497 · Basic 2,464 · Premium 1,528 · Student 983 · Private Banking 528.

### Neo4j graph CSVs *(included, `data/`)*

499 account nodes, 1,554 transaction nodes, 1,554 `SENT` and 1,554 `RECEIVED_BY` edges.
Model: `(Account)-[:SENT]->(Transaction)-[:RECEIVED_BY]->(Account)` — transactions are
first-class nodes so `amount`, `step` and `isFraud` stay queryable along the path.

### Getting the transaction file

PaySim-format data is publicly available (Kaggle: *Synthetic Financial Datasets For Fraud
Detection*). Place it at `data/Transactions.csv`. Every figure in this README comes from the
1,550,448-row extract described above.

---

## Repository layout

```
├── code/
│   ├── kafka/
│   │   ├── producer.py                   # CSV → Kafka JSON, rate-limited to ~1,000 msg/sec
│   │   └── raw-hdfs-sink.properties      # Kafka Connect HDFS Sink, Parquet by step
│   ├── spark/
│   │   ├── load_transactions.py          # CSV → HDFS Parquet
│   │   ├── fraud_streaming.py            # Real-time fraud scoring + per-batch metrics
│   │   ├── compute_customer_baseline.py  # Per-customer historical baseline
│   │   ├── churn_streaming.py            # 24h windowed churn detection, 4 signals
│   │   ├── risk_scoring.py               # Nightly composite risk score
│   │   ├── clv_scoring.py                # Customer lifetime value
│   │   └── spark_sql_jobs.py             # --mode compliance | customer_summary | dormancy
│   ├── sql/                              # Hive DDL, fraud view, CLV table
│   ├── neo4j/neo4j_loader.py             # Graph loader + fraud-ring Cypher
│   ├── false_positive_check.py           # Rule precision measurement
│   ├── check_overlap.py                  # ID-join diagnostic
│   ├── crontab.txt                       # Batch schedule
│   └── extra/                            # Exploratory work not in the final pipeline
├── data/                                 # Neo4j CSVs + customer profiles
├── alteryx/                              # 2 workflows (.yxmd) + their outputs
├── images/                               # Dashboard screenshots
└── docs/                                 # 61-page build report with verified outputs
```

---

## Running it

Requires Hadoop 3.x, Kafka, Spark 3.5.x, Hive metastore, MongoDB, Neo4j, Python 3.10+.

```bash
# Ingestion
kafka-topics.sh --create --topic txn-raw --partitions 3 --replication-factor 1 \
  --bootstrap-server localhost:9092          # repeat for txn-flagged, txn-churn
python code/kafka/producer.py --quiet
spark-submit code/spark/load_transactions.py

# Streaming (two concurrent jobs)
spark-submit --jars <kafka jars> code/spark/fraud_streaming.py
spark-submit code/spark/compute_customer_baseline.py
spark-submit --jars <kafka jars> code/spark/churn_streaming.py

# Batch — scheduled nightly via cron (see code/crontab.txt)
spark-submit code/spark/risk_scoring.py
spark-submit code/spark/clv_scoring.py
spark-submit code/spark/spark_sql_jobs.py --mode compliance
spark-submit code/spark/spark_sql_jobs.py --mode customer_summary
spark-submit code/spark/spark_sql_jobs.py --mode dormancy

# Databases
spark-sql -f code/sql/hive_ddl.sql
spark-sql -f code/sql/fraud_view.sql
mongoimport --db finsight --collection customers \
  --file data/novacrest_customers.json --jsonArray
NEO4J_PASSWORD='<password>' python code/neo4j/neo4j_loader.py
```

---

## Design decisions

Each of these is a reasoned departure from the naive implementation, driven by the dataset
characteristics above.

**Fixed reference scales instead of max-normalisation in risk scoring.** Because nearly every
account appears once, normalising each factor by the in-batch maximum collapses almost all
customers onto identical scores and makes the lowest risk tier mathematically unreachable.
Factors are normalised against fixed business-meaningful scales instead: 5+ transactions, the
$200,000 large-transfer threshold, CASH_OUT proportion, 5+ distinct counterparties. Result:
a genuine spread across Low 692,897 / Medium 647,544 / High 209,448.

**Dormancy counts activity on both sides of the ledger.** A sender-only count leaves almost no
account meeting the "5 prior transactions" threshold, returning an empty report. Real accounts
are active inbound and outbound, so `nameOrig` and `nameDest` occurrences are combined.

**Percentile ranking for CLV components.** Ranking each customer against the population rather
than against the single largest account lets scores span the full 0–1 range.

**Low-balance churn signal uses a windowed count, not strict consecutiveness.** True
consecutive-order enforcement requires per-customer sequence state
(`flatMapGroupsWithState` or an ordered session window). The windowed approximation captures
the same intent — sustained low balance rather than a momentary dip — at a fraction of the
complexity.

---

## Results

| Metric | Value |
|---|---|
| Producer throughput | 999.8 msg/sec (50K records) · 989.7 msg/sec (200K records) |
| Live streaming run | 200,000 records consumed, 989 flagged, 0.4945% fraud rate |
| HDFS landing | Parquet across 154 step partitions |
| Risk tiers | Low 692,897 · Medium 647,544 · High 209,448 |
| CLV tiers | At Risk 1,535,520 · Growth Potential 14,356 · High Value 13 |
| Dormant accounts | 3,202 Dormant + 255 Severely Dormant |
| Hive | 1,550,448 rows external · 1,754-row fraud view · 1,550,432-row mart |
| MongoDB | 10,000 documents, compound index on `customerId` + `segment` |
| Neo4j | 499 accounts · 1,554 transactions · 3,108 edges |
| Fraud rings detected | 157 accounts with >3 distinct inbound senders |
| Rule performance | 2,786 flagged — 538 true positives, 2,248 false positives |

`docs/FinSight_Report_Updated.pdf` documents every stage with the command run, the code, and
the terminal output it produced.

---

## Known limitations

Stated plainly, because they're the interesting part of the engineering story:

- **The fraud rule's precision is poor.** An 80.69% false-positive rate, worse than the 62%
  legacy baseline it was meant to beat. The rule keys on the *destination* balance; PaySim
  fraud characteristically empties the *sender* account. Adding `newbalanceOrig = 0 AND
  oldbalanceOrg = amount` is the obvious next iteration.
- **Customer profile join coverage is 2 of 10,000.** The MongoDB profiles were generated
  independently of PaySim account IDs, so the Customer 360 composite risk score is driven
  almost entirely by churn probability. Deterministically mapping profiles onto active
  transacting accounts would fix it.
- **The churn frequency baseline is degenerate.** With most customers appearing once,
  `hist_avg_txn_per_12` resolves to the same value for nearly everyone. A minimum-history
  filter would make that signal meaningful.
- **Hive ODBC is unavailable** — the metastore runs embedded Derby, so the Alteryx workflow
  reads an exported CSV rather than querying the mart directly. HiveServer2 would resolve it.
- **Dashboards use import mode**, not DirectQuery.

---

## Credentials

Nothing is committed. The Neo4j loader reads from the environment:

```bash
export NEO4J_PASSWORD='<your password>'
```

---

**Venkata Pramod** · [pramodvchalla@gmail.com](mailto:pramodvchalla@gmail.com)
