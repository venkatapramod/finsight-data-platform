# FinSight convenience targets. Run `make help` for the list.
#
# Services come from docker-compose.yml; Spark jobs run locally with spark-submit.

SPARK_SUBMIT ?= spark-submit
PYTHON       ?= python3
RECORDS      ?= 200000

.PHONY: help up down reset status test quality evaluate customer360 stream produce flagged

help:
	@echo "Services:  make up | down | reset | status"
	@echo "Checks:    make test | quality | evaluate"
	@echo "Pipeline:  make stream   (terminal 1, leave running)"
	@echo "           make produce  (terminal 2, RECORDS=$(RECORDS))"
	@echo "           make flagged  (count alerts in txn-flagged)"
	@echo "Batch:     make customer360"

up:            ## start Kafka, MongoDB, Neo4j; create topics; load profiles
	docker compose up -d
	docker compose ps

down:          ## stop services, keep data
	docker compose down

reset:         ## stop services and delete all their data
	docker compose down -v

status:
	docker compose ps

test:          ## unit tests (no services needed)
	$(PYTHON) -m pytest

quality:       ## data-quality checks on the transactions data
	FINSIGHT_EXPECTED_ROWS=1550448 $(SPARK_SUBMIT) code/quality/check_transactions.py

evaluate:      ## score fraud rules against labels
	$(SPARK_SUBMIT) code/spark/evaluate_fraud_rules.py

customer360:   ## build the Customer 360 table
	$(SPARK_SUBMIT) code/spark/build_customer_360.py

stream:        ## streaming fraud scoring: txn-raw -> txn-flagged
	$(SPARK_SUBMIT) code/spark/fraud_streaming.py

produce:       ## publish transactions to txn-raw
	$(PYTHON) code/kafka/producer.py --max-records $(RECORDS) --quiet

flagged:       ## alerts written to txn-flagged, per partition
	docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh \
	  --bootstrap-server localhost:19092 --topic txn-flagged
