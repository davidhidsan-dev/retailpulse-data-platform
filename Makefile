DEMO_LOAD_DATE ?= 2099-01-01
EVENT_COUNT ?= 20
EVENT_SEED ?= 42
KAFKA_CONSUMER_GROUP ?= retailpulse-raw-web-events-v1
KAFKA_CONSUMER_ID ?= retailpulse-consumer-1
KAFKA_CONSUMER_TIMEOUT ?= 15
LOAD_DATE_ARG = $(if $(LOAD_DATE),--load-date $(LOAD_DATE),)

.PHONY: up down logs ps test clean init-db seed-db ingest-lake quality quality-demo load-warehouse dbt-run dbt-test dbt-docs-generate airflow-up airflow-down airflow-logs airflow-ps kafka-up kafka-down kafka-logs kafka-ps kafka-stop-broker-1 kafka-start-broker-1 produce-events consume-events consume-events-partial

up:
	docker compose up -d postgres

down:
	docker compose down

logs:
	docker compose logs -f

ps:
	docker compose ps

test:
	pytest

init-db:
	python -m src.utils.database

seed-db:
	python -m src.synthetic_data.generate_retail_data

ingest-lake:
	python -m src.ingest.postgres_to_lake $(LOAD_DATE_ARG)

quality:
	python -m src.quality.validate_bronze $(LOAD_DATE_ARG)

quality-demo:
	python -m src.quality.create_bad_bronze_demo --source-load-date $(SOURCE_LOAD_DATE) --demo-load-date $(DEMO_LOAD_DATE)
	python -m src.quality.validate_bronze --load-date $(DEMO_LOAD_DATE) --allow-empty --max-rejection-rate 1

load-warehouse:
	python -m src.warehouse.load_silver_to_warehouse $(LOAD_DATE_ARG)

dbt-run:
	cd dbt && dbt run --profiles-dir .

dbt-test:
	cd dbt && dbt test --profiles-dir .

dbt-docs-generate:
	cd dbt && dbt docs generate --profiles-dir .

airflow-up:
	docker compose up -d --build postgres airflow

airflow-down:
	docker compose down

airflow-logs:
	docker compose logs -f airflow

airflow-ps:
	docker compose ps

kafka-up:
	docker compose up -d kafka-1 kafka-2 kafka-3

kafka-down:
	docker compose stop kafka-1 kafka-2 kafka-3

kafka-logs:
	docker compose logs -f kafka-1 kafka-2 kafka-3

kafka-ps:
	docker compose ps kafka-1 kafka-2 kafka-3

kafka-stop-broker-1:
	docker compose stop kafka-1

kafka-start-broker-1:
	docker compose start kafka-1

produce-events:
	python -m src.streaming.produce_web_events --count $(EVENT_COUNT) --seed $(EVENT_SEED)

consume-events:
	python -m src.streaming.consume_web_events --count $(EVENT_COUNT) --timeout-seconds $(KAFKA_CONSUMER_TIMEOUT) --group-id $(KAFKA_CONSUMER_GROUP) --consumer-id $(KAFKA_CONSUMER_ID)

consume-events-partial:
	python -m src.streaming.consume_web_events --count $(EVENT_COUNT) --timeout-seconds $(KAFKA_CONSUMER_TIMEOUT) --group-id $(KAFKA_CONSUMER_GROUP) --consumer-id $(KAFKA_CONSUMER_ID) --allow-partial

clean:
	python -c "import shutil; shutil.rmtree('.pytest_cache', ignore_errors=True)"
	python -c "import shutil; from pathlib import Path; [shutil.rmtree(path, ignore_errors=True) for path in Path('.').rglob('__pycache__') if not {'.git', '.venv', 'venv', 'data'}.intersection(path.parts)]"
