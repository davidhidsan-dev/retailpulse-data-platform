DEMO_LOAD_DATE ?= 2099-01-01

.PHONY: up down logs ps test clean init-db seed-db ingest-lake quality quality-demo load-warehouse dbt-run dbt-test dbt-docs-generate

up:
	docker compose up -d

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
	python -m src.ingest.postgres_to_lake

quality:
	python -m src.quality.validate_bronze

quality-demo:
	python -m src.quality.create_bad_bronze_demo --source-load-date $(SOURCE_LOAD_DATE) --demo-load-date $(DEMO_LOAD_DATE)
	python -m src.quality.validate_bronze --load-date $(DEMO_LOAD_DATE)

load-warehouse:
	python -m src.warehouse.load_silver_to_warehouse $(if $(LOAD_DATE),--load-date $(LOAD_DATE),)

dbt-run:
	cd dbt && dbt run --profiles-dir .

dbt-test:
	cd dbt && dbt test --profiles-dir .

dbt-docs-generate:
	cd dbt && dbt docs generate --profiles-dir .

clean:
	python -c "import shutil; shutil.rmtree('.pytest_cache', ignore_errors=True)"
	python -c "import shutil; from pathlib import Path; [shutil.rmtree(path, ignore_errors=True) for path in Path('.').rglob('__pycache__') if not {'.git', '.venv', 'venv', 'data'}.intersection(path.parts)]"
