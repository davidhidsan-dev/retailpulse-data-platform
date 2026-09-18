# RetailPulse — E-commerce Data Engineering Platform

![CI](https://github.com/davidhidsan-dev/retailpulse-data-platform/actions/workflows/ci.yml/badge.svg)

[Versión en español](README.md)

Data Engineering portfolio project that simulates an end-to-end batch e-commerce data platform: PostgreSQL operational source, local data lake ingestion, quality validation, rejected records, warehouse loading, dbt analytical modeling and optional local Airflow orchestration.

The focus is on building a reproducible, traceable and technically solid batch workflow in a local environment, rather than replicating a production-scale platform.

## Problem simulated

An e-commerce company has operational data spread across customers, products, inventory, orders, order items and payments. Before it can be analyzed, that data needs to be extracted, stored in layers, validated, loaded into a warehouse and transformed into useful analytical models.

RetailPulse reproduces that process with controlled relational synthetic data.

## Current status

Technical v1.0.

Included:

- synthetic e-commerce data generation.
- PostgreSQL operational source.
- local data lake with `raw`, `bronze`, `silver`, `rejected` and `audit` layers.
- Python ingestion from PostgreSQL.
- data quality validation with rejected records.
- deterministic bad-data demo mode.
- `silver` loading into PostgreSQL `warehouse_source`.
- dbt staging and mart models.
- dbt tests for keys, accepted values and relationships.
- optional local Airflow orchestration.
- lightweight pytest and GitHub Actions checks.

v1.0 covers the full pipeline up to the dbt marts and includes local orchestration with Airflow. The dashboard and other extensions are left for later phases.

## Architecture

```text
PostgreSQL source
    ↓
Python ingestion
    ↓
raw CSV
    ↓
bronze Parquet + ingestion metadata
    ↓
Python quality validation
    ├── silver Parquet → warehouse_source PostgreSQL → dbt staging → dbt marts
    ├── rejected Parquet
    └── audit quality_runs.parquet

Airflow orchestrates the end-to-end batch flow above.
```

Airflow does not transform data. It only coordinates existing Python and dbt commands.

More detail: [`docs/architecture.md`](docs/architecture.md).

## Stack

- Python
- pandas
- PostgreSQL
- SQLAlchemy
- Parquet / pyarrow
- dbt-postgres
- local Apache Airflow
- Docker Compose
- pytest
- GitHub Actions
- SQL

## What this project demonstrates

- Layered batch pipeline design with execution traceability.
- Clear separation between ingestion, quality, modeling and orchestration.
- Explicit handling of valid, rejected and audit outputs.
- Dimensional modeling with facts and dimensions built from previously validated data.
- Controlled reruns and practical idempotency through partitions and replace-based loads.
- Use of tests and documentation to define technical and analytical contracts.

## Quick execution

Prepare the environment:

```bash
cp .env.example .env
pip install -r requirements.txt
cp dbt/profiles.yml.example dbt/profiles.yml
```

PowerShell:

```powershell
Copy-Item .env.example .env
Copy-Item dbt/profiles.yml.example dbt/profiles.yml
```

Start PostgreSQL and run the manual flow:

```bash
make up
make init-db
make seed-db
make ingest-lake LOAD_DATE=YYYY-MM-DD
make quality LOAD_DATE=YYYY-MM-DD
make load-warehouse LOAD_DATE=YYYY-MM-DD
make dbt-run
make dbt-test
```

All three targets accept `LOAD_DATE`; if omitted, they use the current UTC date. The Python modules also accept `--load-date`.

## Airflow execution

Airflow is optional and intended for local development.

```bash
make airflow-up
make airflow-ps
make airflow-logs
```

Open:

```text
http://localhost:8080
```

Manually trigger the DAG:

```text
retailpulse_batch_pipeline
```

Optional configuration:

```json
{
  "load_date": "YYYY-MM-DD"
}
```

Stop Airflow:

```bash
make airflow-down
```

More detail: [`docs/orchestration.md`](docs/orchestration.md).

## Data quality and rejected records

Quality validation reads `bronze`, applies completeness, uniqueness, domain, amount and foreign-key checks, and separates:

```text
bronze
    ├── silver   → warehouse → dbt → marts
    ├── rejected → diagnosis / audit / possible reprocessing
    └── audit    → summary by table and run
```

Rejected records keep the original columns and add:

- `quality_run_id`
- `quality_checked_at`
- `rejection_reason`

Rejected records do not feed marts to avoid contaminating business metrics.

More detail: [`docs/data_quality.md`](docs/data_quality.md).

## Quality demo

Because the normal synthetic source is consistent, a run can produce zero rejected records. That is expected.

To demonstrate the rejection path in a controlled way:

```bash
make quality-demo SOURCE_LOAD_DATE=YYYY-MM-DD
```

By default, it creates and validates a demo partition dated `2099-01-01`.

## Analytical model

dbt builds:

Staging:

- `stg_customers`
- `stg_products`
- `stg_inventory`
- `stg_orders`
- `stg_order_items`
- `stg_payments`

Marts:

- `dim_customer`
- `dim_product`
- `dim_date`
- `fact_sales`
- `fact_inventory`

`fact_sales` is at order-item grain (`order_item_id`). `fact_inventory` is at product grain (`product_id`).

More detail: [`docs/warehouse_model.md`](docs/warehouse_model.md).

## Repository structure

```text
.
├── dags/                         # Airflow DAG
├── data/                         # Local lake layers, generated data ignored by Git
├── dbt/                          # dbt project
├── docs/                         # Technical documentation
├── sql/                          # Source and warehouse schema SQL
├── src/
│   ├── ingest/                   # PostgreSQL → lake ingestion
│   ├── quality/                  # Quality, rejected records and demo
│   ├── synthetic_data/           # Synthetic generation
│   ├── utils/                    # Config, paths and database helpers
│   └── warehouse/                # Silver → warehouse loading
├── tests/                        # Unit and static tests
├── docker-compose.yml            # PostgreSQL
├── docker-compose.airflow.yml    # Optional Airflow
├── Dockerfile.airflow
├── Makefile
└── requirements.txt
```

## Technical documentation

- [`docs/architecture.md`](docs/architecture.md)
- [`docs/source_model.md`](docs/source_model.md)
- [`docs/ingestion.md`](docs/ingestion.md)
- [`docs/data_quality.md`](docs/data_quality.md)
- [`docs/data_dictionary.md`](docs/data_dictionary.md)
- [`docs/idempotency.md`](docs/idempotency.md)
- [`docs/warehouse_model.md`](docs/warehouse_model.md)
- [`docs/limitations.md`](docs/limitations.md)
- [`docs/orchestration.md`](docs/orchestration.md)
- [`docs/runbook.md`](docs/runbook.md)
- [`docs/technical_decisions.md`](docs/technical_decisions.md)
- [`docs/v1_validation_checklist.md`](docs/v1_validation_checklist.md)

## Development note

This project was developed with support from Codex as a programming and learning assistant. It was used to compare technical decisions, debug issues, review alternatives, and speed up implementation and documentation tasks.

The pipeline architecture, project scope, result validation, final decisions, and code review were worked through deliberately during development.

## Limitations

- The data is synthetic.
- The pipeline is batch and local.
- The warehouse load uses full refresh / replace.
- There is no incremental loading.
- There is no dashboard in v1.0.
- Airflow is local/dev only.
- There are no SCDs or semantic metrics layer.
- CI does not execute the full pipeline with real services.
- Rejected records are kept in the lake, not analytical marts.

## Reasonable next steps

1. Build a simple dashboard on top of the dbt marts.
2. Add business-oriented metrics for analytical consumption.
3. Explore an incremental loading strategy.
4. Add operational checks around freshness and volume.
5. Evaluate additional extensions only when they solve a concrete project problem.
