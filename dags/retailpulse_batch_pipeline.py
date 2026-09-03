"""Airflow orchestration for the RetailPulse batch pipeline."""

import os
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG


PROJECT_ROOT = Path(
    os.getenv("RETAILPULSE_PROJECT_ROOT", Path(__file__).resolve().parents[1])
)
LOAD_DATE_TEMPLATE = (
    "{{ dag_run.conf.get('load_date') "
    "or ((dag_run.logical_date or dag_run.run_after) | ds) }}"
)
LOAD_DATE_ENV = {"RETAILPULSE_LOAD_DATE": LOAD_DATE_TEMPLATE}


default_args = {
    "owner": "retailpulse",
    "start_date": pendulum.datetime(2026, 1, 1, tz="UTC"),
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


dag = DAG(
    dag_id="retailpulse_batch_pipeline",
    default_args=default_args,
    description="Orchestrate the RetailPulse local batch data pipeline.",
    schedule=None,
    catchup=False,
    is_paused_upon_creation=True,
    tags=["retailpulse", "batch", "data-engineering"],
)


init_source_schema = BashOperator(
    task_id="init_source_schema",
    bash_command="python -m src.utils.database",
    cwd=str(PROJECT_ROOT),
    dag=dag,
)

seed_source_data = BashOperator(
    task_id="seed_source_data",
    bash_command="python -m src.synthetic_data.generate_retail_data",
    cwd=str(PROJECT_ROOT),
    dag=dag,
)

ingest_postgres_to_lake = BashOperator(
    task_id="ingest_postgres_to_lake",
    bash_command=(
        "python -m src.ingest.postgres_to_lake "
        '--load-date "$RETAILPULSE_LOAD_DATE"'
    ),
    env=LOAD_DATE_ENV,
    append_env=True,
    cwd=str(PROJECT_ROOT),
    dag=dag,
)

validate_bronze_quality = BashOperator(
    task_id="validate_bronze_quality",
    bash_command=(
        "python -m src.quality.validate_bronze "
        '--load-date "$RETAILPULSE_LOAD_DATE"'
    ),
    env=LOAD_DATE_ENV,
    append_env=True,
    cwd=str(PROJECT_ROOT),
    dag=dag,
)

load_silver_to_warehouse = BashOperator(
    task_id="load_silver_to_warehouse",
    bash_command=(
        "python -m src.warehouse.load_silver_to_warehouse "
        '--load-date "$RETAILPULSE_LOAD_DATE"'
    ),
    env=LOAD_DATE_ENV,
    append_env=True,
    cwd=str(PROJECT_ROOT),
    dag=dag,
)

dbt_run = BashOperator(
    task_id="dbt_run",
    bash_command="cd dbt && dbt run --profiles-dir .",
    cwd=str(PROJECT_ROOT),
    dag=dag,
)

dbt_test = BashOperator(
    task_id="dbt_test",
    bash_command="cd dbt && dbt test --profiles-dir .",
    cwd=str(PROJECT_ROOT),
    dag=dag,
)


(
    init_source_schema
    >> seed_source_data
    >> ingest_postgres_to_lake
    >> validate_bronze_quality
    >> load_silver_to_warehouse
    >> dbt_run
    >> dbt_test
)
