"""Static contract tests for the optional RetailPulse Airflow orchestration."""

from pathlib import Path
import re


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DAG_PATH = PROJECT_ROOT / "dags" / "retailpulse_batch_pipeline.py"
EXPECTED_TASKS = (
    "init_source_schema",
    "seed_source_data",
    "ingest_postgres_to_lake",
    "validate_bronze_quality",
    "load_silver_to_warehouse",
    "dbt_run",
    "dbt_test",
)


def read_dag() -> str:
    return DAG_PATH.read_text(encoding="utf-8")


def test_airflow_dag_file_and_id_exist() -> None:
    assert DAG_PATH.is_file()
    assert 'dag_id="retailpulse_batch_pipeline"' in read_dag()


def test_dag_defines_expected_bash_tasks() -> None:
    source = read_dag()

    assert source.count("BashOperator(") == len(EXPECTED_TASKS)
    for task_id in EXPECTED_TASKS:
        assert f'task_id="{task_id}"' in source


def test_dag_declares_linear_dependency_order() -> None:
    normalized = re.sub(r"\s+", " ", read_dag())
    dependency_chain = " >> ".join(EXPECTED_TASKS)

    assert dependency_chain in normalized


def test_dag_passes_one_templated_load_date_to_partitioned_steps() -> None:
    source = read_dag()

    assert "dag_run.conf.get('load_date')" in source
    assert "dag_run.logical_date or dag_run.run_after" in source
    assert source.count('--load-date "$RETAILPULSE_LOAD_DATE"') == 3
    assert 'LOAD_DATE_ENV = {"RETAILPULSE_LOAD_DATE": LOAD_DATE_TEMPLATE}' in source


def test_dag_is_manual_paused_and_has_low_retries() -> None:
    source = read_dag()

    assert "schedule=None" in source
    assert "catchup=False" in source
    assert "max_active_runs=1" in source
    assert "is_paused_upon_creation=True" in source
    assert '"retries": 1' in source
    assert '["retailpulse", "batch", "data-engineering"]' in source


def test_dag_only_orchestrates_existing_commands() -> None:
    source = read_dag().lower()

    for forbidden_import in (
        "import pandas",
        "import pyspark",
        "import kafka",
        "import pymongo",
        "import sklearn",
    ):
        assert forbidden_import not in source
    assert "select " not in source
    assert "to_parquet" not in source
    assert "read_sql" not in source


def test_airflow_isolated_from_main_runtime() -> None:
    requirements = (PROJECT_ROOT / "requirements.txt").read_text(encoding="utf-8")
    main_compose = (PROJECT_ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    airflow_compose = (
        PROJECT_ROOT / "docker-compose.airflow.yml"
    ).read_text(encoding="utf-8")

    assert "apache-airflow" not in requirements.lower()
    assert "airflow:" not in main_compose.lower()
    assert "airflow:" in airflow_compose.lower()
    assert "POSTGRES_HOST: postgres" in airflow_compose
    assert 'AIRFLOW__CORE__PARALLELISM: "4"' in airflow_compose
    assert "PATH: /home/airflow/.retailpulse-venv/bin:" in airflow_compose
