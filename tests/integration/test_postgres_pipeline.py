"""Opt-in integration coverage for the pipeline against real PostgreSQL."""

import os
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from src.ingest.postgres_to_lake import ingest_postgres_to_lake
from src.quality.validate_bronze import validate_bronze_quality
from src.synthetic_data.generate_retail_data import generate_all_data
from src.utils.database import (
    execute_sql_file,
    get_engine,
    load_dataframes_to_postgres,
)
from src.warehouse.load_silver_to_warehouse import (
    load_silver_to_warehouse,
    write_warehouse_table,
)


pytestmark = pytest.mark.skipif(
    os.getenv("RUN_POSTGRES_INTEGRATION") != "1",
    reason="requires an explicitly configured disposable PostgreSQL database",
)


def _publish_source_snapshot(
    *,
    engine: Any,
    data_root: Path,
    load_date: date,
    customers: int,
    seed: int,
) -> None:
    frames = generate_all_data(
        n_customers=customers,
        n_products=20,
        n_orders=150,
        seed=seed,
    )
    load_dataframes_to_postgres(frames, engine=engine)
    ingest_postgres_to_lake(load_date, engine=engine, data_root=data_root)
    validate_bronze_quality(load_date, data_root=data_root)


def _customer_count(engine: Any, relation: str) -> int:
    with engine.connect() as connection:
        return int(
            connection.exec_driver_sql(
                f"SELECT COUNT(*) FROM {relation}"
            ).scalar_one()
        )


def _relation_oid(engine: Any, relation: str) -> int:
    with engine.connect() as connection:
        return int(
            connection.exec_driver_sql(
                f"SELECT '{relation}'::regclass::oid"
            ).scalar_one()
        )


def test_pipeline_rerun_and_failed_refresh_are_transactional(
    tmp_path: Path,
) -> None:
    engine = get_engine()
    first_load_date = date(2026, 9, 1)
    second_load_date = date(2026, 9, 2)

    try:
        execute_sql_file(engine=engine)
        _publish_source_snapshot(
            engine=engine,
            data_root=tmp_path,
            load_date=first_load_date,
            customers=40,
            seed=41,
        )
        load_silver_to_warehouse(
            first_load_date,
            engine=engine,
            data_root=tmp_path,
        )

        with engine.begin() as connection:
            connection.exec_driver_sql("CREATE SCHEMA IF NOT EXISTS ci_checks")
            connection.exec_driver_sql(
                "CREATE VIEW ci_checks.customers AS "
                "SELECT customer_id FROM warehouse_source.customers"
            )

        table_oid = _relation_oid(engine, "warehouse_source.customers")
        view_oid = _relation_oid(engine, "ci_checks.customers")
        assert _customer_count(engine, "ci_checks.customers") == 40

        _publish_source_snapshot(
            engine=engine,
            data_root=tmp_path,
            load_date=second_load_date,
            customers=45,
            seed=42,
        )

        def fail_on_products(
            dataframe: pd.DataFrame,
            loading_table: str,
            connection: Any,
        ) -> None:
            if loading_table.startswith("_load_products_"):
                raise RuntimeError("controlled warehouse load failure")
            write_warehouse_table(dataframe, loading_table, connection)

        with pytest.raises(RuntimeError, match="controlled warehouse load failure"):
            load_silver_to_warehouse(
                second_load_date,
                engine=engine,
                data_root=tmp_path,
                table_writer=fail_on_products,
            )

        assert _customer_count(engine, "ci_checks.customers") == 40

        load_silver_to_warehouse(
            second_load_date,
            engine=engine,
            data_root=tmp_path,
        )

        assert _customer_count(engine, "ci_checks.customers") == 45
        assert _relation_oid(engine, "warehouse_source.customers") == table_oid
        assert _relation_oid(engine, "ci_checks.customers") == view_oid
    finally:
        with engine.begin() as connection:
            connection.exec_driver_sql("DROP SCHEMA IF EXISTS ci_checks CASCADE")
        engine.dispose()
