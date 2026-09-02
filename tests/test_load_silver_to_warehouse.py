"""Tests for loading local silver data into the warehouse source schema."""

from contextlib import AbstractContextManager
from datetime import date
from pathlib import Path
from unittest.mock import Mock

import pandas as pd

from src.utils.paths import build_lake_path
from src.warehouse.load_silver_to_warehouse import (
    WAREHOUSE_SCHEMA,
    load_silver_to_warehouse,
    parse_args,
    read_silver_table,
    write_warehouse_table,
)


LOAD_DATE = date(2026, 9, 2)


class FakeConnection:
    """Record SQL sent by the loader without using a real database."""

    def __init__(self) -> None:
        self.executed_sql: list[str] = []

    def exec_driver_sql(self, sql: str) -> None:
        self.executed_sql.append(sql)


class FakeTransaction(AbstractContextManager[FakeConnection]):
    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection

    def __enter__(self) -> FakeConnection:
        return self.connection

    def __exit__(self, *args: object) -> None:
        return None


class FakeEngine:
    def __init__(self) -> None:
        self.connection = FakeConnection()

    def begin(self) -> FakeTransaction:
        return FakeTransaction(self.connection)


def _write_silver(
    tmp_path: Path, table: str, dataframe: pd.DataFrame
) -> Path:
    path = build_lake_path("silver", "postgres", table, LOAD_DATE, tmp_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    dataframe.to_parquet(path, index=False)
    return path


def test_read_silver_table_reads_expected_partition(tmp_path: Path) -> None:
    source = pd.DataFrame({"customer_id": [1, 2], "email": ["a@x.com", "b@x.com"]})
    expected_path = _write_silver(tmp_path, "customers", source)

    dataframe, silver_path = read_silver_table(
        "customers", LOAD_DATE, tmp_path
    )

    assert silver_path == expected_path
    pd.testing.assert_frame_equal(dataframe, source)


def test_write_warehouse_table_uses_replace_schema_and_no_index() -> None:
    dataframe = pd.DataFrame({"product_id": [1]})
    to_sql = Mock()
    dataframe.to_sql = to_sql  # type: ignore[method-assign]
    connection = object()

    write_warehouse_table(dataframe, "products", connection)

    to_sql.assert_called_once_with(
        "products",
        con=connection,
        schema=WAREHOUSE_SCHEMA,
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=1_000,
    )


def test_load_silver_to_warehouse_loads_frames_in_one_transaction(
    tmp_path: Path,
) -> None:
    frames = {
        "customers": pd.DataFrame({"customer_id": [1, 2]}),
        "products": pd.DataFrame({"product_id": [10]}),
    }
    for table, dataframe in frames.items():
        _write_silver(tmp_path, table, dataframe)

    schema_path = tmp_path / "warehouse_schema.sql"
    schema_path.write_text(
        "CREATE SCHEMA IF NOT EXISTS warehouse_source;", encoding="utf-8"
    )
    engine = FakeEngine()
    writes: list[tuple[str, pd.DataFrame, object]] = []

    def record_write(
        dataframe: pd.DataFrame, table: str, connection: object
    ) -> None:
        writes.append((table, dataframe.copy(), connection))

    results = load_silver_to_warehouse(
        LOAD_DATE,
        engine=engine,
        data_root=tmp_path,
        tables=("customers", "products"),
        schema_path=schema_path,
        table_writer=record_write,
    )

    assert engine.connection.executed_sql == [
        "CREATE SCHEMA IF NOT EXISTS warehouse_source;",
        'DROP TABLE IF EXISTS "warehouse_source"."customers" CASCADE',
        'DROP TABLE IF EXISTS "warehouse_source"."products" CASCADE',
    ]
    assert [table for table, _, _ in writes] == ["customers", "products"]
    assert all(connection is engine.connection for _, _, connection in writes)
    assert results["customers"].row_count == 2
    assert results["products"].row_count == 1
    assert results["customers"].warehouse_relation == (
        "warehouse_source.customers"
    )
    pd.testing.assert_frame_equal(writes[0][1], frames["customers"])


def test_parse_args_parses_load_date() -> None:
    args = parse_args(["--load-date", "2026-09-02"])
    defaults = parse_args([])

    assert args.load_date == LOAD_DATE
    assert defaults.load_date is None
