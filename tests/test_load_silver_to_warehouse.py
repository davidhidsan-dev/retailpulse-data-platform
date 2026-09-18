"""Tests for loading local silver data into the warehouse source schema."""

from contextlib import AbstractContextManager
from datetime import date
from pathlib import Path
from unittest.mock import Mock

import pandas as pd
import pytest

from src.quality.validate_bronze import validate_bronze_quality
from src.utils.paths import build_audit_path, build_lake_path
from src.warehouse.load_silver_to_warehouse import (
    WAREHOUSE_SCHEMA,
    load_silver_to_warehouse,
    parse_args,
    read_silver_table,
    write_warehouse_table,
)


LOAD_DATE = date(2026, 9, 2)
QUALITY_RUN_ID = "approved-run"


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
        self.begin_calls = 0

    def begin(self) -> FakeTransaction:
        self.begin_calls += 1
        return FakeTransaction(self.connection)


def _write_silver(
    tmp_path: Path, table: str, dataframe: pd.DataFrame
) -> Path:
    path = build_lake_path("silver", "postgres", table, LOAD_DATE, tmp_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    dataframe.to_parquet(path, index=False)
    return path


def _write_audit(tmp_path: Path, tables: tuple[str, ...]) -> None:
    path = build_audit_path(tmp_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "quality_run_id": QUALITY_RUN_ID,
                "load_date": LOAD_DATE.isoformat(),
                "source_system": "postgres",
                "table_name": table,
                "status": "passed",
                "valid_rows": 2 if table == "customers" else 1,
            }
            for table in tables
        ]
    ).to_parquet(path, index=False)


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
        "customers": pd.DataFrame(
            {"customer_id": [1, 2], "quality_run_id": [QUALITY_RUN_ID] * 2}
        ),
        "products": pd.DataFrame(
            {"product_id": [10], "quality_run_id": [QUALITY_RUN_ID]}
        ),
    }
    for table, dataframe in frames.items():
        _write_silver(tmp_path, table, dataframe)

    _write_audit(tmp_path, ("customers", "products"))
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


def test_load_refuses_stale_silver_after_failed_quality_rerun(
    tmp_path: Path,
) -> None:
    customers = pd.DataFrame(
        {
            "customer_id": [1],
            "email": ["valid@example.com"],
            "country": ["Spain"],
            "created_at": [pd.Timestamp("2026-09-02T10:00:00Z")],
            "synthetic_behavior_segment": ["new"],
        }
    )
    products = pd.DataFrame(
        {
            "product_id": [10],
            "sku": ["SKU-10"],
            "product_name": ["Product"],
            "category": ["Electronics"],
            "unit_price": [50.0],
            "created_at": [pd.Timestamp("2026-09-02T10:00:00Z")],
        }
    )
    for table, frame in {"customers": customers, "products": products}.items():
        path = build_lake_path("bronze", "postgres", table, LOAD_DATE, tmp_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        frame.to_parquet(path, index=False)
    validate_bronze_quality(
        LOAD_DATE, data_root=tmp_path, tables=("customers", "products")
    )

    products.loc[0, "unit_price"] = -1
    path = build_lake_path("bronze", "postgres", "products", LOAD_DATE, tmp_path)
    products.to_parquet(path, index=False)
    with pytest.raises(ValueError, match="products has no valid rows"):
        validate_bronze_quality(
            LOAD_DATE, data_root=tmp_path, tables=("customers", "products")
        )

    engine = FakeEngine()
    with pytest.raises(ValueError, match="did not approve products"):
        load_silver_to_warehouse(
            LOAD_DATE,
            engine=engine,
            data_root=tmp_path,
            tables=("products",),
        )
    assert engine.begin_calls == 0


def test_load_refuses_silver_from_another_quality_run(tmp_path: Path) -> None:
    _write_audit(tmp_path, ("customers", "products"))
    _write_silver(
        tmp_path,
        "customers",
        pd.DataFrame({"customer_id": [1, 2], "quality_run_id": [QUALITY_RUN_ID] * 2}),
    )
    _write_silver(
        tmp_path,
        "products",
        pd.DataFrame({"product_id": [10], "quality_run_id": ["older-run"]}),
    )
    engine = FakeEngine()
    with pytest.raises(ValueError, match="products silver does not match"):
        load_silver_to_warehouse(
            LOAD_DATE,
            engine=engine,
            data_root=tmp_path,
            tables=("customers", "products"),
        )
    assert engine.begin_calls == 0


def test_load_refuses_incomplete_latest_quality_run(tmp_path: Path) -> None:
    _write_audit(tmp_path, ("customers",))
    engine = FakeEngine()
    with pytest.raises(ValueError, match="did not approve products"):
        load_silver_to_warehouse(
            LOAD_DATE,
            engine=engine,
            data_root=tmp_path,
            tables=("customers", "products"),
        )
    assert engine.begin_calls == 0


def test_load_refuses_silver_row_count_mismatch(tmp_path: Path) -> None:
    _write_audit(tmp_path, ("customers",))
    _write_silver(
        tmp_path,
        "customers",
        pd.DataFrame({"customer_id": [1], "quality_run_id": [QUALITY_RUN_ID]}),
    )
    engine = FakeEngine()
    with pytest.raises(ValueError, match="customers silver row count 1 does not match"):
        load_silver_to_warehouse(
            LOAD_DATE, engine=engine, data_root=tmp_path, tables=("customers",)
        )
    assert engine.begin_calls == 0


def test_load_refuses_silver_without_quality_audit(tmp_path: Path) -> None:
    _write_silver(
        tmp_path,
        "customers",
        pd.DataFrame({"customer_id": [1], "quality_run_id": [QUALITY_RUN_ID]}),
    )
    engine = FakeEngine()
    with pytest.raises(ValueError, match="No quality audit found"):
        load_silver_to_warehouse(
            LOAD_DATE, engine=engine, data_root=tmp_path, tables=("customers",)
        )
    assert engine.begin_calls == 0


def test_parse_args_parses_load_date() -> None:
    args = parse_args(["--load-date", "2026-09-02"])
    defaults = parse_args([])

    assert args.load_date == LOAD_DATE
    assert defaults.load_date is None
