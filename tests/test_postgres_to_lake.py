"""Tests for PostgreSQL source ingestion into local raw and bronze layers."""

from datetime import date
from pathlib import Path

import pandas as pd

from src.ingest.postgres_to_lake import (
    BRONZE_METADATA_COLUMNS,
    SOURCE_SYSTEM,
    add_bronze_metadata,
    ingest_postgres_to_lake,
    parse_args,
    write_bronze_table,
    write_raw_table,
)
from src.utils.paths import build_lake_path


LOAD_DATE = date(2026, 8, 21)
INGESTED_AT = pd.Timestamp("2026-08-21T10:30:00Z")
INGESTION_ID = "11111111-2222-3333-4444-555555555555"


def test_build_lake_path_generates_expected_raw_and_bronze_paths(
    tmp_path: Path,
) -> None:
    raw_path = build_lake_path(
        "raw", "postgres", "customers", LOAD_DATE, tmp_path
    )
    bronze_path = build_lake_path(
        "bronze", "postgres", "customers", LOAD_DATE, tmp_path
    )

    assert raw_path == (
        tmp_path
        / "raw"
        / "postgres"
        / "customers"
        / "load_date=2026-08-21"
        / "customers.csv"
    )
    assert bronze_path == (
        tmp_path
        / "bronze"
        / "postgres"
        / "customers"
        / "load_date=2026-08-21"
        / "customers.parquet"
    )


def test_write_raw_table_writes_csv_without_dataframe_index(
    tmp_path: Path,
) -> None:
    source = pd.DataFrame(
        {"customer_id": [1, 2], "email": ["a@example.com", "b@example.com"]},
        index=[10, 20],
    )

    output_path = write_raw_table(source, "customers", LOAD_DATE, tmp_path)
    written = pd.read_csv(output_path)

    assert output_path.is_file()
    assert list(written.columns) == ["customer_id", "email"]
    pd.testing.assert_frame_equal(written, source.reset_index(drop=True))


def test_write_bronze_table_writes_parquet_with_metadata(
    tmp_path: Path,
) -> None:
    source = pd.DataFrame(
        {"product_id": [1, 2], "sku": ["ELEC-WH-0001", "HOME-DL-0002"]},
        index=[50, 60],
    )

    output_path = write_bronze_table(
        source,
        "products",
        LOAD_DATE,
        INGESTION_ID,
        INGESTED_AT,
        tmp_path,
    )
    written = pd.read_parquet(output_path)

    assert output_path.is_file()
    assert all(column in written.columns for column in BRONZE_METADATA_COLUMNS)
    assert written["ingestion_id"].eq(INGESTION_ID).all()
    assert written["ingested_at"].eq(INGESTED_AT).all()
    assert str(written["ingested_at"].dt.tz) == "UTC"
    assert written["source_system"].eq(SOURCE_SYSTEM).all()
    assert written["source_table"].eq("products").all()
    assert "index" not in written.columns
    assert "__index_level_0__" not in written.columns


def test_add_bronze_metadata_does_not_modify_source_dataframe() -> None:
    source = pd.DataFrame({"order_id": [1, 2]})

    bronze = add_bronze_metadata(
        source, "orders", INGESTION_ID, INGESTED_AT
    )

    assert list(source.columns) == ["order_id"]
    assert list(bronze.columns) == [
        "order_id",
        "ingestion_id",
        "ingested_at",
        "source_system",
        "source_table",
    ]


def test_ingestion_id_is_shared_by_all_tables_in_one_run(tmp_path: Path) -> None:
    source_frames = {
        "customers": pd.DataFrame({"customer_id": [1, 2]}),
        "orders": pd.DataFrame({"order_id": [10], "customer_id": [1]}),
    }

    def read_in_memory_table(_engine: object, table: str) -> pd.DataFrame:
        return source_frames[table].copy()

    results = ingest_postgres_to_lake(
        LOAD_DATE,
        engine=object(),
        data_root=tmp_path,
        tables=("customers", "orders"),
        table_reader=read_in_memory_table,
    )

    ingestion_ids = {result.ingestion_id for result in results.values()}
    ingested_at_values = {result.ingested_at for result in results.values()}
    assert len(ingestion_ids) == 1
    assert len(ingested_at_values) == 1

    for table, result in results.items():
        bronze = pd.read_parquet(result.bronze_path)
        assert bronze["ingestion_id"].nunique() == 1
        assert bronze["ingestion_id"].iloc[0] == result.ingestion_id
        assert bronze["source_table"].eq(table).all()


def test_parse_args_parses_load_date() -> None:
    args = parse_args(["--load-date", "2026-08-21"])
    defaults = parse_args([])

    assert args.load_date == LOAD_DATE
    assert defaults.load_date is None
