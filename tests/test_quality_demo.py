"""Tests for the deterministic bad-bronze quality demo."""

from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from src.quality.create_bad_bronze_demo import (
    DEFAULT_DEMO_LOAD_DATE,
    DEMO_INVALID_ID,
    create_bad_bronze_demo,
    parse_args,
)
from src.quality.validate_bronze import validate_bronze_quality
from src.utils.paths import build_lake_path


SOURCE_LOAD_DATE = date(2026, 9, 1)
SECOND_DEMO_LOAD_DATE = date(2099, 1, 2)
INGESTED_AT = pd.Timestamp("2026-09-01T10:00:00Z")


def _with_bronze_metadata(dataframe: pd.DataFrame, table: str) -> pd.DataFrame:
    bronze = dataframe.copy()
    bronze["ingestion_id"] = "11111111-2222-3333-4444-555555555555"
    bronze["ingested_at"] = INGESTED_AT
    bronze["source_system"] = "postgres"
    bronze["source_table"] = table
    return bronze


def _valid_bronze_frames() -> dict[str, pd.DataFrame]:
    return {
        "customers": _with_bronze_metadata(
            pd.DataFrame(
                {
                    "customer_id": [1],
                    "first_name": ["Ana"],
                    "last_name": ["Garcia"],
                    "email": ["valid@example.com"],
                    "country": ["Spain"],
                    "city": ["Madrid"],
                    "created_at": [INGESTED_AT],
                    "synthetic_behavior_segment": ["frequent"],
                }
            ),
            "customers",
        ),
        "products": _with_bronze_metadata(
            pd.DataFrame(
                {
                    "product_id": [10],
                    "sku": ["ELEC-WH-0010"],
                    "product_name": ["Wireless Headphones Pro"],
                    "category": ["Electronics"],
                    "unit_price": [50.0],
                    "created_at": [INGESTED_AT],
                }
            ),
            "products",
        ),
        "inventory": _with_bronze_metadata(
            pd.DataFrame(
                {
                    "product_id": [10],
                    "stock_quantity": [20],
                    "reorder_level": [5],
                    "updated_at": [INGESTED_AT],
                }
            ),
            "inventory",
        ),
        "orders": _with_bronze_metadata(
            pd.DataFrame(
                {
                    "order_id": [100],
                    "customer_id": [1],
                    "order_date": [INGESTED_AT],
                    "order_status": ["completed"],
                    "country": ["Spain"],
                }
            ),
            "orders",
        ),
        "order_items": _with_bronze_metadata(
            pd.DataFrame(
                {
                    "order_item_id": [1000],
                    "order_id": [100],
                    "product_id": [10],
                    "quantity": [2],
                    "unit_price": [50.0],
                    "line_total": [100.0],
                }
            ),
            "order_items",
        ),
        "payments": _with_bronze_metadata(
            pd.DataFrame(
                {
                    "payment_id": [2000],
                    "order_id": [100],
                    "payment_method": ["credit_card"],
                    "payment_status": ["paid"],
                    "payment_amount": [100.0],
                    "payment_date": [INGESTED_AT],
                }
            ),
            "payments",
        ),
    }


def _write_bronze_partition(tmp_path: Path) -> dict[str, pd.DataFrame]:
    frames = _valid_bronze_frames()
    written: dict[str, pd.DataFrame] = {}
    for table, dataframe in frames.items():
        path = build_lake_path(
            "bronze", "postgres", table, SOURCE_LOAD_DATE, tmp_path
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        dataframe.to_parquet(path, index=False)
        written[table] = pd.read_parquet(path)
    return written


def test_demo_copies_all_bronze_tables_and_injects_known_anomalies(
    tmp_path: Path,
) -> None:
    originals = _write_bronze_partition(tmp_path)

    results = create_bad_bronze_demo(
        SOURCE_LOAD_DATE, DEFAULT_DEMO_LOAD_DATE, data_root=tmp_path
    )

    assert set(results) == set(originals)
    demo_frames = {
        table: pd.read_parquet(result.demo_path) for table, result in results.items()
    }
    for table, original in originals.items():
        source_after_demo = pd.read_parquet(
            build_lake_path(
                "bronze", "postgres", table, SOURCE_LOAD_DATE, tmp_path
            )
        )
        pd.testing.assert_frame_equal(source_after_demo, original)
        assert len(demo_frames[table]) == len(original)
        assert list(demo_frames[table].columns) == list(original.columns)

    assert demo_frames["customers"].loc[0, "email"] == "invalid-email"
    assert demo_frames["products"].loc[0, "unit_price"] == -10
    assert demo_frames["inventory"].loc[0, "stock_quantity"] == -5
    assert demo_frames["orders"].loc[0, "customer_id"] == DEMO_INVALID_ID
    assert demo_frames["order_items"].loc[0, "line_total"] == 101.0
    assert demo_frames["payments"].loc[0, "payment_status"] == "unknown_status"


def test_demo_anomalies_are_reproducible(tmp_path: Path) -> None:
    _write_bronze_partition(tmp_path)

    first = create_bad_bronze_demo(
        SOURCE_LOAD_DATE, DEFAULT_DEMO_LOAD_DATE, data_root=tmp_path
    )
    second = create_bad_bronze_demo(
        SOURCE_LOAD_DATE, SECOND_DEMO_LOAD_DATE, data_root=tmp_path
    )

    for table in first:
        first_demo = pd.read_parquet(first[table].demo_path)
        second_demo = pd.read_parquet(second[table].demo_path)
        pd.testing.assert_frame_equal(first_demo, second_demo)


def test_demo_produces_rejected_records_without_changing_normal_flow(
    tmp_path: Path,
) -> None:
    _write_bronze_partition(tmp_path)

    normal_results = validate_bronze_quality(
        SOURCE_LOAD_DATE, data_root=tmp_path
    )
    assert sum(result.rejected_rows for result in normal_results.values()) == 0

    create_bad_bronze_demo(
        SOURCE_LOAD_DATE, DEFAULT_DEMO_LOAD_DATE, data_root=tmp_path
    )
    demo_results = validate_bronze_quality(
        DEFAULT_DEMO_LOAD_DATE,
        data_root=tmp_path,
        allow_empty=True,
        max_rejection_rate=1.0,
    )

    assert sum(result.rejected_rows for result in demo_results.values()) == 6
    assert all(result.status == "warning" for result in demo_results.values())
    for result in demo_results.values():
        rejected = pd.read_parquet(result.rejected_path)
        assert len(rejected) == 1
        assert rejected["rejection_reason"].notna().all()
        assert rejected["rejection_reason"].str.len().gt(0).all()


def test_demo_cli_dates_and_destination_guard(tmp_path: Path) -> None:
    args = parse_args(["--source-load-date", "2026-09-01"])

    assert args.source_load_date == SOURCE_LOAD_DATE
    assert args.demo_load_date == DEFAULT_DEMO_LOAD_DATE

    with pytest.raises(ValueError, match="must be different"):
        create_bad_bronze_demo(
            SOURCE_LOAD_DATE,
            SOURCE_LOAD_DATE,
            data_root=tmp_path,
            tables=(),
        )
