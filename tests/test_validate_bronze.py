"""Tests for bronze-to-silver quality orchestration."""

from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from src.quality.validate_bronze import (
    DEFAULT_MAX_REJECTION_RATE,
    QUALITY_METADATA_COLUMNS,
    parse_args,
    validate_bronze_quality,
    validate_table,
)
from src.utils.paths import build_audit_path, build_lake_path


LOAD_DATE = date(2026, 8, 21)
QUALITY_RUN_ID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
QUALITY_CHECKED_AT = pd.Timestamp("2026-08-21T12:00:00Z")
INGESTED_AT = pd.Timestamp("2026-08-21T10:00:00Z")


def _bronze(dataframe: pd.DataFrame, table: str) -> pd.DataFrame:
    result = dataframe.copy()
    result["ingestion_id"] = "11111111-2222-3333-4444-555555555555"
    result["ingested_at"] = INGESTED_AT
    result["source_system"] = "postgres"
    result["source_table"] = table
    return result


def _valid_bronze_frames() -> dict[str, pd.DataFrame]:
    return {
        "customers": _bronze(
            pd.DataFrame(
                {
                    "customer_id": [1, 2],
                    "first_name": ["Ana", "Louis"],
                    "last_name": ["Garcia", "Martin"],
                    "email": ["valid@example.com", "invalid-email"],
                    "country": ["Spain", "France"],
                    "city": ["Madrid", "Paris"],
                    "created_at": [INGESTED_AT, INGESTED_AT],
                    "synthetic_behavior_segment": ["frequent", "new"],
                }
            ),
            "customers",
        ),
        "products": _bronze(
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
        "inventory": _bronze(
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
        "orders": _bronze(
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
        "order_items": _bronze(
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
        "payments": _bronze(
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


def test_validate_table_separates_rows_and_adds_quality_metadata() -> None:
    customers = _valid_bronze_frames()["customers"]

    valid, rejected = validate_table(
        "customers",
        customers,
        quality_run_id=QUALITY_RUN_ID,
        quality_checked_at=QUALITY_CHECKED_AT,
    )

    assert valid["customer_id"].tolist() == [1]
    assert rejected["customer_id"].tolist() == [2]
    assert rejected["rejection_reason"].str.contains("email").all()
    assert all(column in valid.columns for column in QUALITY_METADATA_COLUMNS)
    assert all(column in rejected.columns for column in QUALITY_METADATA_COLUMNS)
    assert valid["quality_run_id"].eq(QUALITY_RUN_ID).all()
    assert rejected["quality_checked_at"].eq(QUALITY_CHECKED_AT).all()


def test_validate_table_accumulates_multiple_rejection_reasons() -> None:
    products = _valid_bronze_frames()["products"]
    products.loc[0, "sku"] = None
    products.loc[0, "unit_price"] = -1

    _, rejected = validate_table(
        "products",
        products,
        quality_run_id=QUALITY_RUN_ID,
        quality_checked_at=QUALITY_CHECKED_AT,
    )

    reason = rejected.loc[0, "rejection_reason"]
    assert "sku is null" in reason
    assert "unit_price must be > 0" in reason
    assert "; " in reason


@pytest.mark.parametrize("column", ["first_name", "last_name", "city"])
def test_customer_contract_requires_descriptive_columns(column: str) -> None:
    customers = _valid_bronze_frames()["customers"].drop(columns=column)

    with pytest.raises(ValueError, match=rf"customers is missing columns: {column}"):
        validate_table("customers", customers)


def test_customer_contract_rejects_blank_country_and_duplicate_email() -> None:
    customers = _valid_bronze_frames()["customers"]
    customers.loc[0, "country"] = "   "
    customers.loc[1, "email"] = customers.loc[0, "email"]

    _, rejected = validate_table("customers", customers)

    reasons = rejected.set_index("customer_id")["rejection_reason"]
    assert "country is blank" in reasons.loc[1]
    assert "email is duplicated" in reasons.loc[1]
    assert "email is duplicated" in reasons.loc[2]


def test_product_contract_rejects_invalid_date_and_infinite_price() -> None:
    products = _valid_bronze_frames()["products"]
    products["created_at"] = products["created_at"].astype("object")
    products.loc[0, "created_at"] = "not-a-date"
    products.loc[0, "unit_price"] = float("inf")

    _, rejected = validate_table("products", products)

    reason = rejected.loc[0, "rejection_reason"]
    assert "created_at is not a valid datetime" in reason
    assert "unit_price must be > 0" in reason


def test_order_item_contract_rejects_fractional_quantity() -> None:
    frames = _valid_bronze_frames()
    items = frames["order_items"]
    items["quantity"] = items["quantity"].astype(float)
    items.loc[0, "quantity"] = 1.5
    items.loc[0, "line_total"] = 75.0

    _, rejected = validate_table(
        "order_items",
        items,
        {"orders": frames["orders"], "products": frames["products"]},
    )

    reason = rejected.loc[0, "rejection_reason"]
    assert "quantity must be an integer" in reason
    assert "line_total does not match" not in reason


def test_quality_pipeline_writes_silver_rejected_and_audit_without_indexes(
    tmp_path: Path,
) -> None:
    frames = _valid_bronze_frames()
    for table, dataframe in frames.items():
        path = build_lake_path("bronze", "postgres", table, LOAD_DATE, tmp_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        dataframe.to_parquet(path, index=False)

    results = validate_bronze_quality(
        LOAD_DATE,
        data_root=tmp_path,
        quality_run_id=QUALITY_RUN_ID,
        quality_checked_at=QUALITY_CHECKED_AT,
        max_rejection_rate=1.0,
    )

    assert len(results) == 6
    assert results["customers"].input_rows == 2
    assert results["customers"].valid_rows == 1
    assert results["customers"].rejected_rows == 1
    assert results["customers"].rejection_rate == 0.5
    assert results["customers"].status == "warning"

    for table, result in results.items():
        silver = pd.read_parquet(result.silver_path)
        rejected = pd.read_parquet(result.rejected_path)
        assert result.silver_path == build_lake_path(
            "silver", "postgres", table, LOAD_DATE, tmp_path
        )
        assert result.rejected_path == build_lake_path(
            "rejected", "postgres", table, LOAD_DATE, tmp_path
        )
        assert result.silver_path.is_file()
        assert result.rejected_path.is_file()
        assert "index" not in silver.columns
        assert "__index_level_0__" not in silver.columns
        assert "index" not in rejected.columns
        assert "__index_level_0__" not in rejected.columns
        assert all(column in silver.columns for column in QUALITY_METADATA_COLUMNS)
        assert "rejection_reason" in rejected.columns

    audit_path = build_audit_path(tmp_path)
    audit = pd.read_parquet(audit_path)
    assert len(audit) == 6
    assert {
        "input_rows",
        "valid_rows",
        "rejected_rows",
        "rejection_rate",
        "status",
    }.issubset(audit.columns)
    assert audit["quality_run_id"].eq(QUALITY_RUN_ID).all()
    assert set(audit["status"]) == {"passed", "warning"}
    assert "index" not in audit.columns
    assert "__index_level_0__" not in audit.columns


def test_rejected_parents_cause_dependent_rows_to_be_rejected(
    tmp_path: Path,
) -> None:
    frames = _valid_bronze_frames()

    def append_row(table: str, **changes: object) -> None:
        row = frames[table].iloc[[0]].copy()
        for column, value in changes.items():
            row.at[row.index[0], column] = value
        frames[table] = pd.concat([frames[table], row], ignore_index=True)

    append_row("products", product_id=20, sku="BAD-20", unit_price=-1)
    append_row("inventory", product_id=20)
    append_row("orders", order_id=200, customer_id=2)
    append_row("order_items", order_item_id=2000, order_id=200, product_id=20)
    append_row("payments", payment_id=3000, order_id=200)

    for table, dataframe in frames.items():
        path = build_lake_path("bronze", "postgres", table, LOAD_DATE, tmp_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        dataframe.to_parquet(path, index=False)

    results = validate_bronze_quality(
        LOAD_DATE,
        data_root=tmp_path,
        tables=tuple(reversed(tuple(frames))),
        max_rejection_rate=1.0,
    )

    expected = {
        "customers": ("customer_id", 1, 2),
        "products": ("product_id", 10, 20),
        "inventory": ("product_id", 10, 20),
        "orders": ("order_id", 100, 200),
        "order_items": ("order_item_id", 1000, 2000),
        "payments": ("payment_id", 2000, 3000),
    }
    for table, (key, accepted_id, rejected_id) in expected.items():
        result = results[table]
        silver = pd.read_parquet(result.silver_path)
        rejected = pd.read_parquet(result.rejected_path)
        assert silver[key].tolist() == [accepted_id]
        assert rejected[key].tolist() == [rejected_id]
        assert result.rejected_rows == 1

    reasons = {
        table: pd.read_parquet(results[table].rejected_path).loc[0, "rejection_reason"]
        for table in ("inventory", "orders", "order_items", "payments")
    }
    assert "product_id does not exist in products" in reasons["inventory"]
    assert "customer_id does not exist in customers" in reasons["orders"]
    assert "order_id does not exist in orders" in reasons["order_items"]
    assert "product_id does not exist in products" in reasons["order_items"]
    assert "order_id does not exist in orders" in reasons["payments"]


def test_parse_args_parses_load_date() -> None:
    args = parse_args(["--load-date", "2026-08-21"])
    defaults = parse_args([])

    assert args.load_date == LOAD_DATE
    assert defaults.load_date is None
    assert not defaults.allow_empty
    assert defaults.max_rejection_rate == DEFAULT_MAX_REJECTION_RATE
    assert parse_args(["--allow-empty"]).allow_empty
    assert parse_args(["--max-rejection-rate", "0.25"]).max_rejection_rate == 0.25


def test_excessive_rejection_rate_blocks_silver_publication(
    tmp_path: Path,
) -> None:
    base_product = _valid_bronze_frames()["products"].iloc[[0]].copy()
    products = pd.concat([base_product] * 10, ignore_index=True)
    products["product_id"] = range(1, 11)
    products["sku"] = [f"SKU-{product_id}" for product_id in range(1, 11)]
    products.loc[:1, "unit_price"] = -1

    bronze_path = build_lake_path(
        "bronze", "postgres", "products", LOAD_DATE, tmp_path
    )
    bronze_path.parent.mkdir(parents=True, exist_ok=True)
    products.to_parquet(bronze_path, index=False)

    silver_path = build_lake_path(
        "silver", "postgres", "products", LOAD_DATE, tmp_path
    )
    silver_path.parent.mkdir(parents=True, exist_ok=True)
    previous_silver = pd.DataFrame({"previous_run": [42]})
    previous_silver.to_parquet(silver_path, index=False)

    with pytest.raises(
        ValueError,
        match="products rejection rate 20.00% exceeds the 10.00% limit",
    ):
        validate_bronze_quality(
            LOAD_DATE,
            data_root=tmp_path,
            tables=("products",),
            quality_run_id=QUALITY_RUN_ID,
            quality_checked_at=QUALITY_CHECKED_AT,
        )

    pd.testing.assert_frame_equal(pd.read_parquet(silver_path), previous_silver)
    rejected = pd.read_parquet(
        build_lake_path("rejected", "postgres", "products", LOAD_DATE, tmp_path)
    )
    assert len(rejected) == 2

    audit = pd.read_parquet(build_audit_path(tmp_path))
    assert audit.loc[0, "status"] == "failed"
    assert audit.loc[0, "valid_rows"] == 8
    assert audit.loc[0, "rejected_rows"] == 2
    assert audit.loc[0, "rejection_rate"] == pytest.approx(0.2)


@pytest.mark.parametrize("value", ["-0.1", "1.1", "nan", "not-a-number"])
def test_parse_args_rejects_invalid_rejection_rate(value: str) -> None:
    with pytest.raises(SystemExit):
        parse_args(["--max-rejection-rate", value])


def test_missing_bronze_partition_records_failed_audit(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        validate_bronze_quality(
            LOAD_DATE,
            data_root=tmp_path,
            tables=("customers",),
            quality_run_id=QUALITY_RUN_ID,
            quality_checked_at=QUALITY_CHECKED_AT,
        )

    audit = pd.read_parquet(build_audit_path(tmp_path))
    assert len(audit) == 1
    assert audit.loc[0, "table_name"] == "customers"
    assert audit.loc[0, "status"] == "failed"


@pytest.mark.parametrize("products_input", ["all_rejected", "empty_input"])
def test_empty_silver_table_blocks_publication_and_records_failure(
    tmp_path: Path, products_input: str
) -> None:
    frames = _valid_bronze_frames()
    if products_input == "all_rejected":
        frames["products"].loc[0, "unit_price"] = -1
    else:
        frames["products"] = frames["products"].iloc[0:0]

    for table, dataframe in frames.items():
        path = build_lake_path("bronze", "postgres", table, LOAD_DATE, tmp_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        dataframe.to_parquet(path, index=False)

    previous_silver = build_lake_path(
        "silver", "postgres", "customers", LOAD_DATE, tmp_path
    )
    previous_silver.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"previous_run": [42]}).to_parquet(previous_silver, index=False)

    with pytest.raises(ValueError, match="products has no valid rows"):
        validate_bronze_quality(
            LOAD_DATE, data_root=tmp_path, max_rejection_rate=1.0
        )

    pd.testing.assert_frame_equal(
        pd.read_parquet(previous_silver), pd.DataFrame({"previous_run": [42]})
    )
    assert not build_lake_path(
        "silver", "postgres", "products", LOAD_DATE, tmp_path
    ).exists()
    rejected = pd.read_parquet(
        build_lake_path("rejected", "postgres", "products", LOAD_DATE, tmp_path)
    )
    assert len(rejected) == (1 if products_input == "all_rejected" else 0)

    audit = pd.read_parquet(build_audit_path(tmp_path))
    assert len(audit) == 1
    assert audit.loc[0, "table_name"] == "products"
    assert audit.loc[0, "status"] == "failed"
    assert audit.loc[0, "rejected_rows"] == len(rejected)


def test_mixed_bronze_ingestions_block_silver_publication(tmp_path: Path) -> None:
    frames = _valid_bronze_frames()
    frames["customers"]["ingestion_id"] = "new-ingestion"
    frames["products"]["ingestion_id"] = "new-ingestion"
    for table, dataframe in frames.items():
        path = build_lake_path("bronze", "postgres", table, LOAD_DATE, tmp_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        dataframe.to_parquet(path, index=False)

    previous_silver = build_lake_path(
        "silver", "postgres", "customers", LOAD_DATE, tmp_path
    )
    previous_silver.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"previous_run": [42]}).to_parquet(previous_silver, index=False)

    with pytest.raises(ValueError, match="mixed ingestion_id"):
        validate_bronze_quality(LOAD_DATE, data_root=tmp_path)

    pd.testing.assert_frame_equal(
        pd.read_parquet(previous_silver), pd.DataFrame({"previous_run": [42]})
    )
    assert not build_lake_path(
        "silver", "postgres", "inventory", LOAD_DATE, tmp_path
    ).exists()
    audit = pd.read_parquet(build_audit_path(tmp_path))
    assert len(audit) == 1
    assert audit.loc[0, "table_name"] == "inventory"
    assert audit.loc[0, "status"] == "failed"


@pytest.mark.parametrize("invalid_metadata", ["missing", "mixed", "null"])
def test_bronze_requires_one_ingestion_id(
    tmp_path: Path, invalid_metadata: str
) -> None:
    customers = _valid_bronze_frames()["customers"]
    if invalid_metadata == "missing":
        customers = customers.drop(columns="ingestion_id")
    elif invalid_metadata == "mixed":
        customers.loc[1, "ingestion_id"] = "another-ingestion"
    else:
        customers.loc[1, "ingestion_id"] = None
    path = build_lake_path("bronze", "postgres", "customers", LOAD_DATE, tmp_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    customers.to_parquet(path, index=False)

    with pytest.raises(ValueError, match="bronze must have one ingestion_id"):
        validate_bronze_quality(
            LOAD_DATE, data_root=tmp_path, tables=("customers",)
        )

    assert not build_lake_path(
        "silver", "postgres", "customers", LOAD_DATE, tmp_path
    ).exists()
    audit = pd.read_parquet(build_audit_path(tmp_path))
    assert audit.loc[0, "table_name"] == "customers"
    assert audit.loc[0, "status"] == "failed"
