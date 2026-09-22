"""Validate bronze snapshots and write silver, rejected and audit datasets."""

from __future__ import annotations

import argparse
import logging
import math
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Literal
from uuid import uuid4

import pandas as pd

from src.quality.rules import (
    blank_values,
    duplicate_values,
    fractional_values,
    invalid_datetimes,
    invalid_emails,
    invalid_foreign_keys,
    invalid_line_totals,
    negative_values,
    non_positive_values,
    null_values,
    values_not_in,
)
from src.utils.database import SOURCE_TABLE_LOAD_ORDER
from src.utils.paths import (
    DEFAULT_DATA_ROOT,
    build_audit_path,
    build_lake_path,
    resolve_load_date,
)


LOGGER = logging.getLogger(__name__)
SOURCE_SYSTEM = "postgres"
SOURCE_TABLES = SOURCE_TABLE_LOAD_ORDER
QUALITY_METADATA_COLUMNS = ("quality_run_id", "quality_checked_at")
DEFAULT_MAX_REJECTION_RATE = 0.10

CUSTOMER_SEGMENTS = {"high_value", "frequent", "occasional", "inactive", "new"}
ORDER_STATUSES = {"completed", "cancelled", "refunded", "pending"}
PAYMENT_METHODS = {"credit_card", "paypal", "bank_transfer", "gift_card"}
PAYMENT_STATUSES = {"paid", "failed", "refunded", "pending"}

REQUIRED_COLUMNS = {
    "customers": {
        "customer_id",
        "first_name",
        "last_name",
        "email",
        "country",
        "city",
        "created_at",
        "synthetic_behavior_segment",
    },
    "products": {
        "product_id",
        "sku",
        "product_name",
        "category",
        "unit_price",
        "created_at",
    },
    "inventory": {"product_id", "stock_quantity", "reorder_level", "updated_at"},
    "orders": {
        "order_id",
        "customer_id",
        "order_date",
        "order_status",
        "country",
    },
    "order_items": {
        "order_item_id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        "line_total",
    },
    "payments": {
        "payment_id",
        "order_id",
        "payment_method",
        "payment_status",
        "payment_amount",
        "payment_date",
    },
}


@dataclass(frozen=True)
class QualityResult:
    """Summary and output paths for one validated table."""

    table: str
    input_rows: int
    valid_rows: int
    rejected_rows: int
    rejection_rate: float
    status: str
    silver_path: Path
    rejected_path: Path
    quality_run_id: str
    quality_checked_at: pd.Timestamp


def _utc_timestamp(value: pd.Timestamp | str | None = None) -> pd.Timestamp:
    timestamp = pd.Timestamp.now(tz="UTC") if value is None else pd.Timestamp(value)
    if timestamp.tzinfo is None:
        return timestamp.tz_localize("UTC")
    return timestamp.tz_convert("UTC")


def _validate_max_rejection_rate(value: float) -> float:
    rate = float(value)
    if not math.isfinite(rate) or not 0 <= rate <= 1:
        raise ValueError("max_rejection_rate must be between 0 and 1")
    return rate


def _ensure_columns(dataframe: pd.DataFrame, table: str) -> None:
    if table not in REQUIRED_COLUMNS:
        raise ValueError(f"Unsupported source table: {table!r}")
    missing = REQUIRED_COLUMNS[table].difference(dataframe.columns)
    if missing:
        raise ValueError(f"{table} is missing columns: {', '.join(sorted(missing))}")


def _require_reference(
    reference_frames: dict[str, pd.DataFrame], table: str, column: str
) -> pd.DataFrame:
    if table not in reference_frames:
        raise ValueError(f"Missing reference DataFrame: {table}")
    if column not in reference_frames[table].columns:
        raise ValueError(f"{table} is missing reference column: {column}")
    return reference_frames[table]


def validate_table(
    table: str,
    dataframe: pd.DataFrame,
    reference_frames: dict[str, pd.DataFrame] | None = None,
    *,
    quality_run_id: str | None = None,
    quality_checked_at: pd.Timestamp | str | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split one bronze DataFrame into valid and rejected records."""
    _ensure_columns(dataframe, table)
    collisions = set(QUALITY_METADATA_COLUMNS).intersection(dataframe.columns)
    collisions.update({"rejection_reason"}.intersection(dataframe.columns))
    if collisions:
        raise ValueError(
            "Bronze data already contains quality columns: "
            + ", ".join(sorted(collisions))
        )

    working = dataframe.copy().reset_index(drop=True)
    references = reference_frames or {}
    reasons: list[list[str]] = [[] for _ in range(len(working))]

    def reject(mask: pd.Series, reason: str) -> None:
        for position, failed in enumerate(mask.fillna(False).astype(bool).tolist()):
            if failed:
                reasons[position].append(reason)

    if table == "customers":
        reject(null_values(working, "customer_id"), "customer_id is null")
        reject(duplicate_values(working, "customer_id"), "customer_id is duplicated")
        for column in ("first_name", "last_name", "city"):
            reject(null_values(working, column), f"{column} is null")
            reject(blank_values(working, column), f"{column} is blank")
        reject(null_values(working, "email"), "email is null")
        reject(blank_values(working, "email"), "email is blank")
        reject(invalid_emails(working), "email has invalid format")
        reject(duplicate_values(working, "email"), "email is duplicated")
        reject(null_values(working, "country"), "country is null")
        reject(blank_values(working, "country"), "country is blank")
        reject(null_values(working, "created_at"), "created_at is null")
        reject(
            invalid_datetimes(working, "created_at"),
            "created_at is not a valid datetime",
        )
        reject(
            values_not_in(working, "synthetic_behavior_segment", CUSTOMER_SEGMENTS),
            "synthetic_behavior_segment is not allowed",
        )
    elif table == "products":
        reject(null_values(working, "product_id"), "product_id is null")
        reject(duplicate_values(working, "product_id"), "product_id is duplicated")
        reject(null_values(working, "sku"), "sku is null")
        reject(blank_values(working, "sku"), "sku is blank")
        reject(duplicate_values(working, "sku"), "sku is duplicated")
        reject(null_values(working, "product_name"), "product_name is null")
        reject(blank_values(working, "product_name"), "product_name is blank")
        reject(null_values(working, "category"), "category is null")
        reject(blank_values(working, "category"), "category is blank")
        reject(non_positive_values(working, "unit_price"), "unit_price must be > 0")
        reject(null_values(working, "created_at"), "created_at is null")
        reject(
            invalid_datetimes(working, "created_at"),
            "created_at is not a valid datetime",
        )
    elif table == "inventory":
        reject(null_values(working, "product_id"), "product_id is null")
        reject(duplicate_values(working, "product_id"), "product_id is duplicated")
        reject(negative_values(working, "stock_quantity"), "stock_quantity must be >= 0")
        reject(
            fractional_values(working, "stock_quantity"),
            "stock_quantity must be an integer",
        )
        reject(negative_values(working, "reorder_level"), "reorder_level must be >= 0")
        reject(
            fractional_values(working, "reorder_level"),
            "reorder_level must be an integer",
        )
        reject(null_values(working, "updated_at"), "updated_at is null")
        reject(
            invalid_datetimes(working, "updated_at"),
            "updated_at is not a valid datetime",
        )
        products = _require_reference(references, "products", "product_id")
        reject(
            invalid_foreign_keys(working, "product_id", products, "product_id"),
            "product_id does not exist in products",
        )
    elif table == "orders":
        reject(null_values(working, "order_id"), "order_id is null")
        reject(duplicate_values(working, "order_id"), "order_id is duplicated")
        reject(null_values(working, "customer_id"), "customer_id is null")
        reject(null_values(working, "order_date"), "order_date is null")
        reject(
            invalid_datetimes(working, "order_date"),
            "order_date is not a valid datetime",
        )
        reject(
            values_not_in(working, "order_status", ORDER_STATUSES),
            "order_status is not allowed",
        )
        reject(null_values(working, "country"), "country is null")
        reject(blank_values(working, "country"), "country is blank")
        customers = _require_reference(references, "customers", "customer_id")
        reject(
            invalid_foreign_keys(working, "customer_id", customers, "customer_id"),
            "customer_id does not exist in customers",
        )
    elif table == "order_items":
        reject(null_values(working, "order_item_id"), "order_item_id is null")
        reject(
            duplicate_values(working, "order_item_id"),
            "order_item_id is duplicated",
        )
        reject(null_values(working, "order_id"), "order_id is null")
        reject(null_values(working, "product_id"), "product_id is null")
        reject(non_positive_values(working, "quantity"), "quantity must be > 0")
        reject(fractional_values(working, "quantity"), "quantity must be an integer")
        reject(non_positive_values(working, "unit_price"), "unit_price must be > 0")
        reject(invalid_line_totals(working), "line_total does not match quantity * unit_price")
        orders = _require_reference(references, "orders", "order_id")
        products = _require_reference(references, "products", "product_id")
        reject(
            invalid_foreign_keys(working, "order_id", orders, "order_id"),
            "order_id does not exist in orders",
        )
        reject(
            invalid_foreign_keys(working, "product_id", products, "product_id"),
            "product_id does not exist in products",
        )
    elif table == "payments":
        reject(null_values(working, "payment_id"), "payment_id is null")
        reject(duplicate_values(working, "payment_id"), "payment_id is duplicated")
        reject(null_values(working, "order_id"), "order_id is null")
        reject(duplicate_values(working, "order_id"), "order_id is duplicated")
        reject(
            values_not_in(working, "payment_method", PAYMENT_METHODS),
            "payment_method is not allowed",
        )
        reject(
            values_not_in(working, "payment_status", PAYMENT_STATUSES),
            "payment_status is not allowed",
        )
        reject(
            negative_values(working, "payment_amount"),
            "payment_amount must be >= 0",
        )
        reject(null_values(working, "payment_date"), "payment_date is null")
        reject(
            invalid_datetimes(working, "payment_date"),
            "payment_date is not a valid datetime",
        )
        orders = _require_reference(references, "orders", "order_id")
        reject(
            invalid_foreign_keys(working, "order_id", orders, "order_id"),
            "order_id does not exist in orders",
        )

    run_id = quality_run_id or str(uuid4())
    checked_at = _utc_timestamp(quality_checked_at)
    rejection_reason = pd.Series(
        ["; ".join(row_reasons) for row_reasons in reasons], dtype="string"
    )
    rejected_mask = rejection_reason.str.len().gt(0)

    valid = working.loc[~rejected_mask].copy()
    rejected = working.loc[rejected_mask].copy()
    valid["quality_run_id"] = run_id
    valid["quality_checked_at"] = checked_at
    rejected["quality_run_id"] = run_id
    rejected["quality_checked_at"] = checked_at
    rejected["rejection_reason"] = rejection_reason.loc[rejected_mask].to_numpy()
    return valid.reset_index(drop=True), rejected.reset_index(drop=True)


def write_quality_table(
    dataframe: pd.DataFrame,
    layer: Literal["silver", "rejected"],
    table: str,
    load_date: date | str,
    data_root: Path | str = DEFAULT_DATA_ROOT,
) -> Path:
    """Write a silver or rejected DataFrame to its partitioned Parquet path."""
    if layer not in {"silver", "rejected"}:
        raise ValueError(f"Unsupported quality layer: {layer!r}")
    output_path = build_lake_path(layer, SOURCE_SYSTEM, table, load_date, data_root)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    dataframe.to_parquet(output_path, index=False)
    return output_path


def append_quality_audit(
    records: list[dict[str, object]],
    data_root: Path | str = DEFAULT_DATA_ROOT,
) -> Path:
    """Append table summaries to the shared Parquet audit dataset."""
    audit_path = build_audit_path(data_root)
    audit_path.parent.mkdir(parents=True, exist_ok=True)
    current = pd.read_parquet(audit_path) if audit_path.exists() else pd.DataFrame()
    updated = pd.concat([current, pd.DataFrame.from_records(records)], ignore_index=True)
    updated.to_parquet(audit_path, index=False)
    return audit_path


def validate_bronze_quality(
    load_date: date | str | None = None,
    *,
    data_root: Path | str = DEFAULT_DATA_ROOT,
    tables: Sequence[str] = SOURCE_TABLES,
    quality_run_id: str | None = None,
    quality_checked_at: pd.Timestamp | str | None = None,
    allow_empty: bool = False,
    max_rejection_rate: float = DEFAULT_MAX_REJECTION_RATE,
) -> dict[str, QualityResult]:
    """Validate bronze and enforce publication thresholds for silver tables."""
    resolved_date = resolve_load_date(load_date)
    rejection_limit = _validate_max_rejection_rate(max_rejection_rate)
    selected_tables = tuple(tables)
    unsupported = [table for table in selected_tables if table not in SOURCE_TABLES]
    if unsupported:
        raise ValueError(f"Unsupported source tables: {', '.join(unsupported)}")

    run_id = quality_run_id or str(uuid4())
    checked_at = _utc_timestamp(quality_checked_at)
    bronze_frames: dict[str, pd.DataFrame] = {}
    for table in selected_tables:
        bronze_path = build_lake_path(
            "bronze", SOURCE_SYSTEM, table, resolved_date, data_root
        )
        try:
            bronze_frames[table] = pd.read_parquet(bronze_path)
        except Exception:
            append_quality_audit(
                [
                    {
                        "quality_run_id": run_id,
                        "quality_checked_at": checked_at,
                        "load_date": resolved_date.isoformat(),
                        "source_system": SOURCE_SYSTEM,
                        "table_name": table,
                        "input_rows": 0,
                        "valid_rows": 0,
                        "rejected_rows": 0,
                        "rejection_rate": 0.0,
                        "status": "failed",
                    }
                ],
                data_root,
            )
            raise

    validation_order = tuple(
        table for table in SOURCE_TABLES if table in selected_tables
    )
    accepted_frames: dict[str, pd.DataFrame] = {}
    validated_frames: dict[str, tuple[pd.DataFrame, pd.DataFrame]] = {}
    results: dict[str, QualityResult] = {}
    audit_records: list[dict[str, object]] = []
    try:
        ingestion_id: str | None = None
        for table in selected_tables:
            source = bronze_frames[table]
            if source.empty:
                continue
            if (
                "ingestion_id" not in source.columns
                or source["ingestion_id"].isna().any()
                or source["ingestion_id"].nunique() != 1
            ):
                raise ValueError(f"{table} bronze must have one ingestion_id")
            table_ingestion_id = source["ingestion_id"].iloc[0]
            if not isinstance(table_ingestion_id, str) or not table_ingestion_id:
                raise ValueError(f"{table} bronze must have one ingestion_id")
            if ingestion_id is None:
                ingestion_id = table_ingestion_id
            elif table_ingestion_id != ingestion_id:
                raise ValueError(
                    f"Bronze tables contain mixed ingestion_id values; {table} differs"
                )

        for table in validation_order:
            source = bronze_frames[table]
            valid, rejected = validate_table(
                table,
                source,
                accepted_frames,
                quality_run_id=run_id,
                quality_checked_at=checked_at,
            )
            validated_frames[table] = (valid, rejected)
            if valid.empty and not allow_empty:
                write_quality_table(
                    rejected, "rejected", table, resolved_date, data_root
                )
                raise ValueError(
                    f"{table} has no valid rows; silver was not published."
                )
            rejection_rate = len(rejected) / len(source) if len(source) else 0.0
            if rejection_rate > rejection_limit:
                write_quality_table(
                    rejected, "rejected", table, resolved_date, data_root
                )
                raise ValueError(
                    f"{table} rejection rate {rejection_rate:.2%} exceeds "
                    f"the {rejection_limit:.2%} limit; silver was not published."
                )
            accepted_frames[table] = valid

        for table in selected_tables:
            source = bronze_frames[table]
            valid, rejected = validated_frames[table]
            rejection_rate = len(rejected) / len(source) if len(source) else 0.0
            silver_path = write_quality_table(
                valid, "silver", table, resolved_date, data_root
            )
            rejected_path = write_quality_table(
                rejected, "rejected", table, resolved_date, data_root
            )
            status = "passed" if rejected.empty else "warning"
            result = QualityResult(
                table=table,
                input_rows=len(source),
                valid_rows=len(valid),
                rejected_rows=len(rejected),
                rejection_rate=rejection_rate,
                status=status,
                silver_path=silver_path,
                rejected_path=rejected_path,
                quality_run_id=run_id,
                quality_checked_at=checked_at,
            )
            results[table] = result
            audit_records.append(
                {
                    "quality_run_id": run_id,
                    "quality_checked_at": checked_at,
                    "load_date": resolved_date.isoformat(),
                    "source_system": SOURCE_SYSTEM,
                    "table_name": table,
                    "input_rows": len(source),
                    "valid_rows": len(valid),
                    "rejected_rows": len(rejected),
                    "rejection_rate": rejection_rate,
                    "status": status,
                }
            )
            LOGGER.info(
                "Validated %s: input=%s, valid=%s, rejected=%s, status=%s",
                table,
                len(source),
                len(valid),
                len(rejected),
                status,
            )
    except Exception:
        failed_table = table
        failed_frames = validated_frames.get(failed_table)
        failed_input_rows = len(bronze_frames[failed_table])
        failed_valid_rows = len(failed_frames[0]) if failed_frames else 0
        failed_rejected_rows = len(failed_frames[1]) if failed_frames else 0
        audit_records.append(
            {
                "quality_run_id": run_id,
                "quality_checked_at": checked_at,
                "load_date": resolved_date.isoformat(),
                "source_system": SOURCE_SYSTEM,
                "table_name": failed_table,
                "input_rows": failed_input_rows,
                "valid_rows": failed_valid_rows,
                "rejected_rows": failed_rejected_rows,
                "rejection_rate": (
                    failed_rejected_rows / failed_input_rows
                    if failed_input_rows
                    else 0.0
                ),
                "status": "failed",
            }
        )
        append_quality_audit(audit_records, data_root)
        raise

    audit_path = append_quality_audit(audit_records, data_root)
    LOGGER.info("Quality audit updated: %s", audit_path)
    return results


def _parse_load_date(value: str) -> date:
    try:
        return resolve_load_date(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(str(error)) from error


def _parse_rejection_rate(value: str) -> float:
    try:
        return _validate_max_rejection_rate(float(value))
    except ValueError as error:
        raise argparse.ArgumentTypeError(str(error)) from error


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse the optional bronze load-date partition."""
    parser = argparse.ArgumentParser(
        description="Validate bronze snapshots into silver and rejected datasets."
    )
    parser.add_argument(
        "--load-date",
        type=_parse_load_date,
        default=None,
        help="UTC partition date in YYYY-MM-DD format; defaults to today",
    )
    parser.add_argument(
        "--allow-empty",
        action="store_true",
        help="Allow empty silver tables for controlled quality demonstrations",
    )
    parser.add_argument(
        "--max-rejection-rate",
        type=_parse_rejection_rate,
        default=DEFAULT_MAX_REJECTION_RATE,
        help="maximum rejected-row ratio allowed per table before failure",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    """CLI entry point for bronze data-quality validation."""
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    results = validate_bronze_quality(
        load_date=args.load_date,
        allow_empty=args.allow_empty,
        max_rejection_rate=args.max_rejection_rate,
    )
    LOGGER.info(
        "Quality run completed: %s tables, %s valid rows, %s rejected rows, "
        "quality_run_id=%s",
        len(results),
        sum(result.valid_rows for result in results.values()),
        sum(result.rejected_rows for result in results.values()),
        next(iter(results.values())).quality_run_id if results else "n/a",
    )


if __name__ == "__main__":
    main()
