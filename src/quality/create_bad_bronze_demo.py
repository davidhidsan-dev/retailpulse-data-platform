"""Create a deterministic bronze partition with controlled quality anomalies."""

from __future__ import annotations

import argparse
import logging
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pandas as pd

from src.utils.database import SOURCE_TABLE_LOAD_ORDER
from src.utils.paths import DEFAULT_DATA_ROOT, build_lake_path, resolve_load_date


LOGGER = logging.getLogger(__name__)
SOURCE_SYSTEM = "postgres"
SOURCE_TABLES = SOURCE_TABLE_LOAD_ORDER
DEFAULT_DEMO_LOAD_DATE = date(2099, 1, 1)
DEMO_INVALID_ID = 999_999_999

DEMO_ANOMALIES = {
    "customers": "email='invalid-email'",
    "products": "unit_price=-10",
    "inventory": "stock_quantity=-5",
    "orders": f"customer_id={DEMO_INVALID_ID}",
    "order_items": "line_total differs from quantity * unit_price",
    "payments": "payment_status='unknown_status'",
}


@dataclass(frozen=True)
class DemoTableResult:
    """Summary for one bronze table copied into the demo partition."""

    table: str
    row_count: int
    source_path: Path
    demo_path: Path
    anomaly: str


def inject_demo_anomaly(table: str, dataframe: pd.DataFrame) -> pd.DataFrame:
    """Return a copy with one documented anomaly in a deterministic row."""
    if table not in DEMO_ANOMALIES:
        raise ValueError(f"Unsupported source table: {table!r}")
    if dataframe.empty:
        raise ValueError(f"Cannot create a demo anomaly in empty table: {table}")

    demo = dataframe.copy()
    first_row = demo.index[0]

    if table == "customers":
        demo.at[first_row, "email"] = "invalid-email"
    elif table == "products":
        demo.at[first_row, "unit_price"] = -10.0
    elif table == "inventory":
        demo.at[first_row, "stock_quantity"] = -5
    elif table == "orders":
        demo.at[first_row, "customer_id"] = DEMO_INVALID_ID
    elif table == "order_items":
        quantity = pd.to_numeric(
            pd.Series([demo.at[first_row, "quantity"]]), errors="coerce"
        ).iloc[0]
        unit_price = pd.to_numeric(
            pd.Series([demo.at[first_row, "unit_price"]]), errors="coerce"
        ).iloc[0]
        if pd.isna(quantity) or pd.isna(unit_price):
            raise ValueError("Cannot derive a demo line_total from non-numeric values")
        demo.at[first_row, "line_total"] = round(float(quantity * unit_price), 2) + 1.0
    elif table == "payments":
        demo.at[first_row, "payment_status"] = "unknown_status"

    return demo


def create_bad_bronze_demo(
    source_load_date: date | str,
    demo_load_date: date | str = DEFAULT_DEMO_LOAD_DATE,
    *,
    data_root: Path | str = DEFAULT_DATA_ROOT,
    tables: Sequence[str] = SOURCE_TABLES,
) -> dict[str, DemoTableResult]:
    """Copy bronze tables to a new partition and inject controlled anomalies."""
    resolved_source_date = resolve_load_date(source_load_date)
    resolved_demo_date = resolve_load_date(demo_load_date)
    if resolved_source_date == resolved_demo_date:
        raise ValueError("source_load_date and demo_load_date must be different")

    selected_tables = tuple(tables)
    unsupported = [table for table in selected_tables if table not in SOURCE_TABLES]
    if unsupported:
        raise ValueError(f"Unsupported source tables: {', '.join(unsupported)}")

    source_frames: dict[str, pd.DataFrame] = {}
    source_paths: dict[str, Path] = {}
    for table in selected_tables:
        source_path = build_lake_path(
            "bronze", SOURCE_SYSTEM, table, resolved_source_date, data_root
        )
        source_paths[table] = source_path
        source_frames[table] = pd.read_parquet(source_path)

    if "customers" in source_frames:
        customer_ids = source_frames["customers"]["customer_id"].dropna()
        if customer_ids.eq(DEMO_INVALID_ID).any():
            raise ValueError(f"Demo invalid ID already exists in customers: {DEMO_INVALID_ID}")

    results: dict[str, DemoTableResult] = {}
    for table in selected_tables:
        demo = inject_demo_anomaly(table, source_frames[table])
        demo_path = build_lake_path(
            "bronze", SOURCE_SYSTEM, table, resolved_demo_date, data_root
        )
        demo_path.parent.mkdir(parents=True, exist_ok=True)
        demo.to_parquet(demo_path, index=False)
        results[table] = DemoTableResult(
            table=table,
            row_count=len(demo),
            source_path=source_paths[table],
            demo_path=demo_path,
            anomaly=DEMO_ANOMALIES[table],
        )
        LOGGER.info(
            "Created demo bronze %s: rows=%s, anomaly=%s, path=%s",
            table,
            len(demo),
            DEMO_ANOMALIES[table],
            demo_path,
        )

    return results


def _parse_load_date(value: str) -> date:
    try:
        return resolve_load_date(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(str(error)) from error


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse source and destination load-date partitions."""
    parser = argparse.ArgumentParser(
        description="Create a bronze demo partition with deterministic anomalies."
    )
    parser.add_argument(
        "--source-load-date",
        type=_parse_load_date,
        required=True,
        help="Existing bronze partition date in YYYY-MM-DD format",
    )
    parser.add_argument(
        "--demo-load-date",
        type=_parse_load_date,
        default=DEFAULT_DEMO_LOAD_DATE,
        help="Demo partition date; defaults to 2099-01-01",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    """CLI entry point for controlled bronze demo generation."""
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    results = create_bad_bronze_demo(
        source_load_date=args.source_load_date,
        demo_load_date=args.demo_load_date,
    )
    LOGGER.info(
        "Demo bronze completed: %s tables, %s rows, demo_load_date=%s",
        len(results),
        sum(result.row_count for result in results.values()),
        args.demo_load_date.isoformat(),
    )


if __name__ == "__main__":
    main()
