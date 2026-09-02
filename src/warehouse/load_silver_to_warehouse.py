"""Load a local silver partition into the PostgreSQL warehouse source schema."""

from __future__ import annotations

import argparse
import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import Engine

from src.utils.database import SOURCE_TABLE_LOAD_ORDER, get_engine
from src.utils.paths import DEFAULT_DATA_ROOT, build_lake_path, resolve_load_date


LOGGER = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_WAREHOUSE_SCHEMA_PATH = PROJECT_ROOT / "sql" / "warehouse_schema.sql"
SOURCE_SYSTEM = "postgres"
WAREHOUSE_SCHEMA = "warehouse_source"
SILVER_TABLES = SOURCE_TABLE_LOAD_ORDER

TableWriter = Callable[[pd.DataFrame, str, Any], None]


@dataclass(frozen=True)
class WarehouseLoadResult:
    """Summary for one silver table loaded into PostgreSQL."""

    table: str
    row_count: int
    silver_path: Path
    warehouse_relation: str


def read_silver_table(
    table: str,
    load_date: date | str,
    data_root: Path | str = DEFAULT_DATA_ROOT,
) -> tuple[pd.DataFrame, Path]:
    """Read one approved silver Parquet table and return its source path."""
    if table not in SILVER_TABLES:
        raise ValueError(f"Unsupported silver table: {table!r}")
    silver_path = build_lake_path(
        "silver", SOURCE_SYSTEM, table, load_date, data_root
    )
    return pd.read_parquet(silver_path), silver_path


def write_warehouse_table(
    dataframe: pd.DataFrame,
    table: str,
    connection: Any,
) -> None:
    """Replace one warehouse source table without writing the pandas index."""
    dataframe.to_sql(
        table,
        con=connection,
        schema=WAREHOUSE_SCHEMA,
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=1_000,
    )


def load_silver_to_warehouse(
    load_date: date | str | None = None,
    *,
    engine: Engine | Any | None = None,
    data_root: Path | str = DEFAULT_DATA_ROOT,
    tables: Sequence[str] = SILVER_TABLES,
    schema_path: Path | str = DEFAULT_WAREHOUSE_SCHEMA_PATH,
    table_writer: TableWriter | None = None,
) -> dict[str, WarehouseLoadResult]:
    """Replace warehouse source tables from one complete silver partition."""
    resolved_date = resolve_load_date(load_date)
    selected_tables = tuple(tables)
    unsupported = [table for table in selected_tables if table not in SILVER_TABLES]
    if unsupported:
        raise ValueError(f"Unsupported silver tables: {', '.join(unsupported)}")

    warehouse_schema_path = Path(schema_path)
    if not warehouse_schema_path.is_file():
        raise FileNotFoundError(
            f"Warehouse schema SQL not found: {warehouse_schema_path}"
        )

    silver_frames: dict[str, pd.DataFrame] = {}
    silver_paths: dict[str, Path] = {}
    for table in selected_tables:
        dataframe, silver_path = read_silver_table(
            table, resolved_date, data_root
        )
        silver_frames[table] = dataframe
        silver_paths[table] = silver_path

    database_engine = engine if engine is not None else get_engine()
    writer = table_writer if table_writer is not None else write_warehouse_table
    schema_sql = warehouse_schema_path.read_text(encoding="utf-8")
    results: dict[str, WarehouseLoadResult] = {}

    with database_engine.begin() as connection:
        connection.exec_driver_sql(schema_sql)
        for table in selected_tables:
            dataframe = silver_frames[table]
            connection.exec_driver_sql(
                f'DROP TABLE IF EXISTS "{WAREHOUSE_SCHEMA}"."{table}" CASCADE'
            )
            writer(dataframe, table, connection)
            result = WarehouseLoadResult(
                table=table,
                row_count=len(dataframe),
                silver_path=silver_paths[table],
                warehouse_relation=f"{WAREHOUSE_SCHEMA}.{table}",
            )
            results[table] = result
            LOGGER.info(
                "Loaded %s rows into %s",
                len(dataframe),
                result.warehouse_relation,
            )

    return results


def _parse_load_date(value: str) -> date:
    try:
        return resolve_load_date(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(str(error)) from error


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse the optional silver load-date partition."""
    parser = argparse.ArgumentParser(
        description="Load a RetailPulse silver partition into PostgreSQL."
    )
    parser.add_argument(
        "--load-date",
        type=_parse_load_date,
        default=None,
        help="UTC silver partition date in YYYY-MM-DD format; defaults to today",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    """CLI entry point for the silver-to-warehouse load."""
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    results = load_silver_to_warehouse(load_date=args.load_date)
    LOGGER.info(
        "Warehouse load completed: %s tables, %s total rows, load_date=%s",
        len(results),
        sum(result.row_count for result in results.values()),
        resolve_load_date(args.load_date).isoformat(),
    )


if __name__ == "__main__":
    main()
