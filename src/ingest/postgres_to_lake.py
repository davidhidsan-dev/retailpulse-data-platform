"""Extract PostgreSQL source tables into local raw and bronze layers."""

from __future__ import annotations

import argparse
import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any
from uuid import uuid4

import pandas as pd
from sqlalchemy import Engine

from src.utils.database import SOURCE_TABLE_LOAD_ORDER, get_engine
from src.utils.paths import DEFAULT_DATA_ROOT, build_lake_path, resolve_load_date


LOGGER = logging.getLogger(__name__)
SOURCE_SYSTEM = "postgres"
SOURCE_TABLES = SOURCE_TABLE_LOAD_ORDER
BRONZE_METADATA_COLUMNS = (
    "ingestion_id",
    "ingested_at",
    "source_system",
    "source_table",
)

TableReader = Callable[[Any, str], pd.DataFrame]


@dataclass(frozen=True)
class IngestionResult:
    """Summary of one table written during a lake ingestion."""

    table: str
    row_count: int
    raw_path: Path
    bronze_path: Path
    ingestion_id: str
    ingested_at: pd.Timestamp


def extract_table(engine: Engine, table: str) -> pd.DataFrame:
    """Read a complete approved source table from PostgreSQL."""
    if table not in SOURCE_TABLES:
        raise ValueError(f"Unsupported source table: {table!r}")
    return pd.read_sql_table(table, con=engine)


def write_raw_table(
    dataframe: pd.DataFrame,
    table: str,
    load_date: date | str,
    data_root: Path | str = DEFAULT_DATA_ROOT,
) -> Path:
    """Write an unchanged source snapshot as CSV without a pandas index."""
    output_path = build_lake_path(
        "raw", SOURCE_SYSTEM, table, load_date, data_root
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    dataframe.to_csv(output_path, index=False)
    return output_path


def add_bronze_metadata(
    dataframe: pd.DataFrame,
    table: str,
    ingestion_id: str,
    ingested_at: pd.Timestamp | str,
) -> pd.DataFrame:
    """Return a copy enriched with the four bronze technical columns."""
    collisions = set(BRONZE_METADATA_COLUMNS).intersection(dataframe.columns)
    if collisions:
        names = ", ".join(sorted(collisions))
        raise ValueError(f"Source table already contains bronze columns: {names}")

    timestamp = pd.Timestamp(ingested_at)
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize("UTC")
    else:
        timestamp = timestamp.tz_convert("UTC")

    bronze = dataframe.copy()
    bronze["ingestion_id"] = ingestion_id
    bronze["ingested_at"] = timestamp
    bronze["source_system"] = SOURCE_SYSTEM
    bronze["source_table"] = table
    return bronze


def write_bronze_table(
    dataframe: pd.DataFrame,
    table: str,
    load_date: date | str,
    ingestion_id: str,
    ingested_at: pd.Timestamp | str,
    data_root: Path | str = DEFAULT_DATA_ROOT,
) -> Path:
    """Enrich a source snapshot and write it as Parquet without an index."""
    output_path = build_lake_path(
        "bronze", SOURCE_SYSTEM, table, load_date, data_root
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    bronze = add_bronze_metadata(
        dataframe, table, ingestion_id, ingested_at
    )
    bronze.to_parquet(output_path, index=False)
    return output_path


def ingest_postgres_to_lake(
    load_date: date | str | None = None,
    *,
    engine: Engine | None = None,
    data_root: Path | str = DEFAULT_DATA_ROOT,
    tables: Sequence[str] = SOURCE_TABLES,
    table_reader: TableReader | None = None,
) -> dict[str, IngestionResult]:
    """Extract approved PostgreSQL tables into raw and bronze snapshots."""
    resolved_date = resolve_load_date(load_date)
    selected_tables = tuple(tables)
    unsupported = [table for table in selected_tables if table not in SOURCE_TABLES]
    if unsupported:
        raise ValueError(f"Unsupported source tables: {', '.join(unsupported)}")

    database_engine = engine if engine is not None else get_engine()
    reader = table_reader if table_reader is not None else extract_table
    ingestion_id = str(uuid4())
    ingested_at = pd.Timestamp.now(tz="UTC")
    results: dict[str, IngestionResult] = {}

    for table in selected_tables:
        dataframe = reader(database_engine, table)
        raw_path = write_raw_table(
            dataframe, table, resolved_date, data_root
        )
        bronze_path = write_bronze_table(
            dataframe,
            table,
            resolved_date,
            ingestion_id,
            ingested_at,
            data_root,
        )
        results[table] = IngestionResult(
            table=table,
            row_count=len(dataframe),
            raw_path=raw_path,
            bronze_path=bronze_path,
            ingestion_id=ingestion_id,
            ingested_at=ingested_at,
        )
        LOGGER.info(
            "Extracted %s rows from %s; raw=%s; bronze=%s",
            len(dataframe),
            table,
            raw_path,
            bronze_path,
        )

    return results


def _parse_load_date(value: str) -> date:
    try:
        return resolve_load_date(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(str(error)) from error


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse the optional UTC load-date partition."""
    parser = argparse.ArgumentParser(
        description="Extract PostgreSQL source tables into raw and bronze."
    )
    parser.add_argument(
        "--load-date",
        type=_parse_load_date,
        default=None,
        help="UTC partition date in YYYY-MM-DD format; defaults to today",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    """CLI entry point for the PostgreSQL-to-lake ingestion."""
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    results = ingest_postgres_to_lake(load_date=args.load_date)
    total_rows = sum(result.row_count for result in results.values())
    LOGGER.info(
        "Ingestion completed: %s tables, %s total rows, ingestion_id=%s",
        len(results),
        total_rows,
        next(iter(results.values())).ingestion_id if results else "n/a",
    )


if __name__ == "__main__":
    main()
