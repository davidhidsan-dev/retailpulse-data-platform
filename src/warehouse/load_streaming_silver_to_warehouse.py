"""Incrementally load approved Kafka event silver into PostgreSQL."""

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

from src.streaming.raw_to_bronze import (
    BRONZE_METADATA_COLUMNS,
    EVENT_COLUMNS,
    SOURCE_SYSTEM,
    SOURCE_TABLE,
    build_bronze_event_path,
    resolve_event_date,
)
from src.streaming.validate_web_events import (
    QUALITY_METADATA_COLUMNS,
    build_event_quality_path,
    build_streaming_audit_path,
)
from src.utils.database import get_engine
from src.utils.paths import DEFAULT_DATA_ROOT
from src.warehouse.load_silver_to_warehouse import write_warehouse_table


LOGGER = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_WAREHOUSE_SCHEMA_PATH = PROJECT_ROOT / "sql" / "warehouse_schema.sql"
WAREHOUSE_SCHEMA = "warehouse_source"
WAREHOUSE_TABLE = "web_events"
WAREHOUSE_COLUMNS = EVENT_COLUMNS + BRONZE_METADATA_COLUMNS + QUALITY_METADATA_COLUMNS

TableWriter = Callable[[pd.DataFrame, str, Any], None]


@dataclass(frozen=True)
class StreamingWarehouseLoadResult:
    """Summary for one approved event run loaded into PostgreSQL."""

    event_date: date
    run_id: str
    input_rows: int
    inserted_rows: int
    duplicate_rows: int
    silver_path: Path
    warehouse_relation: str
    quality_run_id: str


def _quote_identifier(identifier: str) -> str:
    escaped = identifier.replace('"', '""')
    return f'"{escaped}"'


def _run_id_from_silver_path(path: Path) -> str:
    return path.stem.removeprefix("web_events_")


def discover_streaming_silver_file(
    event_date: date | str,
    *,
    data_root: Path | str = DEFAULT_DATA_ROOT,
    run_id: str | None = None,
) -> tuple[str, Path]:
    """Resolve exactly one event silver file for loading."""
    resolved_date = resolve_event_date(event_date)
    if run_id is not None:
        path = build_event_quality_path(
            "silver", resolved_date, run_id, data_root
        )
        if not path.is_file():
            raise FileNotFoundError(f"Streaming silver file not found: {path}")
        return run_id, path

    partition = (
        Path(data_root)
        / "silver"
        / SOURCE_SYSTEM
        / SOURCE_TABLE
        / f"event_date={resolved_date.isoformat()}"
    )
    paths = tuple(sorted(partition.glob("web_events_*.parquet")))
    if not paths:
        raise FileNotFoundError(
            f"No streaming silver files found for event_date={resolved_date}"
        )
    if len(paths) != 1:
        raise ValueError(
            "More than one streaming silver run exists; provide --run-id"
        )
    return _run_id_from_silver_path(paths[0]), paths[0]


def require_approved_event_quality_run(
    event_date: date,
    run_id: str,
    *,
    data_root: Path | str = DEFAULT_DATA_ROOT,
) -> pd.Series:
    """Return the latest successful audit record for one event run."""
    audit_path = build_streaming_audit_path(data_root)
    if not audit_path.is_file():
        raise ValueError(
            f"No streaming quality audit found for event_date={event_date} "
            f"and run_id={run_id}"
        )

    audit = pd.read_parquet(audit_path)
    required = {
        "quality_run_id",
        "quality_checked_at",
        "event_date",
        "run_id",
        "ingestion_id",
        "source_system",
        "source_table",
        "valid_rows",
        "status",
    }
    missing = required.difference(audit.columns)
    if missing:
        raise ValueError(
            "Streaming quality audit is missing columns: "
            + ", ".join(sorted(missing))
        )

    checked_at = pd.to_datetime(
        audit["quality_checked_at"], errors="coerce", utc=True
    )
    event_dates = pd.to_datetime(audit["event_date"], errors="coerce").dt.date
    selected = audit.loc[
        event_dates.eq(event_date)
        & audit["run_id"].eq(run_id)
        & audit["source_system"].eq(SOURCE_SYSTEM)
        & audit["source_table"].eq(SOURCE_TABLE)
    ].copy()
    selected["_checked_at"] = checked_at.loc[selected.index]
    if selected.empty:
        raise ValueError(
            f"No streaming quality audit found for event_date={event_date} "
            f"and run_id={run_id}"
        )
    if selected["_checked_at"].isna().any():
        raise ValueError("Streaming quality audit has invalid quality_checked_at")

    latest = selected.sort_values("_checked_at").iloc[-1]
    if latest["status"] not in {"passed", "warning"}:
        raise ValueError(
            f"Latest streaming quality run did not approve run_id={run_id}"
        )
    if not isinstance(latest["quality_run_id"], str) or not latest[
        "quality_run_id"
    ]:
        raise ValueError("Latest streaming quality audit has no quality_run_id")
    if not isinstance(latest["ingestion_id"], str) or not latest["ingestion_id"]:
        raise ValueError("Latest streaming quality audit has no ingestion_id")
    return latest


def read_approved_streaming_silver(
    event_date: date | str,
    *,
    data_root: Path | str = DEFAULT_DATA_ROOT,
    run_id: str | None = None,
) -> tuple[pd.DataFrame, Path, str, pd.Series]:
    """Read silver and prove it matches current bronze and quality audit."""
    resolved_date = resolve_event_date(event_date)
    resolved_run_id, silver_path = discover_streaming_silver_file(
        resolved_date,
        data_root=data_root,
        run_id=run_id,
    )
    audit = require_approved_event_quality_run(
        resolved_date,
        resolved_run_id,
        data_root=data_root,
    )
    silver = pd.read_parquet(silver_path)
    missing = set(WAREHOUSE_COLUMNS).difference(silver.columns)
    unexpected = set(silver.columns).difference(WAREHOUSE_COLUMNS)
    if missing or unexpected:
        details = []
        if missing:
            details.append("missing: " + ", ".join(sorted(missing)))
        if unexpected:
            details.append("unexpected: " + ", ".join(sorted(unexpected)))
        raise ValueError("Streaming silver schema mismatch (" + "; ".join(details) + ")")

    expected_rows = int(audit["valid_rows"])
    if len(silver) != expected_rows:
        raise ValueError(
            f"Streaming silver row count {len(silver)} does not match "
            f"quality run {audit['quality_run_id']}: {expected_rows}"
        )

    bronze_path = build_bronze_event_path(
        resolved_date,
        resolved_run_id,
        data_root,
    )
    if not bronze_path.is_file():
        raise ValueError(f"Current streaming bronze file not found: {bronze_path}")
    bronze_ingestion = pd.read_parquet(bronze_path, columns=["ingestion_id"])
    if (
        bronze_ingestion.empty
        or bronze_ingestion["ingestion_id"].isna().any()
        or bronze_ingestion["ingestion_id"].nunique() != 1
    ):
        raise ValueError("Streaming bronze has no single ingestion_id")
    current_ingestion_id = bronze_ingestion["ingestion_id"].iloc[0]
    if current_ingestion_id != audit["ingestion_id"]:
        raise ValueError(
            "Streaming silver quality audit does not match current bronze ingestion"
        )

    if not silver.empty:
        if not silver["ingestion_id"].eq(current_ingestion_id).all():
            raise ValueError(
                "Streaming silver does not match current bronze ingestion"
            )
        if not silver["quality_run_id"].eq(audit["quality_run_id"]).all():
            raise ValueError(
                "Streaming silver does not match latest approved quality run"
            )
        if silver["event_id"].isna().any() or silver["event_id"].duplicated().any():
            raise ValueError("Streaming silver event_id must be non-null and unique")
        expected_metadata = {
            "source_system": SOURCE_SYSTEM,
            "source_table": SOURCE_TABLE,
            "source_file": f"web_events_{resolved_run_id}.jsonl",
        }
        for column, expected in expected_metadata.items():
            if not silver[column].eq(expected).all():
                raise ValueError(
                    f"Streaming silver {column} does not match {expected!r}"
                )

    return (
        silver.loc[:, WAREHOUSE_COLUMNS].copy(),
        silver_path,
        resolved_run_id,
        audit,
    )


def append_warehouse_events(
    dataframe: pd.DataFrame,
    connection: Any,
    *,
    table_writer: TableWriter = write_warehouse_table,
) -> int:
    """Insert unseen event IDs through an isolated loading table."""
    loading_table = f"_load_{WAREHOUSE_TABLE}_{uuid4().hex}"
    table_writer(dataframe, loading_table, connection)
    target = (
        f'{_quote_identifier(WAREHOUSE_SCHEMA)}.'
        f'{_quote_identifier(WAREHOUSE_TABLE)}'
    )
    loading = (
        f'{_quote_identifier(WAREHOUSE_SCHEMA)}.'
        f'{_quote_identifier(loading_table)}'
    )
    columns = ", ".join(_quote_identifier(column) for column in WAREHOUSE_COLUMNS)
    inserted = connection.exec_driver_sql(
        "WITH inserted AS ("
        f"INSERT INTO {target} ({columns}) "
        f"SELECT {columns} FROM {loading} WHERE TRUE "
        'ON CONFLICT ("event_id") DO NOTHING RETURNING 1'
        ") SELECT COUNT(*) FROM inserted"
    ).scalar_one()
    connection.exec_driver_sql(f"DROP TABLE {loading}")
    return int(inserted)


def load_streaming_silver_to_warehouse(
    event_date: date | str,
    *,
    run_id: str | None = None,
    engine: Engine | Any | None = None,
    data_root: Path | str = DEFAULT_DATA_ROOT,
    schema_path: Path | str = DEFAULT_WAREHOUSE_SCHEMA_PATH,
    table_writer: TableWriter | None = None,
) -> StreamingWarehouseLoadResult:
    """Load one approved event run without duplicating existing event IDs."""
    resolved_date = resolve_event_date(event_date)
    silver, silver_path, resolved_run_id, audit = read_approved_streaming_silver(
        resolved_date,
        data_root=data_root,
        run_id=run_id,
    )
    warehouse_schema_path = Path(schema_path)
    if not warehouse_schema_path.is_file():
        raise FileNotFoundError(
            f"Warehouse schema SQL not found: {warehouse_schema_path}"
        )

    database_engine = engine if engine is not None else get_engine()
    writer = table_writer if table_writer is not None else write_warehouse_table
    schema_sql = warehouse_schema_path.read_text(encoding="utf-8")
    with database_engine.begin() as connection:
        connection.exec_driver_sql(schema_sql)
        inserted_rows = append_warehouse_events(
            silver,
            connection,
            table_writer=writer,
        )

    result = StreamingWarehouseLoadResult(
        event_date=resolved_date,
        run_id=resolved_run_id,
        input_rows=len(silver),
        inserted_rows=inserted_rows,
        duplicate_rows=len(silver) - inserted_rows,
        silver_path=silver_path,
        warehouse_relation=f"{WAREHOUSE_SCHEMA}.{WAREHOUSE_TABLE}",
        quality_run_id=str(audit["quality_run_id"]),
    )
    LOGGER.info(
        "Loaded streaming run_id=%s into %s: input=%s, inserted=%s, duplicates=%s",
        result.run_id,
        result.warehouse_relation,
        result.input_rows,
        result.inserted_rows,
        result.duplicate_rows,
    )
    return result


def _parse_event_date(value: str) -> date:
    try:
        return resolve_event_date(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(str(error)) from error


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse the event partition and optional run identifier."""
    parser = argparse.ArgumentParser(
        description="Incrementally load approved Kafka event silver into PostgreSQL."
    )
    parser.add_argument(
        "--event-date",
        type=_parse_event_date,
        required=True,
        help="silver event partition in YYYY-MM-DD format",
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="event run identifier; optional when the partition has one silver file",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    """CLI entry point for the incremental event warehouse load."""
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    load_streaming_silver_to_warehouse(
        args.event_date,
        run_id=args.run_id,
    )


if __name__ == "__main__":
    main()
