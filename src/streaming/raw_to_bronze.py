"""Convert raw Kafka web-event envelopes into partitioned bronze Parquet."""

from __future__ import annotations

import argparse
import json
import logging
import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from uuid import uuid4

import pandas as pd

from src.streaming.consume_web_events import build_raw_event_path
from src.utils.paths import DEFAULT_DATA_ROOT


LOGGER = logging.getLogger(__name__)
SOURCE_SYSTEM = "kafka"
SOURCE_TABLE = "web_events"
RUN_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]*$")
EVENT_COLUMNS = (
    "event_id",
    "event_type",
    "occurred_at",
    "session_id",
    "customer_id",
    "product_id",
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
    "kafka_timestamp",
    "kafka_key",
)
BRONZE_METADATA_COLUMNS = (
    "ingestion_id",
    "ingested_at",
    "source_system",
    "source_table",
    "source_file",
)


@dataclass(frozen=True)
class BronzeEventResult:
    """Summary of one raw event file converted to bronze."""

    raw_path: Path
    bronze_path: Path
    row_count: int
    ingestion_id: str
    ingested_at: pd.Timestamp


def resolve_event_date(value: date | str) -> date:
    """Parse an explicit event-date partition."""
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise ValueError("event_date must use YYYY-MM-DD format") from error


def _validate_run_id(run_id: str) -> None:
    if not RUN_ID_PATTERN.fullmatch(run_id):
        raise ValueError(f"Invalid run_id: {run_id!r}")


def build_bronze_event_path(
    event_date: date | str,
    run_id: str,
    data_root: Path | str = DEFAULT_DATA_ROOT,
) -> Path:
    """Map one raw run to its bronze Parquet path."""
    resolved_date = resolve_event_date(event_date)
    _validate_run_id(run_id)
    return (
        Path(data_root)
        / "bronze"
        / SOURCE_SYSTEM
        / SOURCE_TABLE
        / f"event_date={resolved_date.isoformat()}"
        / f"web_events_{run_id}.parquet"
    )


def discover_raw_event_files(
    event_date: date | str,
    *,
    data_root: Path | str = DEFAULT_DATA_ROOT,
    run_id: str | None = None,
) -> tuple[Path, ...]:
    """Find raw JSONL files for one event-date partition."""
    resolved_date = resolve_event_date(event_date)
    if run_id is not None:
        _validate_run_id(run_id)
        candidates = (build_raw_event_path(resolved_date, run_id, data_root),)
    else:
        partition = (
            Path(data_root)
            / "raw"
            / SOURCE_SYSTEM
            / SOURCE_TABLE
            / f"event_date={resolved_date.isoformat()}"
        )
        candidates = tuple(sorted(partition.glob("web_events_*.jsonl")))

    existing = tuple(path for path in candidates if path.is_file())
    if not existing:
        run_description = f" and run_id={run_id}" if run_id else ""
        raise FileNotFoundError(
            f"No raw web-event files found for event_date={resolved_date}"
            f"{run_description}"
        )
    return existing


def _flatten_raw_envelope(
    envelope: object,
    *,
    source_path: Path,
    line_number: int,
) -> dict[str, object]:
    if not isinstance(envelope, dict):
        raise ValueError(
            f"{source_path}:{line_number} raw envelope must be a JSON object"
        )
    payload = envelope.get("payload")
    kafka = envelope.get("kafka")
    if not isinstance(payload, dict) or not isinstance(kafka, dict):
        raise ValueError(
            f"{source_path}:{line_number} must contain payload and kafka objects"
        )

    return {
        "event_id": payload.get("event_id"),
        "event_type": payload.get("event_type"),
        "occurred_at": payload.get("occurred_at"),
        "session_id": payload.get("session_id"),
        "customer_id": payload.get("customer_id"),
        "product_id": payload.get("product_id"),
        "kafka_topic": kafka.get("topic"),
        "kafka_partition": kafka.get("partition"),
        "kafka_offset": kafka.get("offset"),
        "kafka_timestamp": kafka.get("timestamp"),
        "kafka_key": kafka.get("key"),
    }


def read_raw_event_file(source_path: Path | str) -> pd.DataFrame:
    """Read one raw JSONL file and flatten its envelope into columns."""
    path = Path(source_path)
    rows: list[dict[str, object]] = []
    with path.open(encoding="utf-8") as source:
        for line_number, line in enumerate(source, start=1):
            if not line.strip():
                raise ValueError(f"{path}:{line_number} raw line is empty")
            try:
                envelope = json.loads(line)
            except json.JSONDecodeError as error:
                raise ValueError(
                    f"{path}:{line_number} contains invalid JSON"
                ) from error
            rows.append(
                _flatten_raw_envelope(
                    envelope,
                    source_path=path,
                    line_number=line_number,
                )
            )
    dataframe = pd.DataFrame.from_records(rows, columns=EVENT_COLUMNS)
    for column in (
        "customer_id",
        "product_id",
        "kafka_partition",
        "kafka_offset",
    ):
        dataframe[column] = pd.array(dataframe[column], dtype="Int64")
    return dataframe


def add_bronze_event_metadata(
    dataframe: pd.DataFrame,
    *,
    ingestion_id: str,
    ingested_at: pd.Timestamp | str,
    source_file: str,
) -> pd.DataFrame:
    """Add technical ingestion metadata without modifying the input frame."""
    collisions = set(BRONZE_METADATA_COLUMNS).intersection(dataframe.columns)
    if collisions:
        names = ", ".join(sorted(collisions))
        raise ValueError(f"Event frame already contains bronze columns: {names}")

    timestamp = pd.Timestamp(ingested_at)
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize("UTC")
    else:
        timestamp = timestamp.tz_convert("UTC")

    bronze = dataframe.copy()
    bronze["ingestion_id"] = ingestion_id
    bronze["ingested_at"] = timestamp
    bronze["source_system"] = SOURCE_SYSTEM
    bronze["source_table"] = SOURCE_TABLE
    bronze["source_file"] = source_file
    return bronze


def convert_raw_events_to_bronze(
    event_date: date | str,
    *,
    data_root: Path | str = DEFAULT_DATA_ROOT,
    run_id: str | None = None,
    ingestion_id: str | None = None,
    ingested_at: pd.Timestamp | str | None = None,
) -> tuple[BronzeEventResult, ...]:
    """Convert selected raw files into one bronze file per raw run."""
    resolved_date = resolve_event_date(event_date)
    raw_paths = discover_raw_event_files(
        resolved_date,
        data_root=data_root,
        run_id=run_id,
    )
    resolved_ingestion_id = ingestion_id or str(uuid4())
    resolved_ingested_at = (
        pd.Timestamp.now(tz="UTC")
        if ingested_at is None
        else pd.Timestamp(ingested_at)
    )
    if resolved_ingested_at.tzinfo is None:
        resolved_ingested_at = resolved_ingested_at.tz_localize("UTC")
    else:
        resolved_ingested_at = resolved_ingested_at.tz_convert("UTC")

    results: list[BronzeEventResult] = []
    for raw_path in raw_paths:
        raw_run_id = raw_path.stem.removeprefix("web_events_")
        bronze_path = build_bronze_event_path(
            resolved_date,
            raw_run_id,
            data_root,
        )
        dataframe = read_raw_event_file(raw_path)
        bronze = add_bronze_event_metadata(
            dataframe,
            ingestion_id=resolved_ingestion_id,
            ingested_at=resolved_ingested_at,
            source_file=raw_path.name,
        )
        bronze_path.parent.mkdir(parents=True, exist_ok=True)
        bronze.to_parquet(bronze_path, index=False)
        results.append(
            BronzeEventResult(
                raw_path=raw_path,
                bronze_path=bronze_path,
                row_count=len(bronze),
                ingestion_id=resolved_ingestion_id,
                ingested_at=resolved_ingested_at,
            )
        )
        LOGGER.info(
            "Converted %s rows from %s to %s",
            len(bronze),
            raw_path,
            bronze_path,
        )
    return tuple(results)


def _parse_event_date(value: str) -> date:
    try:
        return resolve_event_date(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(str(error)) from error


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse the required event date and optional raw run identifier."""
    parser = argparse.ArgumentParser(
        description="Convert raw Kafka web events into bronze Parquet."
    )
    parser.add_argument(
        "--event-date",
        type=_parse_event_date,
        required=True,
        help="raw event partition in YYYY-MM-DD format",
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="optional raw run identifier; defaults to every file in the partition",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    """CLI entry point for raw-to-bronze event conversion."""
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    results = convert_raw_events_to_bronze(
        args.event_date,
        run_id=args.run_id,
    )
    LOGGER.info(
        "Bronze conversion completed: files=%s, rows=%s, ingestion_id=%s",
        len(results),
        sum(result.row_count for result in results),
        results[0].ingestion_id,
    )


if __name__ == "__main__":
    main()
