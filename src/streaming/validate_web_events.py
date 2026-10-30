"""Validate Kafka bronze web events into silver, rejected and audit outputs."""

from __future__ import annotations

import argparse
import logging
import os
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from numbers import Integral, Number
from pathlib import Path
from uuid import UUID, uuid4

import numpy as np
import pandas as pd
from dotenv import load_dotenv

from src.quality.rules import (
    blank_values,
    duplicate_values,
    invalid_positive_integers,
    null_values,
    values_not_in,
)
from src.streaming.raw_to_bronze import (
    BRONZE_METADATA_COLUMNS,
    EVENT_COLUMNS,
    SOURCE_SYSTEM,
    SOURCE_TABLE,
    RUN_ID_PATTERN,
    build_bronze_event_path,
    resolve_event_date,
)
from src.streaming.web_events import ALLOWED_EVENT_TYPES
from src.utils.paths import DEFAULT_DATA_ROOT


LOGGER = logging.getLogger(__name__)
DEFAULT_TOPIC = "retailpulse.web_events"
QUALITY_METADATA_COLUMNS = ("quality_run_id", "quality_checked_at")
INTERNAL_RUN_ID_COLUMN = "_bronze_run_id"
AUDIT_COLUMNS = (
    "quality_run_id",
    "quality_checked_at",
    "event_date",
    "run_id",
    "ingestion_id",
    "source_system",
    "source_table",
    "input_rows",
    "valid_rows",
    "rejected_rows",
    "rejection_rate",
    "status",
    "error_message",
)


@dataclass(frozen=True)
class EventQualityResult:
    """Quality summary and output paths for one bronze event run."""

    run_id: str
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


def build_event_quality_path(
    layer: str,
    event_date: date | str,
    run_id: str,
    data_root: Path | str = DEFAULT_DATA_ROOT,
) -> Path:
    """Build a silver or rejected path for one Kafka event run."""
    if layer not in {"silver", "rejected"}:
        raise ValueError(f"Unsupported event quality layer: {layer!r}")
    resolved_date = resolve_event_date(event_date)
    if not RUN_ID_PATTERN.fullmatch(run_id):
        raise ValueError(f"Invalid run_id: {run_id!r}")
    suffix = "_rejected" if layer == "rejected" else ""
    return (
        Path(data_root)
        / layer
        / SOURCE_SYSTEM
        / SOURCE_TABLE
        / f"event_date={resolved_date.isoformat()}"
        / f"web_events_{run_id}{suffix}.parquet"
    )


def build_streaming_audit_path(
    data_root: Path | str = DEFAULT_DATA_ROOT,
) -> Path:
    """Return the Kafka event-quality audit path."""
    return Path(data_root) / "audit" / "streaming_quality_runs.parquet"


def discover_bronze_event_files(
    event_date: date | str,
    *,
    data_root: Path | str = DEFAULT_DATA_ROOT,
    run_id: str | None = None,
) -> tuple[Path, ...]:
    """Find bronze event files in one event-date partition."""
    resolved_date = resolve_event_date(event_date)
    if run_id is not None:
        candidates = (build_bronze_event_path(resolved_date, run_id, data_root),)
    else:
        partition = (
            Path(data_root)
            / "bronze"
            / SOURCE_SYSTEM
            / SOURCE_TABLE
            / f"event_date={resolved_date.isoformat()}"
        )
        candidates = tuple(sorted(partition.glob("web_events_*.parquet")))

    existing = tuple(path for path in candidates if path.is_file())
    if not existing:
        run_description = f" and run_id={run_id}" if run_id else ""
        raise FileNotFoundError(
            f"No bronze web-event files found for event_date={resolved_date}"
            f"{run_description}"
        )
    return existing


def _run_id_from_bronze_path(path: Path) -> str:
    run_id = path.stem.removeprefix("web_events_")
    if not RUN_ID_PATTERN.fullmatch(run_id):
        raise ValueError(f"Invalid bronze event filename: {path.name!r}")
    return run_id


def _ensure_bronze_columns(dataframe: pd.DataFrame) -> None:
    required = set(EVENT_COLUMNS).union(BRONZE_METADATA_COLUMNS)
    missing = required.difference(dataframe.columns)
    if missing:
        raise ValueError(
            "web_events bronze is missing columns: "
            + ", ".join(sorted(missing))
        )


def validate_bronze_event_metadata(
    dataframe: pd.DataFrame,
    *,
    run_id: str,
) -> str:
    """Validate file-level bronze provenance before row-level rules."""
    _ensure_bronze_columns(dataframe)
    if dataframe.empty:
        raise ValueError("web_events bronze must not be empty")

    ingestion_ids = dataframe["ingestion_id"]
    valid_ingestion_ids = ingestion_ids.map(
        lambda value: isinstance(value, str) and bool(value.strip())
    )
    if not valid_ingestion_ids.all() or ingestion_ids.nunique() != 1:
        raise ValueError("web_events bronze must have one ingestion_id")
    if not dataframe["source_system"].eq(SOURCE_SYSTEM).fillna(False).all():
        raise ValueError(f"web_events source_system must be {SOURCE_SYSTEM!r}")
    if not dataframe["source_table"].eq(SOURCE_TABLE).fillna(False).all():
        raise ValueError(f"web_events source_table must be {SOURCE_TABLE!r}")

    expected_source_file = f"web_events_{run_id}.jsonl"
    if not dataframe["source_file"].eq(expected_source_file).fillna(False).all():
        raise ValueError(
            f"web_events source_file must be {expected_source_file!r}"
        )
    ingested_at = dataframe["ingested_at"]
    parsed_ingested_at = pd.to_datetime(
        ingested_at, errors="coerce", utc=True, format="mixed"
    )
    if parsed_ingested_at.isna().any():
        raise ValueError("web_events bronze must have valid ingested_at values")
    return str(ingestion_ids.iloc[0])


def _invalid_uuids(dataframe: pd.DataFrame, column: str) -> pd.Series:
    def is_invalid(value: object) -> bool:
        if pd.isna(value):
            return False
        try:
            UUID(str(value))
        except (ValueError, AttributeError, TypeError):
            return True
        return False

    return dataframe[column].map(is_invalid)


def _invalid_utc_timestamps(dataframe: pd.DataFrame, column: str) -> pd.Series:
    def is_invalid(value: object) -> bool:
        if pd.isna(value):
            return False
        if isinstance(value, Number):
            return True
        try:
            timestamp = pd.Timestamp(value)
        except (ValueError, TypeError, OverflowError):
            return True
        return timestamp.tzinfo is None or timestamp.utcoffset() != timedelta(0)

    return dataframe[column].map(is_invalid)


def _invalid_non_negative_integers(
    dataframe: pd.DataFrame,
    column: str,
) -> pd.Series:
    def is_invalid(value: object) -> bool:
        if pd.isna(value):
            return False
        return not (
            isinstance(value, Integral)
            and not isinstance(value, (bool, np.bool_))
            and int(value) >= 0
        )

    return dataframe[column].astype(object).map(is_invalid)


def validate_web_event_rows(
    dataframe: pd.DataFrame,
    event_date: date | str,
    *,
    expected_topic: str = DEFAULT_TOPIC,
    quality_run_id: str | None = None,
    quality_checked_at: pd.Timestamp | str | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split aggregated bronze event rows into valid and rejected frames."""
    _ensure_bronze_columns(dataframe)
    if INTERNAL_RUN_ID_COLUMN not in dataframe.columns:
        raise ValueError(f"Missing internal column: {INTERNAL_RUN_ID_COLUMN}")
    collisions = set(QUALITY_METADATA_COLUMNS).intersection(dataframe.columns)
    collisions.update({"rejection_reason"}.intersection(dataframe.columns))
    if collisions:
        raise ValueError(
            "Bronze events already contain quality columns: "
            + ", ".join(sorted(collisions))
        )

    resolved_date = resolve_event_date(event_date)
    working = dataframe.copy().reset_index(drop=True)
    reasons: list[list[str]] = [[] for _ in range(len(working))]

    def reject(mask: pd.Series, reason: str) -> None:
        for position, failed in enumerate(mask.fillna(False).astype(bool).tolist()):
            if failed:
                reasons[position].append(reason)

    for column in ("event_id", "session_id", "kafka_topic", "kafka_key"):
        reject(null_values(working, column), f"{column} is null")
        reject(blank_values(working, column), f"{column} is blank")

    reject(_invalid_uuids(working, "event_id"), "event_id is not a valid UUID")
    reject(duplicate_values(working, "event_id"), "event_id is duplicated")
    reject(
        values_not_in(working, "event_type", ALLOWED_EVENT_TYPES),
        "event_type is not allowed",
    )

    reject(null_values(working, "occurred_at"), "occurred_at is null")
    reject(
        _invalid_utc_timestamps(working, "occurred_at"),
        "occurred_at must be a valid UTC timestamp",
    )
    parsed_occurred_at = pd.to_datetime(
        working["occurred_at"], errors="coerce", utc=True, format="mixed"
    )
    reject(
        parsed_occurred_at.notna()
        & parsed_occurred_at.dt.date.ne(resolved_date),
        "occurred_at does not match event_date partition",
    )

    reject(null_values(working, "product_id"), "product_id is null")
    reject(
        invalid_positive_integers(working, "product_id"),
        "product_id must be a positive PostgreSQL INTEGER",
    )
    reject(
        invalid_positive_integers(working, "customer_id"),
        "customer_id must be null or a positive PostgreSQL INTEGER",
    )

    reject(
        working["kafka_topic"].ne(expected_topic),
        "kafka_topic is not the configured topic",
    )
    for column in ("kafka_partition", "kafka_offset"):
        reject(null_values(working, column), f"{column} is null")
        reject(
            _invalid_non_negative_integers(working, column),
            f"{column} must be a non-negative integer",
        )
    reject(null_values(working, "kafka_timestamp"), "kafka_timestamp is null")
    reject(
        _invalid_utc_timestamps(working, "kafka_timestamp"),
        "kafka_timestamp must be a valid UTC timestamp",
    )
    reject(
        working["kafka_key"].ne(working["session_id"]),
        "kafka_key does not match session_id",
    )

    complete_position = working[
        ["kafka_topic", "kafka_partition", "kafka_offset"]
    ].notna().all(axis=1)
    duplicate_position = working.duplicated(
        subset=[
            INTERNAL_RUN_ID_COLUMN,
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
        ],
        keep=False,
    )
    reject(
        complete_position & duplicate_position,
        "Kafka topic-partition-offset is duplicated within run_id",
    )

    run_id = quality_run_id or str(uuid4())
    checked_at = _utc_timestamp(quality_checked_at)
    rejection_reason = pd.Series(
        ["; ".join(row_reasons) for row_reasons in reasons],
        index=working.index,
        dtype="string",
    )
    rejected_mask = rejection_reason.str.len().gt(0)
    valid = working.loc[~rejected_mask].copy()
    rejected = working.loc[rejected_mask].copy()

    valid["occurred_at"] = pd.to_datetime(
        valid["occurred_at"], utc=True, format="mixed"
    )
    valid["kafka_timestamp"] = pd.to_datetime(
        valid["kafka_timestamp"], utc=True, format="mixed"
    )
    valid["customer_id"] = pd.to_numeric(
        valid["customer_id"], errors="raise"
    ).astype("Int64")
    for column in ("product_id", "kafka_partition", "kafka_offset"):
        valid[column] = pd.to_numeric(valid[column], errors="raise").astype("int64")

    valid["quality_run_id"] = run_id
    valid["quality_checked_at"] = checked_at
    rejected["quality_run_id"] = run_id
    rejected["quality_checked_at"] = checked_at
    rejected["rejection_reason"] = rejection_reason.loc[rejected_mask].to_numpy()
    return valid.reset_index(drop=True), rejected.reset_index(drop=True)


def _append_streaming_audit(
    records: list[dict[str, object]],
    *,
    data_root: Path | str,
) -> Path:
    audit_path = build_streaming_audit_path(data_root)
    audit_path.parent.mkdir(parents=True, exist_ok=True)
    new_rows = pd.DataFrame.from_records(records, columns=AUDIT_COLUMNS)
    if audit_path.exists():
        existing = pd.read_parquet(audit_path)
        new_rows = pd.concat([existing, new_rows], ignore_index=True)
    new_rows.to_parquet(audit_path, index=False)
    return audit_path


def validate_bronze_web_events(
    event_date: date | str,
    *,
    data_root: Path | str = DEFAULT_DATA_ROOT,
    run_id: str | None = None,
    expected_topic: str = DEFAULT_TOPIC,
    quality_run_id: str | None = None,
    quality_checked_at: pd.Timestamp | str | None = None,
) -> tuple[EventQualityResult, ...]:
    """Validate selected bronze files and publish per-run quality outputs."""
    resolved_date = resolve_event_date(event_date)
    bronze_paths = discover_bronze_event_files(
        resolved_date,
        data_root=data_root,
        run_id=run_id,
    )
    resolved_quality_run_id = quality_run_id or str(uuid4())
    checked_at = _utc_timestamp(quality_checked_at)
    frames: list[pd.DataFrame] = []
    ingestion_ids: dict[str, str] = {}

    for bronze_path in bronze_paths:
        bronze_run_id = _run_id_from_bronze_path(bronze_path)
        dataframe = pd.read_parquet(bronze_path)
        try:
            ingestion_ids[bronze_run_id] = validate_bronze_event_metadata(
                dataframe,
                run_id=bronze_run_id,
            )
        except ValueError as error:
            _append_streaming_audit(
                [
                    {
                        "quality_run_id": resolved_quality_run_id,
                        "quality_checked_at": checked_at,
                        "event_date": resolved_date,
                        "run_id": bronze_run_id,
                        "ingestion_id": None,
                        "source_system": SOURCE_SYSTEM,
                        "source_table": SOURCE_TABLE,
                        "input_rows": len(dataframe),
                        "valid_rows": 0,
                        "rejected_rows": 0,
                        "rejection_rate": 0.0,
                        "status": "failed",
                        "error_message": str(error),
                    }
                ],
                data_root=data_root,
            )
            raise
        selected = dataframe.copy()
        selected[INTERNAL_RUN_ID_COLUMN] = bronze_run_id
        frames.append(selected)

    aggregated = pd.concat(frames, ignore_index=True)
    valid, rejected = validate_web_event_rows(
        aggregated,
        resolved_date,
        expected_topic=expected_topic,
        quality_run_id=resolved_quality_run_id,
        quality_checked_at=checked_at,
    )

    results: list[EventQualityResult] = []
    audit_records: list[dict[str, object]] = []
    for bronze_path in bronze_paths:
        bronze_run_id = _run_id_from_bronze_path(bronze_path)
        input_rows = int(
            aggregated[INTERNAL_RUN_ID_COLUMN].eq(bronze_run_id).sum()
        )
        valid_run = valid.loc[
            valid[INTERNAL_RUN_ID_COLUMN].eq(bronze_run_id)
        ].drop(columns=[INTERNAL_RUN_ID_COLUMN])
        rejected_run = rejected.loc[
            rejected[INTERNAL_RUN_ID_COLUMN].eq(bronze_run_id)
        ].drop(columns=[INTERNAL_RUN_ID_COLUMN])
        silver_path = build_event_quality_path(
            "silver", resolved_date, bronze_run_id, data_root
        )
        rejected_path = build_event_quality_path(
            "rejected", resolved_date, bronze_run_id, data_root
        )
        silver_path.parent.mkdir(parents=True, exist_ok=True)
        rejected_path.parent.mkdir(parents=True, exist_ok=True)
        valid_run.to_parquet(silver_path, index=False)
        rejected_run.to_parquet(rejected_path, index=False)

        rejected_rows = len(rejected_run)
        rejection_rate = rejected_rows / input_rows if input_rows else 0.0
        status = "warning" if rejected_rows else "passed"
        result = EventQualityResult(
            run_id=bronze_run_id,
            input_rows=input_rows,
            valid_rows=len(valid_run),
            rejected_rows=rejected_rows,
            rejection_rate=rejection_rate,
            status=status,
            silver_path=silver_path,
            rejected_path=rejected_path,
            quality_run_id=resolved_quality_run_id,
            quality_checked_at=checked_at,
        )
        results.append(result)
        audit_records.append(
            {
                "quality_run_id": resolved_quality_run_id,
                "quality_checked_at": checked_at,
                "event_date": resolved_date,
                "run_id": bronze_run_id,
                "ingestion_id": ingestion_ids[bronze_run_id],
                "source_system": SOURCE_SYSTEM,
                "source_table": SOURCE_TABLE,
                "input_rows": input_rows,
                "valid_rows": len(valid_run),
                "rejected_rows": rejected_rows,
                "rejection_rate": rejection_rate,
                "status": status,
                "error_message": None,
            }
        )
        LOGGER.info(
            "Validated web events run_id=%s: input=%s, valid=%s, rejected=%s",
            bronze_run_id,
            input_rows,
            len(valid_run),
            rejected_rows,
        )

    _append_streaming_audit(audit_records, data_root=data_root)
    return tuple(results)


def _parse_event_date(value: str) -> date:
    try:
        return resolve_event_date(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(str(error)) from error


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse the event partition and optional bronze run identifier."""
    parser = argparse.ArgumentParser(
        description="Validate Kafka bronze web events into silver and rejected."
    )
    parser.add_argument(
        "--event-date",
        type=_parse_event_date,
        required=True,
        help="bronze event partition in YYYY-MM-DD format",
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="optional bronze run identifier; defaults to the whole partition",
    )
    parser.add_argument(
        "--topic",
        default=None,
        help="expected Kafka topic; defaults to KAFKA_TOPIC_WEB_EVENTS",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    """CLI entry point for Kafka event quality validation."""
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    load_dotenv()
    expected_topic = args.topic or os.getenv(
        "KAFKA_TOPIC_WEB_EVENTS", DEFAULT_TOPIC
    )
    results = validate_bronze_web_events(
        args.event_date,
        run_id=args.run_id,
        expected_topic=expected_topic,
    )
    LOGGER.info(
        "Event quality completed: files=%s, valid=%s, rejected=%s, quality_run_id=%s",
        len(results),
        sum(result.valid_rows for result in results),
        sum(result.rejected_rows for result in results),
        results[0].quality_run_id,
    )


if __name__ == "__main__":
    main()
