"""Create a separate Kafka bronze run with deterministic quality anomalies."""

from __future__ import annotations

import argparse
import json
import logging
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pandas as pd

from src.streaming.consume_web_events import build_raw_event_path
from src.streaming.raw_to_bronze import (
    RUN_ID_PATTERN,
    build_bronze_event_path,
    resolve_event_date,
)
from src.streaming.validate_web_events import discover_bronze_event_files
from src.utils.paths import DEFAULT_DATA_ROOT


LOGGER = logging.getLogger(__name__)
DEFAULT_DEMO_RUN_ID = "quality-demo"
MINIMUM_DEMO_ROWS = 5
DEMO_ANOMALIES = (
    "event_type is not allowed",
    "product_id is negative",
    "occurred_at does not match event_date",
    "two rows share one Kafka topic-partition-offset position",
)


@dataclass(frozen=True)
class StreamingQualityDemoResult:
    """Paths and counts for one controlled streaming-quality demo run."""

    source_run_id: str
    demo_run_id: str
    row_count: int
    source_bronze_path: Path
    demo_raw_path: Path
    demo_bronze_path: Path


def _run_id_from_path(path: Path) -> str:
    return path.stem.removeprefix("web_events_")


def _resolve_source_bronze(
    event_date: date,
    *,
    data_root: Path | str,
    source_run_id: str | None,
    demo_run_id: str,
) -> tuple[str, Path]:
    paths = discover_bronze_event_files(
        event_date,
        data_root=data_root,
        run_id=source_run_id,
    )
    if source_run_id is None:
        paths = tuple(
            path for path in paths if _run_id_from_path(path) != demo_run_id
        )
    if not paths:
        raise FileNotFoundError("No non-demo source bronze run was found")
    if len(paths) != 1:
        raise ValueError(
            "More than one source bronze run exists; provide --source-run-id"
        )
    path = paths[0]
    return _run_id_from_path(path), path


def inject_streaming_demo_anomalies(
    dataframe: pd.DataFrame,
    event_date: date | str,
) -> pd.DataFrame:
    """Return a copy with five deterministic rejected rows."""
    if len(dataframe) < MINIMUM_DEMO_ROWS:
        raise ValueError(
            f"Streaming quality demo requires at least {MINIMUM_DEMO_ROWS} rows"
        )

    resolved_date = resolve_event_date(event_date)
    demo = dataframe.copy().reset_index(drop=True)
    demo.at[0, "event_type"] = "unknown_event"
    demo.at[1, "product_id"] = -10
    demo.at[2, "occurred_at"] = (
        pd.Timestamp(resolved_date, tz="UTC") + pd.Timedelta(days=1)
    ).isoformat().replace("+00:00", "Z")
    position_columns = ["kafka_topic", "kafka_partition", "kafka_offset"]
    demo.loc[3, position_columns] = demo.loc[4, position_columns].to_numpy()
    return demo


def _json_timestamp(value: object) -> str:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize("UTC")
    else:
        timestamp = timestamp.tz_convert("UTC")
    return timestamp.isoformat().replace("+00:00", "Z")


def _write_demo_raw(dataframe: pd.DataFrame, path: Path) -> None:
    """Write the demo rows as raw envelopes matching their bronze source_file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as output:
        for row in dataframe.itertuples(index=False):
            customer_id = None if pd.isna(row.customer_id) else int(row.customer_id)
            envelope = {
                "kafka": {
                    "topic": row.kafka_topic,
                    "partition": int(row.kafka_partition),
                    "offset": int(row.kafka_offset),
                    "timestamp": _json_timestamp(row.kafka_timestamp),
                    "key": row.kafka_key,
                },
                "payload": {
                    "event_id": row.event_id,
                    "event_type": row.event_type,
                    "occurred_at": _json_timestamp(row.occurred_at),
                    "session_id": row.session_id,
                    "customer_id": customer_id,
                    "product_id": int(row.product_id),
                },
            }
            output.write(
                json.dumps(envelope, ensure_ascii=False, separators=(",", ":"))
                + "\n"
            )


def create_bad_streaming_bronze_demo(
    event_date: date | str,
    *,
    data_root: Path | str = DEFAULT_DATA_ROOT,
    source_run_id: str | None = None,
    demo_run_id: str = DEFAULT_DEMO_RUN_ID,
) -> StreamingQualityDemoResult:
    """Copy one bronze run and inject controlled anomalies into a demo run."""
    resolved_date = resolve_event_date(event_date)
    if not RUN_ID_PATTERN.fullmatch(demo_run_id):
        raise ValueError(f"Invalid demo_run_id: {demo_run_id!r}")

    resolved_source_run_id, source_path = _resolve_source_bronze(
        resolved_date,
        data_root=data_root,
        source_run_id=source_run_id,
        demo_run_id=demo_run_id,
    )
    if resolved_source_run_id == demo_run_id:
        raise ValueError("source_run_id and demo_run_id must be different")

    source = pd.read_parquet(source_path)
    demo = inject_streaming_demo_anomalies(source, resolved_date)
    demo_raw_path = build_raw_event_path(
        resolved_date,
        demo_run_id,
        data_root,
    )
    demo_bronze_path = build_bronze_event_path(
        resolved_date,
        demo_run_id,
        data_root,
    )
    demo["ingestion_id"] = f"demo-{resolved_source_run_id}"
    demo["source_file"] = demo_raw_path.name

    _write_demo_raw(demo, demo_raw_path)
    demo_bronze_path.parent.mkdir(parents=True, exist_ok=True)
    demo.to_parquet(demo_bronze_path, index=False)
    LOGGER.info(
        "Created streaming quality demo: source_run_id=%s, demo_run_id=%s, "
        "rows=%s, bronze=%s",
        resolved_source_run_id,
        demo_run_id,
        len(demo),
        demo_bronze_path,
    )
    return StreamingQualityDemoResult(
        source_run_id=resolved_source_run_id,
        demo_run_id=demo_run_id,
        row_count=len(demo),
        source_bronze_path=source_path,
        demo_raw_path=demo_raw_path,
        demo_bronze_path=demo_bronze_path,
    )


def _parse_event_date(value: str) -> date:
    try:
        return resolve_event_date(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(str(error)) from error


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse the source partition and demo run identifier."""
    parser = argparse.ArgumentParser(
        description="Create a Kafka bronze demo run with controlled anomalies."
    )
    parser.add_argument(
        "--event-date",
        type=_parse_event_date,
        required=True,
        help="existing bronze partition in YYYY-MM-DD format",
    )
    parser.add_argument(
        "--source-run-id",
        default=None,
        help="source bronze run; optional when the partition has one file",
    )
    parser.add_argument(
        "--demo-run-id",
        default=DEFAULT_DEMO_RUN_ID,
        help=f"demo run identifier; defaults to {DEFAULT_DEMO_RUN_ID}",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    """CLI entry point for the controlled streaming-quality demo."""
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    result = create_bad_streaming_bronze_demo(
        args.event_date,
        source_run_id=args.source_run_id,
        demo_run_id=args.demo_run_id,
    )
    LOGGER.info(
        "Streaming quality demo completed: run_id=%s, rows=%s",
        result.demo_run_id,
        result.row_count,
    )


if __name__ == "__main__":
    main()
