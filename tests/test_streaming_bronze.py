"""Tests for raw Kafka web-event conversion into bronze Parquet."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import pytest

from src.streaming.consume_web_events import (
    ConsumedKafkaMessage,
    write_raw_messages,
)
from src.streaming.raw_to_bronze import (
    BRONZE_METADATA_COLUMNS,
    EVENT_COLUMNS,
    build_bronze_event_path,
    convert_raw_events_to_bronze,
    parse_args,
    read_raw_event_file,
)
from src.streaming.web_events import (
    event_message_key,
    generate_web_events,
    serialize_web_event,
)


EVENT_DATE = "2026-01-01"
INGESTED_AT = pd.Timestamp("2026-10-28T10:00:00Z")
INGESTION_ID = "11111111-2222-3333-4444-555555555555"


def _write_raw_run(
    data_root: Path,
    *,
    run_id: str,
    seed: int,
    offset: int,
) -> Path:
    event = generate_web_events(
        1,
        customer_ids=(1, 2),
        product_ids=(10, 20),
        seed=seed,
    )[0]
    message = ConsumedKafkaMessage(
        topic="retailpulse.web_events",
        partition=seed % 3,
        offset=offset,
        timestamp_ms=int(
            datetime(2026, 10, 28, 10, tzinfo=UTC).timestamp() * 1_000
        ),
        key=event_message_key(event),
        value=serialize_web_event(event),
    )
    return write_raw_messages(
        [message],
        data_root=data_root,
        run_id=run_id,
    )[0]


def test_build_bronze_event_path_uses_event_partition_and_run_id(
    tmp_path: Path,
) -> None:
    path = build_bronze_event_path(EVENT_DATE, "run-123", tmp_path)

    assert path == (
        tmp_path
        / "bronze"
        / "kafka"
        / "web_events"
        / f"event_date={EVENT_DATE}"
        / "web_events_run-123.parquet"
    )


def test_raw_event_conversion_preserves_payload_and_kafka_metadata(
    tmp_path: Path,
) -> None:
    raw_path = _write_raw_run(tmp_path, run_id="run-a", seed=42, offset=17)

    results = convert_raw_events_to_bronze(
        EVENT_DATE,
        data_root=tmp_path,
        run_id="run-a",
        ingestion_id=INGESTION_ID,
        ingested_at=INGESTED_AT,
    )

    assert len(results) == 1
    result = results[0]
    assert result.raw_path == raw_path
    assert result.row_count == 1
    bronze = pd.read_parquet(result.bronze_path)
    assert list(bronze.columns) == [*EVENT_COLUMNS, *BRONZE_METADATA_COLUMNS]
    assert bronze.loc[0, "event_id"]
    assert bronze.loc[0, "kafka_topic"] == "retailpulse.web_events"
    assert bronze.loc[0, "kafka_partition"] == 0
    assert bronze.loc[0, "kafka_offset"] == 17
    assert bronze.loc[0, "kafka_key"] == bronze.loc[0, "session_id"]
    assert bronze.loc[0, "ingestion_id"] == INGESTION_ID
    assert bronze.loc[0, "source_system"] == "kafka"
    assert bronze.loc[0, "source_table"] == "web_events"
    assert bronze.loc[0, "source_file"] == raw_path.name
    assert "__index_level_0__" not in bronze.columns


def test_one_conversion_shares_ingestion_id_between_raw_files(
    tmp_path: Path,
) -> None:
    _write_raw_run(tmp_path, run_id="run-a", seed=42, offset=1)
    _write_raw_run(tmp_path, run_id="run-b", seed=43, offset=2)

    results = convert_raw_events_to_bronze(
        EVENT_DATE,
        data_root=tmp_path,
        ingestion_id=INGESTION_ID,
        ingested_at=INGESTED_AT,
    )

    assert {result.bronze_path.name for result in results} == {
        "web_events_run-a.parquet",
        "web_events_run-b.parquet",
    }
    assert {result.ingestion_id for result in results} == {INGESTION_ID}
    for result in results:
        bronze = pd.read_parquet(result.bronze_path)
        assert bronze["ingestion_id"].eq(INGESTION_ID).all()


def test_run_id_limits_conversion_to_one_raw_file(tmp_path: Path) -> None:
    _write_raw_run(tmp_path, run_id="run-a", seed=42, offset=1)
    _write_raw_run(tmp_path, run_id="run-b", seed=43, offset=2)

    results = convert_raw_events_to_bronze(
        EVENT_DATE,
        data_root=tmp_path,
        run_id="run-b",
    )

    assert [result.raw_path.name for result in results] == [
        "web_events_run-b.jsonl"
    ]
    assert not build_bronze_event_path(EVENT_DATE, "run-a", tmp_path).exists()


def test_rerun_reuses_the_same_bronze_path(tmp_path: Path) -> None:
    _write_raw_run(tmp_path, run_id="same-run", seed=42, offset=1)

    first = convert_raw_events_to_bronze(
        EVENT_DATE,
        data_root=tmp_path,
        run_id="same-run",
        ingestion_id="first-ingestion",
        ingested_at=INGESTED_AT,
    )[0]
    second = convert_raw_events_to_bronze(
        EVENT_DATE,
        data_root=tmp_path,
        run_id="same-run",
        ingestion_id="second-ingestion",
        ingested_at=INGESTED_AT,
    )[0]

    assert first.bronze_path == second.bronze_path
    assert len(tuple(second.bronze_path.parent.glob("*.parquet"))) == 1
    bronze = pd.read_parquet(second.bronze_path)
    assert bronze["ingestion_id"].eq("second-ingestion").all()


def test_invalid_raw_json_reports_file_and_line(tmp_path: Path) -> None:
    raw_path = tmp_path / "broken.jsonl"
    raw_path.write_text("not-json\n", encoding="utf-8")

    with pytest.raises(ValueError, match=r"broken\.jsonl:1 contains invalid JSON"):
        read_raw_event_file(raw_path)


def test_cli_requires_a_valid_event_date_and_accepts_run_id() -> None:
    args = parse_args(["--event-date", EVENT_DATE, "--run-id", "run-a"])

    assert args.event_date.isoformat() == EVENT_DATE
    assert args.run_id == "run-a"

    with pytest.raises(SystemExit):
        parse_args(["--event-date", "28-10-2026"])
