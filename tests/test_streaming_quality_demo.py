"""Behavior tests for the controlled Kafka quality demo."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import pytest

from src.streaming.consume_web_events import (
    ConsumedKafkaMessage,
    write_raw_messages,
)
from src.streaming.create_bad_bronze_demo import (
    create_bad_streaming_bronze_demo,
    parse_args,
)
from src.streaming.raw_to_bronze import convert_raw_events_to_bronze
from src.streaming.validate_web_events import validate_bronze_web_events
from src.streaming.web_events import (
    event_message_key,
    generate_web_events,
    serialize_web_event,
)


EVENT_DATE = "2026-01-01"
SOURCE_RUN_ID = "source-run"
DEMO_RUN_ID = "quality-demo"


def _write_source_bronze(data_root: Path, count: int = 6) -> Path:
    events = generate_web_events(
        count,
        seed=42,
        customer_ids=(1, 2, 3),
        product_ids=(10, 20, 30),
    )
    messages = [
        ConsumedKafkaMessage(
            topic="retailpulse.web_events",
            partition=index % 3,
            offset=index,
            timestamp_ms=int(
                datetime(2026, 1, 1, 12, 0, tzinfo=UTC).timestamp() * 1_000
            ),
            key=event_message_key(event),
            value=serialize_web_event(event),
        )
        for index, event in enumerate(events)
    ]
    write_raw_messages(messages, data_root=data_root, run_id=SOURCE_RUN_ID)
    return convert_raw_events_to_bronze(
        EVENT_DATE,
        data_root=data_root,
        run_id=SOURCE_RUN_ID,
        ingestion_id="source-ingestion",
        ingested_at="2026-01-01T12:05:00Z",
    )[0].bronze_path


def test_demo_creates_separate_raw_and_bronze_without_changing_source(
    tmp_path: Path,
) -> None:
    source_path = _write_source_bronze(tmp_path)
    source_before = pd.read_parquet(source_path)

    result = create_bad_streaming_bronze_demo(
        EVENT_DATE,
        data_root=tmp_path,
        source_run_id=SOURCE_RUN_ID,
        demo_run_id=DEMO_RUN_ID,
    )

    pd.testing.assert_frame_equal(pd.read_parquet(source_path), source_before)
    assert result.row_count == 6
    assert result.demo_raw_path.exists()
    assert result.demo_bronze_path.exists()
    assert len(result.demo_raw_path.read_text(encoding="utf-8").splitlines()) == 6
    demo = pd.read_parquet(result.demo_bronze_path)
    assert demo["source_file"].eq(result.demo_raw_path.name).all()
    assert demo.loc[0, "event_type"] == "unknown_event"
    assert demo.loc[1, "product_id"] == -10
    assert pd.Timestamp(demo.loc[2, "occurred_at"]).date().isoformat() == (
        "2026-01-02"
    )
    assert demo.loc[3, "kafka_partition"] == demo.loc[4, "kafka_partition"]
    assert demo.loc[3, "kafka_offset"] == demo.loc[4, "kafka_offset"]

    rerun = create_bad_streaming_bronze_demo(
        EVENT_DATE,
        data_root=tmp_path,
        demo_run_id=DEMO_RUN_ID,
    )
    assert rerun.source_run_id == SOURCE_RUN_ID
    assert rerun.demo_bronze_path == result.demo_bronze_path


def test_demo_validation_publishes_five_rejected_rows(tmp_path: Path) -> None:
    _write_source_bronze(tmp_path)
    create_bad_streaming_bronze_demo(
        EVENT_DATE,
        data_root=tmp_path,
        source_run_id=SOURCE_RUN_ID,
        demo_run_id=DEMO_RUN_ID,
    )

    result = validate_bronze_web_events(
        EVENT_DATE,
        data_root=tmp_path,
        run_id=DEMO_RUN_ID,
        quality_run_id="demo-quality-run",
        quality_checked_at="2026-01-01T13:00:00Z",
    )[0]

    assert (result.input_rows, result.valid_rows, result.rejected_rows) == (6, 1, 5)
    assert result.status == "warning"
    rejected = pd.read_parquet(result.rejected_path)
    assert "rejection_reason" in rejected.columns
    reasons = "; ".join(rejected["rejection_reason"].tolist())
    assert "event_type is not allowed" in reasons
    assert "product_id must be a positive PostgreSQL INTEGER" in reasons
    assert "occurred_at does not match event_date partition" in reasons
    assert "Kafka topic-partition-offset is duplicated within run_id" in reasons


def test_demo_requires_at_least_five_source_rows(tmp_path: Path) -> None:
    _write_source_bronze(tmp_path, count=4)

    with pytest.raises(ValueError, match="at least 5 rows"):
        create_bad_streaming_bronze_demo(
            EVENT_DATE,
            data_root=tmp_path,
            source_run_id=SOURCE_RUN_ID,
        )


def test_demo_cli_parses_source_and_demo_run_ids() -> None:
    args = parse_args(
        [
            "--event-date",
            EVENT_DATE,
            "--source-run-id",
            SOURCE_RUN_ID,
            "--demo-run-id",
            DEMO_RUN_ID,
        ]
    )

    assert args.event_date.isoformat() == EVENT_DATE
    assert args.source_run_id == SOURCE_RUN_ID
    assert args.demo_run_id == DEMO_RUN_ID
