"""Behavior tests for Kafka bronze-to-silver event quality."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import pytest

from src.streaming.consume_web_events import (
    ConsumedKafkaMessage,
    write_raw_messages,
)
from src.streaming.raw_to_bronze import convert_raw_events_to_bronze
from src.streaming.validate_web_events import (
    build_event_quality_path,
    build_streaming_audit_path,
    parse_args,
    validate_bronze_web_events,
)
from src.streaming.web_events import (
    event_message_key,
    generate_web_events,
    serialize_web_event,
)


EVENT_DATE = "2026-01-01"
QUALITY_RUN_ID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
QUALITY_CHECKED_AT = pd.Timestamp("2026-10-28T12:00:00Z")


def _write_bronze_run(
    data_root: Path,
    *,
    run_id: str,
    seed: int,
    count: int = 2,
    offset_start: int = 0,
) -> Path:
    events = generate_web_events(
        count,
        customer_ids=(1, 2, 3),
        product_ids=(10, 20, 30),
        seed=seed,
    )
    messages = [
        ConsumedKafkaMessage(
            topic="retailpulse.web_events",
            partition=seed % 3,
            offset=offset_start + position,
            timestamp_ms=int(
                datetime(2026, 10, 28, 12, tzinfo=UTC).timestamp() * 1_000
            ),
            key=event_message_key(event),
            value=serialize_web_event(event),
        )
        for position, event in enumerate(events)
    ]
    write_raw_messages(messages, data_root=data_root, run_id=run_id)
    return convert_raw_events_to_bronze(
        EVENT_DATE,
        data_root=data_root,
        run_id=run_id,
        ingestion_id=f"ingestion-{run_id}",
        ingested_at="2026-10-28T11:00:00Z",
    )[0].bronze_path


def _validate(data_root: Path, run_id: str | None = None):
    return validate_bronze_web_events(
        EVENT_DATE,
        data_root=data_root,
        run_id=run_id,
        quality_run_id=QUALITY_RUN_ID,
        quality_checked_at=QUALITY_CHECKED_AT,
    )


def test_valid_bronze_events_publish_silver_rejected_and_audit(
    tmp_path: Path,
) -> None:
    _write_bronze_run(tmp_path, run_id="run-a", seed=42, count=3)

    results = _validate(tmp_path, "run-a")

    assert len(results) == 1
    result = results[0]
    assert (result.input_rows, result.valid_rows, result.rejected_rows) == (3, 3, 0)
    assert result.status == "passed"
    silver = pd.read_parquet(result.silver_path)
    rejected = pd.read_parquet(result.rejected_path)
    assert len(silver) == 3
    assert rejected.empty
    assert silver["quality_run_id"].eq(QUALITY_RUN_ID).all()
    assert isinstance(silver["occurred_at"].dtype, pd.DatetimeTZDtype)
    assert "__index_level_0__" not in silver.columns

    audit = pd.read_parquet(build_streaming_audit_path(tmp_path))
    assert audit.loc[0, "run_id"] == "run-a"
    assert audit.loc[0, "status"] == "passed"
    assert audit.loc[0, "valid_rows"] == 3


def test_invalid_business_and_transport_fields_move_to_rejected(
    tmp_path: Path,
) -> None:
    bronze_path = _write_bronze_run(tmp_path, run_id="run-a", seed=42)
    bronze = pd.read_parquet(bronze_path)
    bronze.loc[0, "event_type"] = "checkout"
    bronze.loc[0, "product_id"] = -10
    bronze.loc[0, "kafka_key"] = "another-session"
    bronze.to_parquet(bronze_path, index=False)

    result = _validate(tmp_path, "run-a")[0]

    silver = pd.read_parquet(result.silver_path)
    rejected = pd.read_parquet(result.rejected_path)
    assert len(silver) == 1
    assert len(rejected) == 1
    reason = rejected.loc[0, "rejection_reason"]
    assert "event_type is not allowed" in reason
    assert "product_id must be a positive PostgreSQL INTEGER" in reason
    assert "kafka_key does not match session_id" in reason
    assert result.status == "warning"


def test_event_id_duplicates_are_detected_across_bronze_runs(
    tmp_path: Path,
) -> None:
    first_path = _write_bronze_run(
        tmp_path, run_id="run-a", seed=42, count=1, offset_start=7
    )
    second_path = _write_bronze_run(
        tmp_path, run_id="run-b", seed=43, count=1, offset_start=20
    )
    first = pd.read_parquet(first_path)
    second = pd.read_parquet(second_path)
    second.loc[0, "event_id"] = first.loc[0, "event_id"]
    second.to_parquet(second_path, index=False)

    results = _validate(tmp_path)

    assert {result.run_id for result in results} == {"run-a", "run-b"}
    assert all(result.rejected_rows == 1 for result in results)
    for result in results:
        rejected = pd.read_parquet(result.rejected_path)
        reason = rejected.loc[0, "rejection_reason"]
        assert "event_id is duplicated" in reason


def test_kafka_position_duplicates_are_detected_within_one_run(
    tmp_path: Path,
) -> None:
    bronze_path = _write_bronze_run(
        tmp_path, run_id="run-a", seed=42, count=2, offset_start=7
    )
    bronze = pd.read_parquet(bronze_path)
    bronze.loc[1, ["kafka_topic", "kafka_partition", "kafka_offset"]] = (
        bronze.loc[
            0, ["kafka_topic", "kafka_partition", "kafka_offset"]
        ].to_numpy()
    )
    bronze.to_parquet(bronze_path, index=False)

    result = _validate(tmp_path, "run-a")[0]

    rejected = pd.read_parquet(result.rejected_path)
    assert len(rejected) == 2
    assert rejected["rejection_reason"].str.contains(
        "Kafka topic-partition-offset is duplicated within run_id"
    ).all()


def test_null_customer_is_valid_but_event_date_mismatch_is_rejected(
    tmp_path: Path,
) -> None:
    bronze_path = _write_bronze_run(tmp_path, run_id="run-a", seed=42)
    bronze = pd.read_parquet(bronze_path)
    bronze["customer_id"] = pd.Series([pd.NA, 1], dtype="Int64")
    bronze.loc[1, "occurred_at"] = "2026-01-02T00:00:00Z"
    bronze.to_parquet(bronze_path, index=False)

    result = _validate(tmp_path, "run-a")[0]

    silver = pd.read_parquet(result.silver_path)
    rejected = pd.read_parquet(result.rejected_path)
    assert len(silver) == 1
    assert silver["customer_id"].isna().all()
    assert rejected["rejection_reason"].str.contains(
        "occurred_at does not match event_date partition"
    ).all()


def test_invalid_bronze_provenance_fails_and_is_audited(tmp_path: Path) -> None:
    bronze_path = _write_bronze_run(tmp_path, run_id="run-a", seed=42)
    bronze = pd.read_parquet(bronze_path)
    bronze["source_file"] = "wrong.jsonl"
    bronze.to_parquet(bronze_path, index=False)

    with pytest.raises(ValueError, match="source_file"):
        _validate(tmp_path, "run-a")

    assert not build_event_quality_path(
        "silver", EVENT_DATE, "run-a", tmp_path
    ).exists()
    audit = pd.read_parquet(build_streaming_audit_path(tmp_path))
    assert audit.loc[0, "status"] == "failed"
    assert "source_file" in audit.loc[0, "error_message"]


def test_quality_cli_parses_event_date_run_id_and_topic() -> None:
    args = parse_args(
        [
            "--event-date",
            EVENT_DATE,
            "--run-id",
            "run-a",
            "--topic",
            "retailpulse.web_events",
        ]
    )

    assert args.event_date.isoformat() == EVENT_DATE
    assert args.run_id == "run-a"
    assert args.topic == "retailpulse.web_events"
