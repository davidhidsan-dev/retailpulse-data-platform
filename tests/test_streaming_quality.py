"""Behavior tests for Kafka bronze-to-silver event quality."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import pytest

from src.streaming.consume_web_events import (
    ConsumedKafkaMessage,
    build_raw_event_path,
    write_raw_messages,
)
from src.streaming.raw_to_bronze import (
    RAW_VALUE_COLUMN_BY_FIELD,
    RAW_VALUE_COLUMNS,
    convert_raw_events_to_bronze,
)
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
            )
            + position,
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
    bronze.loc[0, RAW_VALUE_COLUMN_BY_FIELD["product_id"]] = "-10"
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
    for column in ("kafka_partition", "kafka_offset"):
        raw_column = RAW_VALUE_COLUMN_BY_FIELD[column]
        bronze.loc[1, raw_column] = bronze.loc[0, raw_column]
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
    bronze[RAW_VALUE_COLUMN_BY_FIELD["customer_id"]] = ["null", "1"]
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


def test_quality_builds_integer_columns_from_original_json_values(
    tmp_path: Path,
) -> None:
    bronze_path = _write_bronze_run(tmp_path, run_id="run-a", seed=42)
    bronze = pd.read_parquet(bronze_path)
    expected = {
        "customer_id": [None, 2_147_483_647],
        "product_id": [10, 2_147_483_647],
        "kafka_partition": [0, 2_147_483_647],
        "kafka_offset": [0, 9_223_372_036_854_775_807],
    }
    for column, values in expected.items():
        bronze[RAW_VALUE_COLUMN_BY_FIELD[column]] = [
            json.dumps(value) for value in values
        ]
        # An inconsistent numeric copy must not change approved values or
        # create a false duplicate Kafka position.
        bronze[column] = 999
    bronze.to_parquet(bronze_path, index=False)

    result = _validate(tmp_path, "run-a")[0]

    assert (result.valid_rows, result.rejected_rows) == (2, 0)
    silver = pd.read_parquet(result.silver_path)
    for column, values in expected.items():
        dtype = "Int64" if column == "customer_id" else "int64"
        pd.testing.assert_series_equal(
            silver[column], pd.Series(values, name=column, dtype=dtype)
        )


@pytest.mark.parametrize(
    ("column", "raw_value"),
    [
        ("product_id", "0"),
        ("product_id", "2147483648"),
        ("customer_id", "true"),
        ("customer_id", "2147483648"),
        ("kafka_partition", "-1"),
        ("kafka_offset", "9223372036854775808"),
    ],
)
def test_invalid_raw_integer_values_are_rejected(
    tmp_path: Path, column: str, raw_value: str,
) -> None:
    bronze_path = _write_bronze_run(
        tmp_path, run_id="run-a", seed=42, count=1
    )
    bronze = pd.read_parquet(bronze_path)
    raw_column = RAW_VALUE_COLUMN_BY_FIELD[column]
    bronze[raw_column] = raw_value
    bronze.to_parquet(bronze_path, index=False)

    result = _validate(tmp_path, "run-a")[0]

    assert (result.valid_rows, result.rejected_rows) == (0, 1)
    rejected = pd.read_parquet(result.rejected_path)
    assert rejected.loc[0, raw_column] == raw_value
    assert f"{column} must be" in rejected.loc[0, "rejection_reason"]


def test_invalid_json_id_types_reach_rejected_without_coercion(
    tmp_path: Path,
) -> None:
    run_id = "raw-id-types"
    raw_path = build_raw_event_path(
        datetime.fromisoformat(EVENT_DATE).date(), run_id, tmp_path
    )
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    product_values = [10, True, 1.0, "01", "bad-id", 1.5]
    envelopes = []
    for position, product_id in enumerate(product_values):
        session_id = f"session-{position}"
        envelopes.append(
            {
                "kafka": {
                    "topic": "retailpulse.web_events",
                    "partition": 0,
                    "offset": position,
                    "timestamp": "2026-01-01T12:00:00Z",
                    "key": session_id,
                },
                "payload": {
                    "event_id": (
                        f"00000000-0000-0000-0000-{position + 1:012d}"
                    ),
                    "event_type": "product_view",
                    "occurred_at": "2026-01-01T12:00:00Z",
                    "session_id": session_id,
                    "customer_id": 1,
                    "product_id": product_id,
                },
            }
        )
    raw_path.write_text(
        "".join(
            json.dumps(envelope, separators=(",", ":")) + "\n"
            for envelope in envelopes
        ),
        encoding="utf-8",
    )

    bronze_result = convert_raw_events_to_bronze(
        EVENT_DATE,
        data_root=tmp_path,
        run_id=run_id,
        ingestion_id="raw-types-ingestion",
        ingested_at="2026-01-01T12:05:00Z",
    )[0]
    bronze = pd.read_parquet(bronze_result.bronze_path)

    assert bronze["product_id"].tolist()[0] == 10
    assert bronze["product_id"].iloc[1:].isna().all()
    assert bronze["product_id_raw_value"].tolist() == [
        "10",
        "true",
        "1.0",
        '"01"',
        '"bad-id"',
        "1.5",
    ]

    result = _validate(tmp_path, run_id)[0]
    silver = pd.read_parquet(result.silver_path)
    rejected = pd.read_parquet(result.rejected_path)

    assert (result.valid_rows, result.rejected_rows) == (1, 5)
    assert silver["product_id"].tolist() == [10]
    assert set(RAW_VALUE_COLUMNS).isdisjoint(silver.columns)
    assert rejected["product_id_raw_value"].tolist() == [
        "true",
        "1.0",
        '"01"',
        '"bad-id"',
        "1.5",
    ]
    assert rejected["rejection_reason"].str.contains(
        "product_id must be a positive PostgreSQL INTEGER"
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
            "--data-root",
            "custom-data",
        ]
    )

    assert args.event_date.isoformat() == EVENT_DATE
    assert args.run_id == "run-a"
    assert args.topic == "retailpulse.web_events"
    assert args.data_root == Path("custom-data")
