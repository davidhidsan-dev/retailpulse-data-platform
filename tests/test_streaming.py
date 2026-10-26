"""Behavior tests for the finite RetailPulse Kafka web-event flow."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from sqlalchemy import create_engine

from src.streaming.consume_web_events import (
    ConsumedKafkaMessage,
    build_raw_event_path,
    parse_args as parse_consumer_args,
    write_raw_messages,
)
from src.streaming.produce_web_events import (
    load_source_reference_ids,
    parse_args as parse_producer_args,
)
from src.streaming.web_events import (
    ALLOWED_EVENT_TYPES,
    WebEvent,
    deserialize_web_event,
    event_message_key,
    generate_web_events,
    serialize_web_event,
)


CUSTOMER_IDS = (3, 8, 21, 34)
PRODUCT_IDS = (2, 7, 19)


def _generate_events(count: int, *, seed: int) -> list[WebEvent]:
    return generate_web_events(
        count,
        customer_ids=CUSTOMER_IDS,
        product_ids=PRODUCT_IDS,
        seed=seed,
    )


def test_web_event_generation_is_reproducible_with_a_seed() -> None:
    first = _generate_events(25, seed=123)
    second = _generate_events(25, seed=123)

    assert first == second


def test_generated_event_ids_are_unique() -> None:
    events = _generate_events(500, seed=42)

    assert len({event.event_id for event in events}) == len(events)


def test_generated_events_follow_type_and_utc_contracts() -> None:
    events = _generate_events(100, seed=42)

    assert {event.event_type for event in events}.issubset(ALLOWED_EVENT_TYPES)
    assert all(event.occurred_at.tzinfo is not None for event in events)
    assert all(event.occurred_at.utcoffset().total_seconds() == 0 for event in events)


def test_generated_events_only_reference_existing_source_ids() -> None:
    events = _generate_events(100, seed=42)

    assert {
        event.customer_id for event in events if event.customer_id is not None
    }.issubset(CUSTOMER_IDS)
    assert {event.product_id for event in events}.issubset(PRODUCT_IDS)


def test_each_session_appears_and_keeps_one_customer() -> None:
    count = 40
    events = _generate_events(count, seed=42)
    expected_session_count = max(1, min(count, max(2, count // 4)))
    customers_by_session: dict[str, set[int | None]] = {}

    for event in events:
        customers_by_session.setdefault(event.session_id, set()).add(
            event.customer_id
        )

    assert len(customers_by_session) == expected_session_count
    assert all(
        len(customer_ids) == 1 for customer_ids in customers_by_session.values()
    )


def test_source_reference_ids_are_read_from_existing_rows() -> None:
    engine = create_engine("sqlite://")
    with engine.begin() as connection:
        connection.exec_driver_sql("CREATE TABLE customers (customer_id INTEGER)")
        connection.exec_driver_sql("CREATE TABLE products (product_id INTEGER)")
        connection.exec_driver_sql(
            "INSERT INTO customers (customer_id) VALUES (8), (3), (21)"
        )
        connection.exec_driver_sql(
            "INSERT INTO products (product_id) VALUES (19), (2), (7)"
        )

    references = load_source_reference_ids(engine)

    assert references.customer_ids == (3, 8, 21)
    assert references.product_ids == (2, 7, 19)


def test_json_round_trip_and_message_key_use_session_id() -> None:
    event = _generate_events(1, seed=7)[0]

    payload = serialize_web_event(event)
    restored = deserialize_web_event(payload)

    assert restored == event
    assert json.loads(payload.decode("utf-8"))["event_id"] == event.event_id
    assert event_message_key(event) == event.session_id.encode("utf-8")


def test_raw_jsonl_preserves_payload_and_kafka_metadata(tmp_path: Path) -> None:
    event = _generate_events(1, seed=9)[0]
    message = ConsumedKafkaMessage(
        topic="retailpulse.web_events",
        partition=0,
        offset=17,
        timestamp_ms=1_767_225_600_000,
        key=event_message_key(event),
        value=serialize_web_event(event),
    )

    paths = write_raw_messages(
        [message], data_root=tmp_path, run_id="test-run"
    )

    expected_path = build_raw_event_path(
        event.occurred_at.date(), "test-run", tmp_path
    )
    assert paths == (expected_path,)
    lines = expected_path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1

    stored = json.loads(lines[0])
    assert stored["payload"] == event.to_dict()
    assert stored["kafka"] == {
        "topic": "retailpulse.web_events",
        "partition": 0,
        "offset": 17,
        "timestamp": "2026-01-01T00:00:00Z",
        "key": event.session_id,
    }


def test_raw_writer_does_not_silently_overwrite_a_run(tmp_path: Path) -> None:
    event = _generate_events(1, seed=11)[0]
    message = ConsumedKafkaMessage(
        topic="retailpulse.web_events",
        partition=0,
        offset=0,
        timestamp_ms=int(datetime(2026, 1, 1, tzinfo=UTC).timestamp() * 1_000),
        key=event_message_key(event),
        value=serialize_web_event(event),
    )
    write_raw_messages([message], data_root=tmp_path, run_id="same-run")

    with pytest.raises(FileExistsError):
        write_raw_messages([message], data_root=tmp_path, run_id="same-run")


def test_cli_accepts_controlled_counts_and_seed() -> None:
    producer_args = parse_producer_args(["--count", "20", "--seed", "123"])
    consumer_args = parse_consumer_args(
        ["--count", "20", "--timeout-seconds", "5"]
    )

    assert (producer_args.count, producer_args.seed) == (20, 123)
    assert (consumer_args.count, consumer_args.timeout_seconds) == (20, 5)

    with pytest.raises(SystemExit):
        parse_producer_args(["--count", "10001"])
