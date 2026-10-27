"""Behavior tests for the finite RetailPulse Kafka web-event flow."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine

from src.streaming.config import get_kafka_settings
from src.streaming.consume_web_events import (
    ConsumedKafkaMessage,
    build_raw_event_path,
    consume_web_events,
    parse_args as parse_consumer_args,
    write_raw_messages,
)
from src.streaming.produce_web_events import (
    ensure_topic,
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


def test_kafka_settings_include_positive_partition_count(monkeypatch) -> None:
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
    monkeypatch.setenv("KAFKA_TOPIC_WEB_EVENTS", "retailpulse.web_events")
    monkeypatch.setenv("KAFKA_TOPIC_PARTITIONS", "3")
    monkeypatch.setenv("KAFKA_TOPIC_REPLICATION_FACTOR", "3")

    settings = get_kafka_settings()

    assert settings.topic_partitions == 3
    assert settings.topic_replication_factor == 3


@pytest.mark.filterwarnings("ignore::DeprecationWarning:kafka\\..*")
@pytest.mark.filterwarnings(
    "ignore::DeprecationWarning:importlib\\.resources\\._legacy"
)
def test_ensure_topic_creates_configured_partition_count() -> None:
    admin = MagicMock()
    admin.list_topics.return_value = set()

    with patch("kafka.KafkaAdminClient", return_value=admin):
        ensure_topic(
            bootstrap_servers="unused:9092",
            topic="retailpulse.web_events",
            partition_count=3,
            replication_factor=3,
        )

    created_topic = admin.create_topics.call_args.args[0][0]
    assert created_topic.num_partitions == 3
    assert created_topic.replication_factor == 3
    admin.close.assert_called_once()


@pytest.mark.filterwarnings("ignore::DeprecationWarning:kafka\\..*")
@pytest.mark.filterwarnings(
    "ignore::DeprecationWarning:importlib\\.resources\\._legacy"
)
def test_ensure_topic_expands_an_existing_topic() -> None:
    admin = MagicMock()
    admin.list_topics.return_value = {"retailpulse.web_events"}
    admin.describe_topics.return_value = [
        {
            "name": "retailpulse.web_events",
            "partitions": [
                {
                    "partition_index": 0,
                    "replica_nodes": [1, 2, 3],
                }
            ],
        }
    ]

    with patch("kafka.KafkaAdminClient", return_value=admin):
        ensure_topic(
            bootstrap_servers="unused:9092",
            topic="retailpulse.web_events",
            partition_count=3,
            replication_factor=3,
        )

    new_partitions = admin.create_partitions.call_args.args[0]
    assert new_partitions["retailpulse.web_events"].total_count == 3
    admin.close.assert_called_once()


@pytest.mark.filterwarnings("ignore::DeprecationWarning:kafka\\..*")
@pytest.mark.filterwarnings(
    "ignore::DeprecationWarning:importlib\\.resources\\._legacy"
)
def test_ensure_topic_rejects_an_incompatible_replication_factor() -> None:
    admin = MagicMock()
    admin.list_topics.return_value = {"retailpulse.web_events"}
    admin.describe_topics.return_value = [
        {
            "name": "retailpulse.web_events",
            "partitions": [
                {
                    "partition_index": 0,
                    "replica_nodes": [1],
                }
            ],
        }
    ]

    with patch("kafka.KafkaAdminClient", return_value=admin):
        with pytest.raises(RuntimeError, match="partition reassignment"):
            ensure_topic(
                bootstrap_servers="unused:9092",
                topic="retailpulse.web_events",
                partition_count=3,
                replication_factor=3,
            )

    admin.create_partitions.assert_not_called()
    admin.close.assert_called_once()


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


def test_consumer_group_mode_accepts_a_partial_partition_share(
    tmp_path: Path,
) -> None:
    event = _generate_events(1, seed=13)[0]
    record = SimpleNamespace(
        topic="retailpulse.web_events",
        partition=2,
        offset=5,
        timestamp=int(datetime(2026, 1, 1, tzinfo=UTC).timestamp() * 1_000),
        key=event_message_key(event),
        value=serialize_web_event(event),
    )
    consumer = MagicMock()
    consumer.__iter__.return_value = iter([record])

    with patch("kafka.KafkaConsumer", return_value=consumer):
        summary = consume_web_events(
            10,
            bootstrap_servers="unused:9092",
            topic="retailpulse.web_events",
            timeout_seconds=1,
            data_root=tmp_path,
            group_id="demo-group",
            consumer_id="consumer-3",
            require_exact_count=False,
        )

    assert summary.message_count == 1
    assert summary.consumer_id == "consumer-3"
    assert summary.partitions == {2: 1}
    consumer.commit.assert_called_once()
    consumer.close.assert_called_once()


def test_cli_accepts_controlled_counts_and_seed() -> None:
    producer_args = parse_producer_args(["--count", "20", "--seed", "123"])
    consumer_args = parse_consumer_args(
        ["--count", "20", "--timeout-seconds", "5"]
    )

    assert (producer_args.count, producer_args.seed) == (20, 123)
    assert (consumer_args.count, consumer_args.timeout_seconds) == (20, 5)

    with pytest.raises(SystemExit):
        parse_producer_args(["--count", "10001"])


def test_consumer_cli_accepts_group_demo_options() -> None:
    args = parse_consumer_args(
        [
            "--count",
            "100",
            "--group-id",
            "partition-demo",
            "--consumer-id",
            "consumer-2",
            "--allow-partial",
        ]
    )

    assert args.count == 100
    assert args.group_id == "partition-demo"
    assert args.consumer_id == "consumer-2"
    assert args.allow_partial is True
