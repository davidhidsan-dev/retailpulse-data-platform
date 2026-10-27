"""Consume a finite Kafka batch and persist raw event envelopes as JSONL."""

from __future__ import annotations

import argparse
import json
import logging
from collections import Counter, defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from uuid import uuid4

from src.streaming.config import get_kafka_settings
from src.streaming.web_events import DEFAULT_EVENT_COUNT
from src.utils.paths import DEFAULT_DATA_ROOT


LOGGER = logging.getLogger(__name__)
DEFAULT_TIMEOUT_SECONDS = 15
CONSUMER_GROUP_ID = "retailpulse-raw-web-events-v1"
DEFAULT_CONSUMER_ID = "retailpulse-consumer-1"


@dataclass(frozen=True)
class ConsumedKafkaMessage:
    """Kafka payload and broker metadata needed by raw storage."""

    topic: str
    partition: int
    offset: int
    timestamp_ms: int
    key: bytes | None
    value: bytes


@dataclass(frozen=True)
class ConsumptionSummary:
    """Summary of a finite consumer execution."""

    message_count: int
    topic: str
    consumer_id: str
    partitions: dict[int, int]
    output_paths: tuple[Path, ...]


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be a positive integer")
    return parsed


def _utc_iso_from_milliseconds(timestamp_ms: int) -> str:
    timestamp = datetime.fromtimestamp(timestamp_ms / 1_000, tz=UTC)
    return timestamp.isoformat().replace("+00:00", "Z")


def _parse_event_date(payload: dict[str, object]) -> date:
    occurred_at = payload.get("occurred_at")
    if not isinstance(occurred_at, str):
        raise ValueError("Kafka payload must contain a string occurred_at")
    timestamp = datetime.fromisoformat(occurred_at.replace("Z", "+00:00"))
    if timestamp.tzinfo is None:
        raise ValueError("Kafka payload occurred_at must be timezone-aware")
    return timestamp.astimezone(UTC).date()


def build_raw_envelope(message: ConsumedKafkaMessage) -> dict[str, object]:
    """Keep the original event payload separate from Kafka metadata."""
    payload = json.loads(message.value.decode("utf-8"))
    key = message.key.decode("utf-8") if message.key is not None else None
    return {
        "kafka": {
            "topic": message.topic,
            "partition": message.partition,
            "offset": message.offset,
            "timestamp": _utc_iso_from_milliseconds(message.timestamp_ms),
            "key": key,
        },
        "payload": payload,
    }


def build_raw_event_path(
    event_date: date,
    run_id: str,
    data_root: Path | str = DEFAULT_DATA_ROOT,
) -> Path:
    """Build a unique path for one raw Kafka event-date partition."""
    return (
        Path(data_root)
        / "raw"
        / "kafka"
        / "web_events"
        / f"event_date={event_date.isoformat()}"
        / f"web_events_{run_id}.jsonl"
    )


def write_raw_messages(
    messages: Iterable[ConsumedKafkaMessage],
    *,
    data_root: Path | str = DEFAULT_DATA_ROOT,
    run_id: str | None = None,
) -> tuple[Path, ...]:
    """Write JSONL envelopes grouped by the event's UTC date."""
    resolved_run_id = run_id or str(uuid4())
    envelopes_by_date: dict[date, list[dict[str, object]]] = defaultdict(list)

    for message in messages:
        envelope = build_raw_envelope(message)
        payload = envelope["payload"]
        if not isinstance(payload, dict):
            raise ValueError("Kafka event payload must be a JSON object")
        envelopes_by_date[_parse_event_date(payload)].append(envelope)

    output_paths: list[Path] = []
    for event_date in sorted(envelopes_by_date):
        output_path = build_raw_event_path(
            event_date, resolved_run_id, data_root
        )
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("x", encoding="utf-8", newline="\n") as output:
            for envelope in envelopes_by_date[event_date]:
                output.write(
                    json.dumps(
                        envelope,
                        ensure_ascii=False,
                        separators=(",", ":"),
                    )
                    + "\n"
                )
        output_paths.append(output_path)

    return tuple(output_paths)


def consume_web_events(
    count: int,
    *,
    bootstrap_servers: str,
    topic: str,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    data_root: Path | str = DEFAULT_DATA_ROOT,
    group_id: str = CONSUMER_GROUP_ID,
    consumer_id: str = DEFAULT_CONSUMER_ID,
    require_exact_count: bool = True,
) -> ConsumptionSummary:
    """Consume a finite batch, persist it, then commit offsets."""
    from kafka import KafkaConsumer

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        client_id=consumer_id,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=timeout_seconds * 1_000,
    )
    messages: list[ConsumedKafkaMessage] = []
    try:
        for record in consumer:
            messages.append(
                ConsumedKafkaMessage(
                    topic=record.topic,
                    partition=record.partition,
                    offset=record.offset,
                    timestamp_ms=record.timestamp,
                    key=record.key,
                    value=record.value,
                )
            )
            if len(messages) == count:
                break

        if require_exact_count and len(messages) != count:
            raise RuntimeError(
                f"Expected {count} messages but received {len(messages)} "
                f"before the {timeout_seconds}s timeout."
            )

        output_paths = write_raw_messages(messages, data_root=data_root)
        consumer.commit()
    finally:
        consumer.close()

    partition_counts = Counter(message.partition for message in messages)
    return ConsumptionSummary(
        message_count=len(messages),
        topic=topic,
        consumer_id=consumer_id,
        partitions=dict(sorted(partition_counts.items())),
        output_paths=output_paths,
    )


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Consume a finite web-event batch into raw JSONL."
    )
    parser.add_argument(
        "--count",
        type=_positive_int,
        default=DEFAULT_EVENT_COUNT,
        help="number of messages that must be consumed",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=_positive_int,
        default=DEFAULT_TIMEOUT_SECONDS,
        help="maximum idle wait before the controlled run fails",
    )
    parser.add_argument(
        "--group-id",
        default=CONSUMER_GROUP_ID,
        help="Kafka consumer group shared by cooperating consumers",
    )
    parser.add_argument(
        "--consumer-id",
        default=DEFAULT_CONSUMER_ID,
        help="client identifier used to distinguish this consumer instance",
    )
    parser.add_argument(
        "--allow-partial",
        action="store_true",
        help="accept fewer messages after the idle timeout for group demos",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    logging.getLogger("kafka").setLevel(logging.ERROR)
    settings = get_kafka_settings()
    summary = consume_web_events(
        args.count,
        bootstrap_servers=settings.bootstrap_servers,
        topic=settings.topic,
        timeout_seconds=args.timeout_seconds,
        group_id=args.group_id,
        consumer_id=args.consumer_id,
        require_exact_count=not args.allow_partial,
    )
    LOGGER.info(
        "Consumer %s read %s events from %s; partitions=%s; raw files=%s",
        summary.consumer_id,
        summary.message_count,
        summary.topic,
        summary.partitions,
        ", ".join(str(path) for path in summary.output_paths),
    )


if __name__ == "__main__":
    main()
