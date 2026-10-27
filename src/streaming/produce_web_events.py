"""Publish a finite deterministic batch of synthetic web events to Kafka."""

from __future__ import annotations

import argparse
import logging
from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass

from sqlalchemy import Engine

from src.streaming.config import get_kafka_settings
from src.streaming.web_events import (
    DEFAULT_EVENT_COUNT,
    DEFAULT_SEED,
    WebEvent,
    event_message_key,
    generate_web_events,
    serialize_web_event,
)
from src.utils.database import get_engine


LOGGER = logging.getLogger(__name__)
MAX_EVENT_COUNT = 10_000


@dataclass(frozen=True)
class ProductionSummary:
    """Summary of a finite producer execution."""

    event_count: int
    topic: str
    partitions: dict[int, int]


@dataclass(frozen=True)
class SourceReferenceIds:
    """Identifiers read from the operational PostgreSQL source."""

    customer_ids: tuple[int, ...]
    product_ids: tuple[int, ...]


def load_source_reference_ids(engine: Engine | None = None) -> SourceReferenceIds:
    """Read the customer and product IDs that events may reference."""
    database_engine = engine if engine is not None else get_engine()
    with database_engine.connect() as connection:
        customer_ids = tuple(
            int(value)
            for value in connection.exec_driver_sql(
                "SELECT customer_id FROM customers ORDER BY customer_id"
            ).scalars()
        )
        product_ids = tuple(
            int(value)
            for value in connection.exec_driver_sql(
                "SELECT product_id FROM products ORDER BY product_id"
            ).scalars()
        )

    if not customer_ids or not product_ids:
        raise RuntimeError(
            "PostgreSQL source must contain customers and products. "
            "Run make init-db and make seed-db first."
        )

    return SourceReferenceIds(
        customer_ids=customer_ids,
        product_ids=product_ids,
    )


def _bounded_positive_int(value: str) -> int:
    parsed = int(value)
    if not 1 <= parsed <= MAX_EVENT_COUNT:
        raise argparse.ArgumentTypeError(
            f"value must be between 1 and {MAX_EVENT_COUNT}"
        )
    return parsed


def ensure_topic(
    *, bootstrap_servers: str, topic: str, partition_count: int
) -> None:
    """Create the topic or increase it to the configured partition count."""
    from kafka import KafkaAdminClient
    from kafka.admin import NewPartitions, NewTopic

    if partition_count <= 0:
        raise ValueError("partition_count must be positive")

    admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    try:
        if topic not in admin.list_topics():
            admin.create_topics(
                [
                    NewTopic(
                        name=topic,
                        num_partitions=partition_count,
                        replication_factor=1,
                    )
                ],
                wait_for_metadata=True,
            )
            return

        description = next(
            (
                item
                for item in admin.describe_topics([topic])
                if item.get("name", item.get("topic")) == topic
            ),
            None,
        )
        if description is None:
            raise RuntimeError(f"Kafka did not return metadata for topic {topic!r}")
        current_count = len(description["partitions"])
        if current_count < partition_count:
            admin.create_partitions(
                {topic: NewPartitions(total_count=partition_count)}
            )
        elif current_count > partition_count:
            raise RuntimeError(
                f"Topic {topic!r} already has {current_count} partitions; "
                "Kafka cannot reduce a topic's partition count."
            )
    finally:
        admin.close()


def publish_web_events(
    events: Sequence[WebEvent],
    *,
    bootstrap_servers: str,
    topic: str,
    partition_count: int,
) -> ProductionSummary:
    """Publish events and wait for broker acknowledgements."""
    from kafka import KafkaProducer

    ensure_topic(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        partition_count=partition_count,
    )
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        acks="all",
        retries=3,
    )
    partition_counts: Counter[int] = Counter()
    try:
        for event in events:
            metadata = producer.send(
                topic,
                key=event_message_key(event),
                value=serialize_web_event(event),
            ).get(timeout=10)
            partition_counts[metadata.partition] += 1
        producer.flush(timeout=10)
    finally:
        producer.close(timeout=10)

    return ProductionSummary(
        event_count=len(events),
        topic=topic,
        partitions=dict(sorted(partition_counts.items())),
    )


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Produce synthetic web events.")
    parser.add_argument(
        "--count",
        type=_bounded_positive_int,
        default=DEFAULT_EVENT_COUNT,
        help=f"number of events to publish (1-{MAX_EVENT_COUNT})",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=DEFAULT_SEED,
        help="seed used for deterministic event generation",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    logging.getLogger("kafka").setLevel(logging.ERROR)
    settings = get_kafka_settings()
    reference_ids = load_source_reference_ids()
    events = generate_web_events(
        args.count,
        customer_ids=reference_ids.customer_ids,
        product_ids=reference_ids.product_ids,
        seed=args.seed,
    )
    summary = publish_web_events(
        events,
        bootstrap_servers=settings.bootstrap_servers,
        topic=settings.topic,
        partition_count=settings.topic_partitions,
    )
    LOGGER.info(
        "Published %s events to %s; partitions=%s; seed=%s",
        summary.event_count,
        summary.topic,
        summary.partitions,
        args.seed,
    )


if __name__ == "__main__":
    main()
