"""Environment-based configuration for the local Kafka flow."""

from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv


REQUIRED_KAFKA_VARIABLES = (
    "KAFKA_BOOTSTRAP_SERVERS",
    "KAFKA_TOPIC_WEB_EVENTS",
    "KAFKA_TOPIC_PARTITIONS",
)


@dataclass(frozen=True)
class KafkaSettings:
    """Connection settings shared by the producer and consumer."""

    bootstrap_servers: str
    topic: str
    topic_partitions: int


def get_kafka_settings() -> KafkaSettings:
    """Load required Kafka settings from the environment or local .env file."""
    load_dotenv()
    values = {name: os.getenv(name) for name in REQUIRED_KAFKA_VARIABLES}
    missing = [name for name, value in values.items() if not value]
    if missing:
        raise RuntimeError(
            "Missing required environment variables: "
            f"{', '.join(missing)}. Copy .env.example to .env."
        )

    try:
        topic_partitions = int(str(values["KAFKA_TOPIC_PARTITIONS"]))
    except ValueError as error:
        raise RuntimeError("KAFKA_TOPIC_PARTITIONS must be an integer.") from error
    if topic_partitions <= 0:
        raise RuntimeError("KAFKA_TOPIC_PARTITIONS must be positive.")

    return KafkaSettings(
        bootstrap_servers=str(values["KAFKA_BOOTSTRAP_SERVERS"]),
        topic=str(values["KAFKA_TOPIC_WEB_EVENTS"]),
        topic_partitions=topic_partitions,
    )
