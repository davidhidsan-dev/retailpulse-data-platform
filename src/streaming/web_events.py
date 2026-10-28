"""Simple deterministic web-event model and JSON serialization."""

from __future__ import annotations

import json
import random
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from numbers import Integral
from uuid import UUID


ALLOWED_EVENT_TYPES = ("product_view", "add_to_cart")
DEFAULT_EVENT_START = datetime(2026, 1, 1, tzinfo=UTC)
DEFAULT_EVENT_COUNT = 20
DEFAULT_SEED = 42


@dataclass(frozen=True)
class WebEvent:
    """Business payload published to the web-events Kafka topic."""

    event_id: str
    event_type: str
    occurred_at: datetime
    session_id: str
    customer_id: int | None
    product_id: int

    def __post_init__(self) -> None:
        if self.event_type not in ALLOWED_EVENT_TYPES:
            raise ValueError(f"Unsupported event_type: {self.event_type!r}")
        if self.occurred_at.tzinfo is None:
            raise ValueError("occurred_at must be timezone-aware")
        if self.occurred_at.utcoffset() != timedelta(0):
            raise ValueError("occurred_at must be in UTC")
        if not self.session_id:
            raise ValueError("session_id cannot be empty")
        if self.customer_id is not None and self.customer_id <= 0:
            raise ValueError("customer_id must be positive when present")
        if self.product_id <= 0:
            raise ValueError("product_id must be positive")
        UUID(self.event_id)

    def to_dict(self) -> dict[str, str | int | None]:
        """Return a JSON-ready representation with an explicit UTC suffix."""
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "occurred_at": self.occurred_at.isoformat().replace("+00:00", "Z"),
            "session_id": self.session_id,
            "customer_id": self.customer_id,
            "product_id": self.product_id,
        }


def _validate_reference_ids(name: str, values: Sequence[int]) -> tuple[int, ...]:
    """Return a non-empty tuple of positive integer source identifiers."""
    if not values:
        raise ValueError(f"{name} cannot be empty")

    invalid = [
        value
        for value in values
        if isinstance(value, bool) or not isinstance(value, Integral) or value <= 0
    ]
    if invalid:
        raise ValueError(f"{name} must contain only positive integers")

    return tuple(int(value) for value in values)


def generate_web_events(
    count: int,
    *,
    customer_ids: Sequence[int],
    product_ids: Sequence[int],
    seed: int = DEFAULT_SEED,
    start_at: datetime = DEFAULT_EVENT_START,
) -> list[WebEvent]:
    """Generate a reproducible, intentionally small web-event simulation."""
    if count < 0:
        raise ValueError("count must be non-negative")
    if start_at.tzinfo is None or start_at.utcoffset() != timedelta(0):
        raise ValueError("start_at must be timezone-aware and in UTC")
    if count == 0:
        return []

    available_customer_ids = _validate_reference_ids("customer_ids", customer_ids)
    available_product_ids = _validate_reference_ids("product_ids", product_ids)
    generator = random.Random(seed)
    session_count = max(1, min(count, max(2, count // 4)))
    seed_label = f"neg-{abs(seed)}" if seed < 0 else str(seed)
    sessions = [
        f"session-seed-{seed_label}-{number:06d}"
        for number in range(1, session_count + 1)
    ]
    session_customers = {
        session_id: (
            None
            if generator.random() < 0.25
            else generator.choice(available_customer_ids)
        )
        for session_id in sessions
    }
    event_sessions = sessions.copy()
    event_sessions.extend(
        generator.choice(sessions) for _ in range(count - session_count)
    )
    generator.shuffle(event_sessions)
    events: list[WebEvent] = []

    for index, session_id in enumerate(event_sessions):
        event_uuid = UUID(int=generator.getrandbits(128), version=4)
        events.append(
            WebEvent(
                event_id=str(event_uuid),
                event_type=generator.choice(ALLOWED_EVENT_TYPES),
                occurred_at=start_at
                + timedelta(seconds=(index * 5) + generator.randint(0, 4)),
                session_id=session_id,
                customer_id=session_customers[session_id],
                product_id=generator.choice(available_product_ids),
            )
        )

    return events


def serialize_web_event(event: WebEvent) -> bytes:
    """Serialize one event as compact UTF-8 JSON bytes."""
    return json.dumps(
        event.to_dict(), ensure_ascii=False, separators=(",", ":")
    ).encode("utf-8")


def deserialize_web_event(payload: bytes) -> WebEvent:
    """Deserialize UTF-8 JSON bytes into a validated WebEvent."""
    values = json.loads(payload.decode("utf-8"))
    occurred_at = datetime.fromisoformat(values["occurred_at"].replace("Z", "+00:00"))
    return WebEvent(
        event_id=values["event_id"],
        event_type=values["event_type"],
        occurred_at=occurred_at,
        session_id=values["session_id"],
        customer_id=values["customer_id"],
        product_id=values["product_id"],
    )


def event_message_key(event: WebEvent) -> bytes:
    """Use session_id as the Kafka key to preserve per-session ordering."""
    return event.session_id.encode("utf-8")
