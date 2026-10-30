"""Tests for incremental Kafka silver loading into PostgreSQL."""

from __future__ import annotations

from contextlib import AbstractContextManager
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from src.streaming.raw_to_bronze import build_bronze_event_path
from src.streaming.validate_web_events import (
    build_event_quality_path,
    build_streaming_audit_path,
)
from src.warehouse.load_streaming_silver_to_warehouse import (
    WAREHOUSE_COLUMNS,
    load_streaming_silver_to_warehouse,
    parse_args,
    read_approved_streaming_silver,
)


EVENT_DATE = date(2026, 1, 1)
RUN_ID = "event-run"
INGESTION_ID = "event-ingestion"
QUALITY_RUN_ID = "event-quality"


class FakeScalarResult:
    def __init__(self, value: int) -> None:
        self.value = value

    def scalar_one(self) -> int:
        return self.value


class FakeConnection:
    def __init__(self, inserted_rows: int = 2) -> None:
        self.executed_sql: list[str] = []
        self.inserted_rows = inserted_rows

    def exec_driver_sql(self, sql: str) -> FakeScalarResult:
        self.executed_sql.append(sql)
        return FakeScalarResult(
            self.inserted_rows if sql.startswith("WITH inserted AS") else 0
        )


class FakeTransaction(AbstractContextManager[FakeConnection]):
    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection

    def __enter__(self) -> FakeConnection:
        return self.connection

    def __exit__(self, *args: object) -> None:
        return None


class FakeEngine:
    def __init__(self, inserted_rows: int = 2) -> None:
        self.connection = FakeConnection(inserted_rows)
        self.begin_calls = 0

    def begin(self) -> FakeTransaction:
        self.begin_calls += 1
        return FakeTransaction(self.connection)


def _event_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "event_id": ["event-1", "event-2"],
            "event_type": ["product_view", "add_to_cart"],
            "occurred_at": pd.to_datetime(
                ["2026-01-01T10:00:00Z", "2026-01-01T10:01:00Z"], utc=True
            ),
            "session_id": ["session-1", "session-1"],
            "customer_id": pd.Series([1, 1], dtype="Int64"),
            "product_id": [10, 20],
            "kafka_topic": ["retailpulse.web_events"] * 2,
            "kafka_partition": [0, 0],
            "kafka_offset": [5, 6],
            "kafka_timestamp": pd.to_datetime(
                ["2026-01-01T10:00:01Z", "2026-01-01T10:01:01Z"], utc=True
            ),
            "kafka_key": ["session-1", "session-1"],
            "ingestion_id": [INGESTION_ID] * 2,
            "ingested_at": pd.to_datetime(
                ["2026-01-01T10:05:00Z"] * 2, utc=True
            ),
            "source_system": ["kafka"] * 2,
            "source_table": ["web_events"] * 2,
            "source_file": [f"web_events_{RUN_ID}.jsonl"] * 2,
            "quality_run_id": [QUALITY_RUN_ID] * 2,
            "quality_checked_at": pd.to_datetime(
                ["2026-01-01T10:06:00Z"] * 2, utc=True
            ),
        }
    )


def _write_approved_run(
    data_root: Path,
    *,
    status: str = "passed",
    bronze_ingestion_id: str = INGESTION_ID,
) -> tuple[Path, Path]:
    silver = _event_frame()
    silver_path = build_event_quality_path(
        "silver", EVENT_DATE, RUN_ID, data_root
    )
    silver_path.parent.mkdir(parents=True, exist_ok=True)
    silver.to_parquet(silver_path, index=False)

    bronze = silver.drop(columns=["quality_run_id", "quality_checked_at"])
    bronze["ingestion_id"] = bronze_ingestion_id
    bronze_path = build_bronze_event_path(EVENT_DATE, RUN_ID, data_root)
    bronze_path.parent.mkdir(parents=True, exist_ok=True)
    bronze.to_parquet(bronze_path, index=False)

    audit_path = build_streaming_audit_path(data_root)
    audit_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "quality_run_id": QUALITY_RUN_ID,
                "quality_checked_at": pd.Timestamp("2026-01-01T10:06:00Z"),
                "event_date": EVENT_DATE.isoformat(),
                "run_id": RUN_ID,
                "ingestion_id": INGESTION_ID,
                "source_system": "kafka",
                "source_table": "web_events",
                "input_rows": 2,
                "valid_rows": 2,
                "rejected_rows": 0,
                "rejection_rate": 0.0,
                "status": status,
                "error_message": None,
            }
        ]
    ).to_parquet(audit_path, index=False)
    return silver_path, bronze_path


def test_load_appends_approved_events_with_conflict_protection(
    tmp_path: Path,
) -> None:
    silver_path, _ = _write_approved_run(tmp_path)
    schema_path = tmp_path / "warehouse_schema.sql"
    schema_path.write_text(
        "CREATE SCHEMA IF NOT EXISTS warehouse_source;", encoding="utf-8"
    )
    engine = FakeEngine(inserted_rows=2)
    writes: list[tuple[str, pd.DataFrame, object]] = []

    def record_write(
        dataframe: pd.DataFrame, table: str, connection: object
    ) -> None:
        writes.append((table, dataframe.copy(), connection))

    result = load_streaming_silver_to_warehouse(
        EVENT_DATE,
        run_id=RUN_ID,
        engine=engine,
        data_root=tmp_path,
        schema_path=schema_path,
        table_writer=record_write,
    )

    assert engine.begin_calls == 1
    assert len(writes) == 1
    assert writes[0][0].startswith("_load_web_events_")
    assert tuple(writes[0][1].columns) == WAREHOUSE_COLUMNS
    insert_sql = next(
        sql for sql in engine.connection.executed_sql if sql.startswith("WITH")
    )
    assert 'ON CONFLICT ("event_id") DO NOTHING' in insert_sql
    assert result.silver_path == silver_path
    assert result.input_rows == 2
    assert result.inserted_rows == 2
    assert result.duplicate_rows == 0
    assert result.warehouse_relation == "warehouse_source.web_events"


def test_load_reports_events_already_present_as_duplicates(tmp_path: Path) -> None:
    _write_approved_run(tmp_path)
    schema_path = tmp_path / "warehouse_schema.sql"
    schema_path.write_text("CREATE SCHEMA warehouse_source;", encoding="utf-8")

    result = load_streaming_silver_to_warehouse(
        EVENT_DATE,
        run_id=RUN_ID,
        engine=FakeEngine(inserted_rows=0),
        data_root=tmp_path,
        schema_path=schema_path,
        table_writer=lambda _frame, _table, _connection: None,
    )

    assert result.inserted_rows == 0
    assert result.duplicate_rows == 2


def test_load_refuses_silver_after_bronze_is_replaced(tmp_path: Path) -> None:
    _write_approved_run(tmp_path, bronze_ingestion_id="new-ingestion")

    with pytest.raises(ValueError, match="current bronze ingestion"):
        read_approved_streaming_silver(
            EVENT_DATE,
            data_root=tmp_path,
            run_id=RUN_ID,
        )


def test_load_refuses_failed_quality_run(tmp_path: Path) -> None:
    _write_approved_run(tmp_path, status="failed")

    with pytest.raises(ValueError, match="did not approve"):
        read_approved_streaming_silver(
            EVENT_DATE,
            data_root=tmp_path,
            run_id=RUN_ID,
        )


def test_load_refuses_modified_silver_provenance(tmp_path: Path) -> None:
    silver_path, _ = _write_approved_run(tmp_path)
    silver = pd.read_parquet(silver_path)
    silver["source_file"] = "another-run.jsonl"
    silver.to_parquet(silver_path, index=False)

    with pytest.raises(ValueError, match="source_file"):
        read_approved_streaming_silver(
            EVENT_DATE,
            data_root=tmp_path,
            run_id=RUN_ID,
        )


def test_streaming_warehouse_cli_parses_event_date_and_run_id() -> None:
    args = parse_args(
        ["--event-date", EVENT_DATE.isoformat(), "--run-id", RUN_ID]
    )

    assert args.event_date == EVENT_DATE
    assert args.run_id == RUN_ID
