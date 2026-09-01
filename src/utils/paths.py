"""Path helpers for the local RetailPulse data lake."""

from __future__ import annotations

import re
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Literal


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DATA_ROOT = PROJECT_ROOT / "data"
LAKE_EXTENSIONS = {
    "raw": "csv",
    "bronze": "parquet",
    "silver": "parquet",
    "rejected": "parquet",
}
LAKE_FILE_SUFFIXES = {"rejected": "_rejected"}
SAFE_COMPONENT_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def resolve_load_date(value: date | str | None = None) -> date:
    """Return an explicit load date or today's date in UTC."""
    if value is None:
        return datetime.now(timezone.utc).date()
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise ValueError("load_date must use YYYY-MM-DD format.") from error


def _validate_component(value: str, name: str) -> None:
    if not SAFE_COMPONENT_PATTERN.fullmatch(value):
        raise ValueError(f"Invalid {name}: {value!r}")


def build_lake_path(
    layer: Literal["raw", "bronze", "silver", "rejected"],
    source_system: str,
    table: str,
    load_date: date | str,
    data_root: Path | str = DEFAULT_DATA_ROOT,
) -> Path:
    """Build a partitioned raw, bronze, silver or rejected file path."""
    if layer not in LAKE_EXTENSIONS:
        raise ValueError(f"Unsupported lake layer: {layer!r}")
    _validate_component(source_system, "source system")
    _validate_component(table, "table")

    resolved_date = resolve_load_date(load_date)
    extension = LAKE_EXTENSIONS[layer]
    file_suffix = LAKE_FILE_SUFFIXES.get(layer, "")
    return (
        Path(data_root)
        / layer
        / source_system
        / table
        / f"load_date={resolved_date.isoformat()}"
        / f"{table}{file_suffix}.{extension}"
    )


def build_audit_path(data_root: Path | str = DEFAULT_DATA_ROOT) -> Path:
    """Return the shared quality-run audit path."""
    return Path(data_root) / "audit" / "quality_runs.parquet"
