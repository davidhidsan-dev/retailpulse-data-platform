from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize(
    "relative_path",
    [
        "dags",
        "src/extract",
        "src/ingest",
        "src/clean",
        "src/quality",
        "src/audit",
        "src/utils",
        "src/synthetic_data",
        "dbt",
        "sql",
        "data/raw",
        "data/bronze",
        "data/silver",
        "data/gold",
        "docs",
        "dashboards/screenshots",
        "notebooks",
    ],
)
def test_expected_directory_exists(relative_path: str) -> None:
    """Keep the Phase 0 repository contract explicit."""
    assert (PROJECT_ROOT / relative_path).is_dir()
