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
        "src/warehouse",
        "dbt",
        "sql",
        "data/raw",
        "data/bronze",
        "data/silver",
        "data/rejected",
        "data/audit",
        "data/gold",
        "docs",
        "dashboards/screenshots",
        "notebooks",
    ],
)
def test_expected_directory_exists(relative_path: str) -> None:
    """Keep the Phase 0 repository contract explicit."""
    assert (PROJECT_ROOT / relative_path).is_dir()


@pytest.mark.parametrize(
    "relative_path",
    [
        "README.md",
        "docs/architecture.md",
        "docs/runbook.md",
        "docs/technical_decisions.md",
        "docs/v1_validation_checklist.md",
        "docs/warehouse_model.md",
    ],
)
def test_v1_documentation_exists_and_is_not_empty(relative_path: str) -> None:
    """Keep the v1 documentation contract without coupling tests to wording."""
    document = PROJECT_ROOT / relative_path

    assert document.is_file()
    assert document.read_text(encoding="utf-8").strip()
