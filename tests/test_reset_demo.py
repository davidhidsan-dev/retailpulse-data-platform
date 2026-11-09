"""Tests for the explicitly destructive local demo reset."""

from pathlib import Path

import pytest

from src.utils.reset_demo import reset_generated_artifacts


def _build_project(tmp_path: Path) -> Path:
    for filename in ("Makefile", "docker-compose.yml"):
        (tmp_path / filename).write_text("placeholder", encoding="utf-8")
    for directory in ("src", "data", "dbt"):
        (tmp_path / directory).mkdir()
    return tmp_path


def test_reset_removes_only_generated_artifacts_and_preserves_gitkeep(
    tmp_path: Path,
) -> None:
    project = _build_project(tmp_path)
    raw = project / "data" / "raw"
    raw.mkdir()
    (raw / ".gitkeep").write_text("", encoding="utf-8")
    generated_partition = raw / "postgres" / "customers"
    generated_partition.mkdir(parents=True)
    (generated_partition / "customers.csv").write_text("1,Ana", encoding="utf-8")

    demo = project / "data" / "demo"
    demo.mkdir()
    (demo / "bad.parquet").write_text("generated", encoding="utf-8")

    dbt_target = project / "dbt" / "target"
    dbt_target.mkdir()
    (dbt_target / "manifest.json").write_text("{}", encoding="utf-8")
    dbt_logs = project / "dbt" / "logs"
    dbt_logs.mkdir()
    (dbt_logs / "dbt.log").write_text("generated", encoding="utf-8")

    protected = project / "dbt" / "profiles.yml.example"
    protected.write_text("retailpulse:", encoding="utf-8")

    reset_generated_artifacts(project)

    assert (raw / ".gitkeep").exists()
    assert not generated_partition.exists()
    assert demo.exists() and not any(demo.iterdir())
    assert not dbt_target.exists()
    assert not dbt_logs.exists()
    assert protected.exists()


def test_reset_rejects_a_directory_that_is_not_the_project(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="Not a RetailPulse project root"):
        reset_generated_artifacts(tmp_path, check_only=True)


def test_preflight_failure_happens_before_any_deletion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = _build_project(tmp_path)
    raw = project / "data" / "raw"
    raw.mkdir()
    original = raw / "keep-on-failure.csv"
    original.write_text("important", encoding="utf-8")
    unsafe = project / "data" / "bronze" / "unsafe-link"
    unsafe.mkdir(parents=True)

    monkeypatch.setattr(
        "src.utils.reset_demo._is_reparse_point",
        lambda path: path.name == "unsafe-link",
    )

    with pytest.raises(ValueError, match="reparse point"):
        reset_generated_artifacts(project)

    assert original.exists()
