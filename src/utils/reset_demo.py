"""Remove generated state for a fresh local RetailPulse demo."""

from __future__ import annotations

import argparse
import shutil
import stat
from pathlib import Path
from typing import Sequence


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_LAYERS = ("raw", "bronze", "silver", "rejected", "audit", "demo")
DBT_ARTIFACTS = ("target", "logs")
PROJECT_MARKERS = ("Makefile", "docker-compose.yml", "src", "data", "dbt")


def _is_reparse_point(path: Path) -> bool:
    if path.is_symlink():
        return True
    attributes = getattr(path.lstat(), "st_file_attributes", 0)
    flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    return bool(attributes & flag)


def _reset_targets(project_root: Path | str) -> tuple[Path, ...]:
    root = Path(project_root).resolve(strict=True)
    missing = [name for name in PROJECT_MARKERS if not (root / name).exists()]
    if root.parent == root or missing:
        details = ", ".join(missing) if missing else "filesystem root"
        raise ValueError(f"Not a RetailPulse project root: {details}")

    targets = tuple(root / "data" / name for name in DATA_LAYERS) + tuple(
        root / "dbt" / name for name in DBT_ARTIFACTS
    )
    for target in targets:
        resolved = target.resolve(strict=False)
        if resolved == root or root not in resolved.parents:
            raise ValueError(f"Reset target is outside the project: {target}")
        if target.exists() and _is_reparse_point(target):
            raise ValueError(f"Reset target is a reparse point: {target}")
        if target.exists():
            for child in target.iterdir():
                if _is_reparse_point(child):
                    raise ValueError(f"Reset target contains a reparse point: {child}")
    return targets


def reset_generated_artifacts(
    project_root: Path | str = PROJECT_ROOT,
    *,
    check_only: bool = False,
) -> tuple[Path, ...]:
    """Clear fixed demo paths, preserving tracked directory placeholders."""
    targets = _reset_targets(project_root)
    if check_only:
        return targets

    for target in targets:
        if not target.exists():
            continue
        if target.parent.name == "data":
            for child in target.iterdir():
                if child.name == ".gitkeep":
                    continue
                if child.is_dir():
                    shutil.rmtree(child)
                else:
                    child.unlink()
        else:
            shutil.rmtree(target)
    return targets


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Clear generated data and dbt artifacts for a fresh demo."
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="validate reset paths without deleting files",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    targets = reset_generated_artifacts(check_only=args.check)
    action = "validated" if args.check else "cleaned"
    print(f"Demo reset paths {action}: {len(targets)}")


if __name__ == "__main__":
    main()
