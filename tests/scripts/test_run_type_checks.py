"""Contract tests for the repository's maintainability quality helper."""

from __future__ import annotations

import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "run_type_checks.sh"


def _dry_run(*arguments: str) -> str:
    completed = subprocess.run(
        ["bash", str(SCRIPT), *arguments, "--dry-run"],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout


def test_default_run_is_offline_and_enforces_complexity() -> None:
    output = _dry_run()

    assert "Install step:" not in output
    assert "modern complexity step:" in output
    assert "lint.mccabe.max-complexity=10" in output
    assert "storage-manager complexity step:" in output
    assert "lint.mccabe.max-complexity=15" in output


def test_installing_quality_dependencies_requires_explicit_opt_in() -> None:
    assert "Install step:" in _dry_run("--install")


def test_skip_install_remains_a_compatibility_alias() -> None:
    assert "Install step:" not in _dry_run("--install", "--skip-install")
