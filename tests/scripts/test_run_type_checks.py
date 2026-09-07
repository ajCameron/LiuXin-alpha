"""Contract tests for the repository's maintainability quality helper."""

from __future__ import annotations

import shlex
import shutil
import subprocess
from pathlib import Path

import pytest

from scripts.run_format_checks import format_paths

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "run_type_checks.sh"


def _dry_run(*arguments: str) -> str:
    completed = subprocess.run(
        ["bash", str(SCRIPT), "--dry-run", *arguments],
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
    assert "internal basedpyright contract step:" in output
    assert "internal mypy contract step:" in output
    complexity = next(
        line
        for line in output.splitlines()
        if line.startswith("modern complexity step:")
    )
    assert "core/program_services" in complexity
    assert "surfaces/cli/storage_commands" in complexity
    assert "surfaces/presentation.py" in complexity
    assert "surfaces/acquisition_types.py" in complexity


def test_single_checker_selection_also_selects_its_contract_probe() -> None:
    output = _dry_run("--mypy")
    assert "internal mypy contract step:" in output
    assert "internal basedpyright contract step:" not in output
    output = _dry_run("--basedpyright")
    assert "internal basedpyright contract step:" in output
    assert "internal mypy contract step:" not in output


def test_installing_quality_dependencies_requires_explicit_opt_in() -> None:
    assert "Install step:" in _dry_run("--install")


def test_skip_install_remains_a_compatibility_alias() -> None:
    assert "Install step:" not in _dry_run("--install", "--skip-install")


@pytest.mark.parametrize("args", [(), ("--mypy",), ("--basedpyright",), ("--all",)])
def test_formatting_is_checked_in_every_checker_mode(args: tuple[str, ...]) -> None:
    output = _dry_run(*args)
    step = next(
        line
        for line in output.splitlines()
        if line.startswith("modern formatting step:")
    )
    assert "scripts/run_format_checks.py" in step
    assert "--write" not in step


def test_checker_passthrough_cannot_change_formatter_mode() -> None:
    output = _dry_run("--mypy", "--", "--show-traceback")
    step = next(
        line
        for line in output.splitlines()
        if line.startswith("modern formatting step:")
    )
    assert "--show-traceback" not in step
    assert "--write" not in step


def test_format_scope_covers_every_modern_lint_target() -> None:
    output = _dry_run()
    prefix = "modern lint step: "
    line = next(line for line in output.splitlines() if line.startswith(prefix))
    targets = shlex.split(line.removeprefix(prefix))[2:]
    selected = set(format_paths(REPO_ROOT))
    assert targets
    for target in targets:
        path = REPO_ROOT / target
        candidates = set(path.rglob("*.py")) if path.is_dir() else {path}
        assert candidates and candidates <= selected, target


@pytest.mark.parametrize("format_status", [0, 1, 2])
def test_runner_executes_formatter_and_propagates_failure(
    tmp_path: Path, format_status: int
) -> None:
    root = tmp_path / "quality checkout"
    (root / "scripts").mkdir(parents=True)
    (root / ".venv/bin").mkdir(parents=True)
    shutil.copyfile(SCRIPT, root / "scripts/run_type_checks.sh")
    trace = root / "calls.txt"
    for name in ("python", "basedpyright", "mypy", "ruff"):
        tool = root / ".venv/bin" / name
        tool.write_text(
            "#!/bin/sh\n"
            f"printf '%s %s\\n' {name} \"$*\" >> {shlex.quote(str(trace))}\n"
            f'case "$1" in *run_format_checks.py) exit {format_status};; esac\n'
            "exit 0\n"
        )
        tool.chmod(0o755)
    result = subprocess.run(
        ["bash", str(root / "scripts/run_type_checks.sh"), "--mypy"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == format_status, result.stdout + result.stderr
    calls = trace.read_text().splitlines()
    assert calls[0].endswith("scripts/run_format_checks.py")
    assert "--write" not in calls[0]
    if format_status:
        assert len(calls) == 1
    else:
        assert any("annotate_file_formats.py --check" in call for call in calls)
        assert any(call.startswith("ruff check") for call in calls)
        assert any(call.startswith("mypy ") for call in calls)


def test_ci_uses_the_local_quality_runner_and_formatter_contract_tests() -> None:
    workflow = (REPO_ROOT / ".github/workflows/tests-on-push.yml").read_text()
    quality_job = workflow.split("  quality-modern:", 1)[1].split("\n  smoke:", 1)[0]
    assert "run: bash scripts/run_type_checks.sh" in quality_job
    assert "tests/scripts/test_run_format_checks.py" in quality_job
    assert "run_format_checks.py --write" not in quality_job
