"""Exercise the formatting scope with real Ruff, including non-mutating failures."""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
import tomllib
from pathlib import Path

import pytest

from scripts.run_format_checks import format_paths

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts/run_format_checks.py"


def _config(root: Path, paths: object, *, settings: str = "") -> None:
    (root / "pyproject.toml").write_text(
        '[tool.ruff]\ntarget-version = "py312"\nline-length = 88\n'
        f'{settings}\n[tool.ruff.format]\nline-ending = "lf"\n'
        f"[tool.liuxin.format]\npaths = {json.dumps(paths)}\n"
    )


@pytest.fixture
def checkout(tmp_path: Path) -> Path:
    root = tmp_path / "checkout with spaces"
    (root / "scripts").mkdir(parents=True)
    (root / ".venv/bin").mkdir(parents=True)
    (root / "src").mkdir()
    (root / "src/example.py").write_text("answer = 42\n")
    shutil.copyfile(SCRIPT, root / "scripts/run_format_checks.py")
    (root / ".venv/bin/ruff").symlink_to(REPO_ROOT / ".venv/bin/ruff")
    _config(root, ["src"])
    return root


def _run(root: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(root / "scripts/run_format_checks.py"), *args],
        cwd=root.parent,
        capture_output=True,
        text=True,
        check=False,
    )


def test_default_check_works_outside_checkout_and_does_not_write(
    checkout: Path,
) -> None:
    source = checkout / "src/example.py"
    before = source.read_bytes()
    result = _run(checkout)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "Formatting check: 1 Python files" in result.stdout
    assert source.read_bytes() == before
    assert not (checkout / ".ruff_cache").exists()


def test_unformatted_source_fails_without_rewriting(checkout: Path) -> None:
    source = checkout / "src/example.py"
    source.write_bytes(b"answer=  42\r\n")
    before = source.read_bytes()
    result = _run(checkout)
    assert result.returncode == 1, result.stdout + result.stderr
    assert "example.py" in result.stdout + result.stderr
    assert source.read_bytes() == before


def test_write_is_explicit_bounded_and_idempotent(checkout: Path) -> None:
    source = checkout / "src/example.py"
    source.write_bytes(b"answer=  42\r\n")
    unrelated = checkout / "legacy.py"
    unrelated.write_text("not even valid Python !\n")
    before = unrelated.read_bytes()
    result = _run(checkout, "--write")
    assert result.returncode == 0, result.stdout + result.stderr
    assert source.read_bytes() == b"answer = 42\n"
    assert unrelated.read_bytes() == before
    assert _run(checkout).returncode == 0
    assert _run(checkout, "--write").returncode == 0
    assert source.read_bytes() == b"answer = 42\n"


@pytest.mark.parametrize("args", [(), ("--write",)])
def test_invalid_python_is_a_failure_not_an_empty_success(
    checkout: Path, args: tuple[str, ...]
) -> None:
    source = checkout / "src/example.py"
    source.write_text("def broken(:\n")
    before = source.read_bytes()
    result = _run(checkout, *args)
    assert result.returncode != 0
    assert "example.py" in result.stdout + result.stderr
    assert source.read_bytes() == before


def test_new_nested_modules_are_automatically_checked(checkout: Path) -> None:
    nested = checkout / "src/new_family"
    nested.mkdir()
    (nested / "new_owner.py").write_text("answer=42\n")
    result = _run(checkout)
    assert result.returncode == 1
    assert "new_owner.py" in result.stdout + result.stderr


def test_manifest_is_authoritative_over_ignore_and_nested_config(
    checkout: Path,
) -> None:
    _config(checkout, ["src"], settings='exclude = ["src"]\nforce-exclude = true')
    (checkout / ".gitignore").write_text("src/\n")
    (checkout / "src/ruff.toml").write_text('[format]\nquote-style = "single"\n')
    (checkout / "src/example.py").write_text("answer = 'hello'\n")
    result = _run(checkout)
    assert result.returncode == 1, result.stdout + result.stderr
    assert "example.py" in result.stdout + result.stderr


def test_overlapping_scopes_are_deduplicated(checkout: Path) -> None:
    _config(checkout, ["src/example.py", "src", "src/example.py"])
    assert format_paths(checkout) == (checkout / "src/example.py",)


@pytest.mark.parametrize("entries", [[], "src", [""], [5], [True]])
def test_invalid_scope_is_rejected_before_ruff(checkout: Path, entries: object) -> None:
    _config(checkout, entries)
    result = _run(checkout)
    assert result.returncode == 2
    assert "Invalid formatting scope" in result.stderr
    assert "Formatting check:" not in result.stdout


@pytest.mark.parametrize(
    "entry", ["missing.py", "empty", "notes.md", ".", "../outside"]
)
def test_bad_target_cannot_silently_shrink_scope(checkout: Path, entry: str) -> None:
    (checkout / "empty").mkdir()
    (checkout / "notes.md").write_text("Not Python.\n")
    _config(checkout, ["src", entry])
    source = checkout / "src/example.py"
    source.write_text("answer=42\n")
    with pytest.raises(ValueError):
        format_paths(checkout)
    assert _run(checkout, "--write").returncode == 2
    assert source.read_text() == "answer=42\n"


def test_absolute_paths_are_rejected_even_inside_checkout(checkout: Path) -> None:
    _config(checkout, [str(checkout / "src")])
    with pytest.raises(ValueError, match="repository-relative"):
        format_paths(checkout)


def test_symlink_cannot_expand_write_scope_outside_checkout(checkout: Path) -> None:
    outside = checkout.parent / "outside.py"
    outside.write_text("outside=42\n")
    (checkout / "src/outside.py").symlink_to(outside)
    result = _run(checkout, "--write")
    assert result.returncode == 2
    assert "escapes the repository" in result.stderr
    assert outside.read_text() == "outside=42\n"


@pytest.mark.parametrize("contents", ["", "[tool.liuxin.format\n"])
def test_missing_or_malformed_config_is_an_error(checkout: Path, contents: str) -> None:
    (checkout / "pyproject.toml").write_text(contents)
    assert _run(checkout).returncode == 2


def test_missing_ruff_explains_local_dependency_requirement(checkout: Path) -> None:
    (checkout / ".venv/bin/ruff").unlink()
    result = _run(checkout)
    assert result.returncode == 2
    assert "install the typing extra" in result.stderr


def test_wrong_ruff_version_fails_without_rewriting(checkout: Path) -> None:
    _config(checkout, ["src"], settings='required-version = "==0.0.0"')
    source = checkout / "src/example.py"
    source.write_text("answer=42\n")
    result = _run(checkout, "--write")
    assert result.returncode != 0
    assert "required version" in result.stderr.lower()
    assert source.read_text() == "answer=42\n"


def test_dry_run_neither_requires_tools_nor_formats_sources(checkout: Path) -> None:
    (checkout / ".venv/bin/ruff").unlink()
    source = checkout / "src/example.py"
    source.write_text("answer=42\n")
    result = _run(checkout, "--write", "--dry-run")
    assert result.returncode == 0
    assert "ruff' format" in result.stdout
    assert "--check" not in result.stdout
    assert source.read_text() == "answer=42\n"


def test_production_scope_covers_extracted_owners_and_regression_tests() -> None:
    selected = set(format_paths(REPO_ROOT))
    for directory in (
        "src/LiuXin_alpha/core/program_services",
        "src/LiuXin_alpha/core/program_endpoints",
        "src/LiuXin_alpha/surfaces/cli/storage_commands",
        "src/LiuXin_alpha/storage/storage_manager/mixins",
    ):
        assert set((REPO_ROOT / directory).rglob("*.py")) <= selected
    for relative in (
        "src/LiuXin_alpha/surfaces/presentation.py",
        "src/LiuXin_alpha/surfaces/acquisition_types.py",
        "tests/core/test_core_program_api.py",
        "tests/core/test_core_application_api.py",
        "tests/surfaces/test_cli_storage.py",
        "tests/surfaces/test_cli_init_ingest.py",
        "tests/surfaces/test_cli_operator_hardening.py",
        "tests/surfaces/test_cli_operational_families.py",
        "tests/surfaces/test_shared_surface_dependencies.py",
        "tests/typing/internal_contracts.py",
        "scripts/run_format_checks.py",
        "tests/scripts/test_run_format_checks.py",
    ):
        assert REPO_ROOT / relative in selected
    assert not any("file_formats" in path.parts for path in selected)


def test_ruff_install_and_runtime_requirements_use_the_same_exact_version() -> None:
    config = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text())
    requirement = config["tool"]["ruff"]["required-version"]
    assert requirement.startswith("==") and "*" not in requirement
    assert f"ruff{requirement}" in config["project"]["optional-dependencies"]["typing"]
    assert config["tool"]["ruff"]["format"]["line-ending"] == "lf"
