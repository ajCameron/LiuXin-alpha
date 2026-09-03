#!/usr/bin/env python3
"""Run a named LiuXin pytest stream.

The default remains the complete test suite.  The smaller streams provide
quieter, faster feedback while working in an isolated part of the codebase;
they are not replacements for the external full-suite merge gate.
"""

from __future__ import annotations

import argparse
import shlex
import subprocess
import sys

from pathlib import Path
from typing import Sequence


DATABASE_TEST_FILES = (
    "tests/databases/api/test_database_api_signature_parity.py",
    "tests/databases/database_driver_plugins/database_driver_contract/test_contract_surface.py",
    "tests/databases/database_driver_plugins/database_driver_contract/test_contract_schema_introspection.py",
    "tests/databases/database_driver_plugins/database_driver_contract/test_contract_basic_crud_roundtrips.py",
    "tests/databases/database_driver_plugins/database_driver_contract/test_contract_error_handling.py",
    "tests/databases/database/database_contract/test_db_surface_and_lifecycle.py",
    "tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py",
    "tests/databases/database_driver_plugins/SQLite_database_driver/test_sqlite_pure_driver_no_apsw.py",
)

# Smoke-named files already cover file_formats, metadata, scripts, support, and
# utils.  These sentinels fill the active top-level areas that do not have a
# useful smoke-named module.  Keeping this mapping explicit makes gaps visible.
SMOKE_SENTINELS_BY_AREA = {
    "root": "tests/test_constants.py",
    "catalog": "tests/catalog/test_catalog_imports.py",
    "core": "tests/core/test_core_runtime_phase1.py",
    "customize": "tests/customize/test_customize_base.py",
    "ingest": "tests/ingest/test_store_ingest.py",
    "jobs": "tests/jobs/test_jobs_repository.py",
    "library": "tests/library/test_unified_library.py",
    "preferences": "tests/preferences/test_preferences_regression.py",
    "storage": "tests/storage/api/test_placement_hints_api.py",
    "surfaces": "tests/surfaces/test_catalog_api.py",
}

STREAM_DESCRIPTIONS = {
    "full": "all tests (the default and merge-gate scope)",
    "database": "representative database API, driver, backend, and lifecycle contracts",
    "smoke": "non-database smoke tests plus one sentinel for every active test area",
    "confidence": "the database and smoke streams combined",
}


class StreamConfigurationError(RuntimeError):
    """Raised when a configured stream points outside the available test tree."""


def _deduplicate(paths: Sequence[str]) -> list[str]:
    return list(dict.fromkeys(paths))


def discover_smoke_test_files(repo_root: Path) -> list[str]:
    """Discover smoke-named tests outside the database suite."""

    tests_root = repo_root / "tests"
    discovered: list[str] = []
    for path in sorted(tests_root.rglob("*smoke*.py")):
        relative = path.relative_to(repo_root)
        if len(relative.parts) > 1 and relative.parts[1] == "databases":
            continue
        discovered.append(relative.as_posix())
    return discovered


def resolve_stream_files(repo_root: Path, stream: str) -> list[str]:
    """Resolve and validate the pytest paths for ``stream``."""

    if stream not in STREAM_DESCRIPTIONS:
        raise StreamConfigurationError(f"unknown test stream: {stream!r}")

    if stream == "full":
        selected = ["tests"]
    else:
        smoke_files = _deduplicate(
            [
                *discover_smoke_test_files(repo_root),
                *SMOKE_SENTINELS_BY_AREA.values(),
            ]
        )
        if stream == "database":
            selected = list(DATABASE_TEST_FILES)
        elif stream == "smoke":
            selected = smoke_files
        else:
            selected = _deduplicate([*DATABASE_TEST_FILES, *smoke_files])

    invalid: list[str] = []
    for configured_path in selected:
        relative = Path(configured_path)
        if not relative.parts or relative.parts[0] != "tests":
            invalid.append(configured_path)
            continue
        if not (repo_root / relative).exists():
            invalid.append(configured_path)
    if invalid:
        joined = ", ".join(repr(path) for path in invalid)
        raise StreamConfigurationError(f"test stream {stream!r} has invalid paths: {joined}")

    return selected


def build_pytest_command(
    repo_root: Path,
    stream: str,
    extra_pytest_args: Sequence[str] = (),
) -> list[str]:
    """Build the quiet pytest command used by the named stream."""

    selected = resolve_stream_files(repo_root, stream)
    extras = list(extra_pytest_args)
    if extras and extras[0] == "--":
        extras = extras[1:]
    command = [
        sys.executable,
        "-m",
        "pytest",
        "-q",
        "--tb=short",
    ]
    if stream != "full":
        command.append("--disable-warnings")
    command.extend([*selected, *extras])
    return command


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run a named test stream; the default stream is the complete suite."
    )
    parser.add_argument(
        "--stream",
        choices=tuple(STREAM_DESCRIPTIONS),
        default="full",
        help="Test stream to run (default: full)",
    )
    parser.add_argument(
        "--list-streams",
        action="store_true",
        help="List available streams and exit",
    )
    parser.add_argument(
        "--list-files",
        action="store_true",
        help="List the paths selected by --stream and exit",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the pytest command without running it",
    )
    parser.add_argument(
        "pytest_args",
        nargs=argparse.REMAINDER,
        help="Additional pytest arguments; prefix them with --",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    repo_root = Path(__file__).resolve().parents[1]

    if args.list_streams:
        for name, description in STREAM_DESCRIPTIONS.items():
            default = " (default)" if name == "full" else ""
            print(f"{name}{default}: {description}")
        return 0

    try:
        selected = resolve_stream_files(repo_root, args.stream)
        command = build_pytest_command(repo_root, args.stream, args.pytest_args)
    except StreamConfigurationError as exc:
        parser.error(str(exc))

    if args.list_files:
        for path in selected:
            print(path)
        return 0

    target_label = "target" if len(selected) == 1 else "test files"
    print(
        f"Test stream '{args.stream}': {STREAM_DESCRIPTIONS[args.stream]} "
        f"({len(selected)} {target_label})",
        flush=True,
    )
    if args.dry_run:
        print(shlex.join(command))
        return 0

    return subprocess.call(command, cwd=repo_root)


if __name__ == "__main__":
    raise SystemExit(main())
