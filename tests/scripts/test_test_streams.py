from __future__ import annotations

from pathlib import Path

from scripts import run_test_stream


REPO_ROOT = Path(__file__).resolve().parents[2]


def _test_area(path: Path) -> str:
    relative = path.relative_to(REPO_ROOT / "tests")
    return "root" if len(relative.parts) == 1 else relative.parts[0]


def test_full_is_the_default_stream_and_keeps_the_whole_test_root() -> None:
    args = run_test_stream.build_parser().parse_args([])

    assert args.stream == "full"
    assert run_test_stream.resolve_stream_files(REPO_ROOT, "full") == ["tests"]
    assert "--disable-warnings" not in run_test_stream.build_pytest_command(REPO_ROOT, "full")


def test_database_stream_is_curated_and_covers_public_backend_boundaries() -> None:
    selected = run_test_stream.resolve_stream_files(REPO_ROOT, "database")

    assert selected
    assert all(path.startswith("tests/databases/") for path in selected)
    assert "tests/databases/api/test_database_api_signature_parity.py" in selected
    assert (
        "tests/databases/database_driver_plugins/database_driver_contract/"
        "test_contract_schema_introspection.py"
    ) in selected
    assert (
        "tests/databases/database_driver_plugins/PostgreSQL_database_driver/"
        "test_postgresql_backend.py"
    ) in selected
    assert (
        "tests/databases/database_driver_plugins/SQLite_database_driver/"
        "test_sqlite_pure_driver_no_apsw.py"
    ) in selected


def test_smoke_stream_covers_every_active_non_database_test_area() -> None:
    all_non_database_areas = {
        _test_area(path)
        for path in (REPO_ROOT / "tests").rglob("test_*.py")
        if _test_area(path) != "databases"
    }
    selected_areas = {
        _test_area(REPO_ROOT / path)
        for path in run_test_stream.resolve_stream_files(REPO_ROOT, "smoke")
    }

    assert all_non_database_areas <= selected_areas


def test_confidence_stream_is_the_deduplicated_database_smoke_union() -> None:
    database = run_test_stream.resolve_stream_files(REPO_ROOT, "database")
    smoke = run_test_stream.resolve_stream_files(REPO_ROOT, "smoke")
    confidence = run_test_stream.resolve_stream_files(REPO_ROOT, "confidence")

    assert confidence == list(dict.fromkeys([*database, *smoke]))
    assert len(confidence) == len(set(confidence))


def test_all_named_stream_paths_exist() -> None:
    for stream in run_test_stream.STREAM_DESCRIPTIONS:
        for relative_path in run_test_stream.resolve_stream_files(REPO_ROOT, stream):
            assert (REPO_ROOT / relative_path).exists()


def test_pytest_command_is_quiet_and_accepts_extra_arguments() -> None:
    command = run_test_stream.build_pytest_command(
        REPO_ROOT,
        "database",
        ("--", "--collect-only", "--maxfail=1"),
    )

    assert command[:7] == [
        run_test_stream.sys.executable,
        "-m",
        "pytest",
        "-q",
        "--tb=short",
        "--disable-warnings",
        *run_test_stream.DATABASE_TEST_FILES[:1],
    ]
    assert command[-2:] == ["--collect-only", "--maxfail=1"]
