"""
Provide test test streams utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test test streams through a consuming regression::

        python -m pytest -q tests/scripts/test_test_streams.py
"""
from __future__ import annotations

from pathlib import Path

from scripts import run_test_stream


REPO_ROOT = Path(__file__).resolve().parents[2]


def _test_area(path: Path) -> str:
    """
    Perform the test area operation under explicit file-format and conversion rules.

    Example:
        Exercise  test area through a consuming regression::

            python -m pytest -q tests/scripts/test_test_streams.py


    :param path: Filesystem path read, written, normalized or validated by the
        operation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    relative = path.relative_to(REPO_ROOT / "tests")
    return "root" if len(relative.parts) == 1 else relative.parts[0]


def test_full_is_the_default_stream_and_keeps_the_whole_test_root() -> None:
    """
    Perform the test full is the default stream and keeps the whole test root operation under explicit file-format and conversion rules.

    Example:
        Exercise test full is the default stream and keeps the whole test root through a consuming regression::

            python -m pytest -q tests/scripts/test_test_streams.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    args = run_test_stream.build_parser().parse_args([])

    assert args.stream == "full"
    assert run_test_stream.resolve_stream_files(REPO_ROOT, "full") == ["tests"]
    assert "--disable-warnings" not in run_test_stream.build_pytest_command(REPO_ROOT, "full")


def test_database_stream_is_curated_and_covers_public_backend_boundaries() -> None:
    """
    Perform the test database stream is curated and covers public backend boundaries operation under explicit file-format and conversion rules.

    Example:
        Exercise test database stream is curated and covers public backend boundaries through a consuming regression::

            python -m pytest -q tests/scripts/test_test_streams.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
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
    """
    Perform the test smoke stream covers every active non database test area operation under explicit file-format and conversion rules.

    Example:
        Exercise test smoke stream covers every active non database test area through a consuming regression::

            python -m pytest -q tests/scripts/test_test_streams.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
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
    """
    Perform the test confidence stream is the deduplicated database smoke union operation under explicit file-format and conversion rules.

    Example:
        Exercise test confidence stream is the deduplicated database smoke union through a consuming regression::

            python -m pytest -q tests/scripts/test_test_streams.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    database = run_test_stream.resolve_stream_files(REPO_ROOT, "database")
    smoke = run_test_stream.resolve_stream_files(REPO_ROOT, "smoke")
    confidence = run_test_stream.resolve_stream_files(REPO_ROOT, "confidence")

    assert confidence == list(dict.fromkeys([*database, *smoke]))
    assert len(confidence) == len(set(confidence))


def test_all_named_stream_paths_exist() -> None:
    """
    Perform the test all named stream paths exist operation under explicit file-format and conversion rules.

    Example:
        Exercise test all named stream paths exist through a consuming regression::

            python -m pytest -q tests/scripts/test_test_streams.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    for stream in run_test_stream.STREAM_DESCRIPTIONS:
        for relative_path in run_test_stream.resolve_stream_files(REPO_ROOT, stream):
            assert (REPO_ROOT / relative_path).exists()


def test_pytest_command_is_quiet_and_accepts_extra_arguments() -> None:
    """
    Perform the test pytest command is quiet and accepts extra arguments operation under explicit file-format and conversion rules.

    Example:
        Exercise test pytest command is quiet and accepts extra arguments through a consuming regression::

            python -m pytest -q tests/scripts/test_test_streams.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
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
