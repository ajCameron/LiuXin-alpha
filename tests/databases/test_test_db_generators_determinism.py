"""
Compare canonical contents from two independent regenerations of each legacy test database.

The slow parametrized checks omit volatile columns and sort repr-encoded rows; they
do not compare database bytes, schema SQL, or generated asset files.

Example:
    Run with pytest::

        python -m pytest -q tests/databases/test_test_db_generators_determinism.py
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from tests.support import test_resources_manager as trm


ALL_TEST_DB_NAMES = tuple(f"test_db_{i}" for i in range(26))


def _is_volatile_column(col_name: str) -> bool:
    """
    Recognize timestamp/datestamp columns and UUID column names without case sensitivity.

    Example:
        >>> [_is_volatile_column(n) for n in ('modified_timestamp', 'store_UUID', 'title')]
        [True, True, False]


    :param col_name: Column name to lowercase and classify.
    :return: True for names containing timestamp or datestamp, equal to uuid, or ending
        in _uuid.
    """
    low = col_name.lower()
    return (
        "timestamp" in low
        or "datestamp" in low
        or low.endswith("_uuid")
        or low == "uuid"
    )


def _canonical_snapshot(db_path: Path) -> tuple[tuple[str, tuple[str, ...], tuple[tuple[str, ...], ...]], ...]:
    """
    Read sorted repr-encoded rows from noninternal SQLite tables, excluding volatile columns.

    Visit tables alphabetically and retain stable columns in declaration order. Tables
    with no stable columns contribute empty column and row tuples. Always close the
    opened connection; SQL and filesystem failures propagate.

    Example:
        >>> _canonical_snapshot(':memory:')
        ()


    :param db_path: Path of the SQLite database to open; the helper closes its
        connection.
    :return: Tuple of (table_name, stable_column_names, sorted_row_tuples) entries.
    """
    conn = sqlite3.connect(str(db_path))
    try:
        tables = [
            str(r[0])
            for r in conn.execute(
                "SELECT name FROM sqlite_master "
                "WHERE type='table' AND name NOT LIKE 'sqlite_%' "
                "ORDER BY name;"
            ).fetchall()
        ]

        snapshot: list[tuple[str, tuple[str, ...], tuple[tuple[str, ...], ...]]] = []
        for table in tables:
            cols = [
                str(r[1])
                for r in conn.execute(f"PRAGMA table_info(`{table}`);").fetchall()
            ]
            stable_cols = [c for c in cols if not _is_volatile_column(c)]
            if not stable_cols:
                snapshot.append((table, tuple(), tuple()))
                continue

            cols_sql = ", ".join([f"`{c}`" for c in stable_cols])
            rows = conn.execute(f"SELECT {cols_sql} FROM `{table}`;").fetchall()
            row_strings = [tuple(repr(v) for v in row) for row in rows]
            row_strings.sort()
            snapshot.append((table, tuple(stable_cols), tuple(row_strings)))

        return tuple(snapshot)
    finally:
        conn.close()


def _build_and_dump(
    *,
    tmp_path: Path,
    db_name: str,
    run_tag: str,
) -> tuple[tuple[str, tuple[str, ...], tuple[tuple[str, ...], ...]], ...]:
    """
    Regenerate a named fixture in run-specific cache and output directories, then snapshot it.

    Files remain beneath the caller-provided temporary directory for pytest cleanup.
    Generator and snapshot errors propagate.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/test_test_db_generators_determinism.py


    :param tmp_path: Pytest-provided temporary directory for isolated database or TOML
        files.
    :param db_name: Legacy fixture name selected by pytest from test_db_0 through
        test_db_25.
    :param run_tag: Suffix distinguishing independent cache and output directories for
        this run.
    :return: The canonical snapshot of the provisioned database.
    """
    cache_dir = tmp_path / f"cache_{db_name}_{run_tag}"
    out_dir = tmp_path / f"out_{db_name}_{run_tag}"
    mgr = trm.TestResourcesManager(cache_dir=cache_dir, regenerate=True)
    provisioned = mgr.provision_named_test_database(name=db_name, dst_dir=out_dir)
    return _canonical_snapshot(provisioned.db_path)


@pytest.mark.catalog
@pytest.mark.slow
@pytest.mark.parametrize("db_name", ALL_TEST_DB_NAMES)
def test_test_db_generators_are_deterministic(tmp_path: Path, db_name: str) -> None:
    """
    Check that independently regenerated a/b copies have equal stable-column snapshots.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/test_test_db_generators_determinism.py::test_test_db_generators_are_deterministic


    :param tmp_path: Pytest-provided temporary directory for isolated database or TOML
        files.
    :param db_name: Legacy fixture name selected by pytest from test_db_0 through
        test_db_25.
    :return: None; failed expectations raise AssertionError.
    """
    dump_a = _build_and_dump(tmp_path=tmp_path, db_name=db_name, run_tag="a")
    dump_b = _build_and_dump(tmp_path=tmp_path, db_name=db_name, run_tag="b")
    assert dump_a == dump_b
