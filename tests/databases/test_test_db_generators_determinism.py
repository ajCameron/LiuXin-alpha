from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from tests.support import test_resources_manager as trm


ALL_TEST_DB_NAMES = tuple(f"test_db_{i}" for i in range(26))


def _is_volatile_column(col_name: str) -> bool:
    low = col_name.lower()
    return (
        "timestamp" in low
        or "datestamp" in low
        or low.endswith("_uuid")
        or low == "uuid"
    )


def _canonical_snapshot(db_path: Path) -> tuple[tuple[str, tuple[str, ...], tuple[tuple[str, ...], ...]], ...]:
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
    cache_dir = tmp_path / f"cache_{db_name}_{run_tag}"
    out_dir = tmp_path / f"out_{db_name}_{run_tag}"
    mgr = trm.TestResourcesManager(cache_dir=cache_dir, regenerate=True)
    provisioned = mgr.provision_named_test_database(name=db_name, dst_dir=out_dir)
    return _canonical_snapshot(provisioned.db_path)


@pytest.mark.db
@pytest.mark.slow
@pytest.mark.parametrize("db_name", ALL_TEST_DB_NAMES)
def test_test_db_generators_are_deterministic(tmp_path: Path, db_name: str) -> None:
    dump_a = _build_and_dump(tmp_path=tmp_path, db_name=db_name, run_tag="a")
    dump_b = _build_and_dump(tmp_path=tmp_path, db_name=db_name, run_tag="b")
    assert dump_a == dump_b
