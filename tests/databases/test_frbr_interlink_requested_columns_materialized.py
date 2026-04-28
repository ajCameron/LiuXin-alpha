"""Regression tests for FRBR generator link-table requested columns.

These ensure TOML `requested_columns` (origin/policy/data etc.) are actually
materialised in the generated interlink tables.
"""

from __future__ import annotations

import pathlib
import sqlite3

try:
    import tomllib  # py3.11+
except ImportError:  # pragma: no cover
    import tomli as tomllib  # type: ignore

from LiuXin_alpha.databases.database_driver_plugins.SQL.database_generator_frbr import database_generator as frbr_gen
from LiuXin_alpha.databases.database_driver_plugins.SQL.utility_mixins import ColumnNameMixin


def _frbr_pkg_root() -> pathlib.Path:
    return pathlib.Path(frbr_gen.__file__).resolve().parent


def _load_interlinks() -> list[dict]:
    path = _frbr_pkg_root() / "interlink_table_requests.toml"
    data = tomllib.loads(path.read_text(encoding="utf-8", errors="replace"))
    return list(data.get("interlinks", []))


def _without_main_triggers(monkeypatch) -> None:
    monkeypatch.setattr(frbr_gen, "get_trigger_sql_files", lambda: [], raising=True)


def test_frbr_requested_columns_origin_policy_data_sequence_and_required_are_materialized(monkeypatch) -> None:
    interlinks = _load_interlinks()

    # Pick representative specs that request each column.
    targets: dict[str, dict] = {}
    for needed in ("origin", "policy", "data", "sequence_number", "is_required"):
        for entry in interlinks:
            cols = set(entry.get("requested_columns") or [])
            if needed in cols:
                targets[needed] = entry
                break

    assert set(targets.keys()) == {"origin", "policy", "data", "sequence_number", "is_required"}, (
        "Expected to find at least one interlink requesting each of origin/policy/data/sequence_number/is_required; "
        f"found: {sorted(targets.keys())}"
    )

    _without_main_triggers(monkeypatch)

    conn = sqlite3.connect(":memory:")
    try:
        conn.execute("PRAGMA foreign_keys = ON;")
        frbr_gen.create_new_database(conn)

        for needed, entry in targets.items():
            left = entry["left_table"]
            right = entry["right_table"]
            table_name, col_base = ColumnNameMixin.get_interlink_table_name(left, right)

            cols = {row[1] for row in conn.execute(f"PRAGMA table_info(`{table_name}`);")}
            expected_col = f"{col_base}_{needed}"
            assert expected_col in cols, (
                f"Interlink table {table_name!r} missing requested column {expected_col!r} "
                f"for spec {left!r}↔{right!r}"
            )

    finally:
        conn.close()


def test_frbr_interlink_tables_include_nullable_source(monkeypatch) -> None:
    _without_main_triggers(monkeypatch)

    conn = sqlite3.connect(":memory:")
    try:
        conn.execute("PRAGMA foreign_keys = ON;")
        frbr_gen.create_new_database(conn)

        for entry in _load_interlinks():
            left = entry["left_table"]
            right = entry["right_table"]
            table_name, col_base = ColumnNameMixin.get_interlink_table_name(left, right)

            table_info = {
                row[1]: row
                for row in conn.execute(f"PRAGMA table_info(`{table_name}`);").fetchall()
            }
            expected_col = f"{col_base}_source"
            assert expected_col in table_info, (
                f"Interlink table {table_name!r} missing standard source column {expected_col!r}"
            )
            assert int(table_info[expected_col][3]) == 0, (
                f"Interlink source column {expected_col!r} should be nullable"
            )

    finally:
        conn.close()
