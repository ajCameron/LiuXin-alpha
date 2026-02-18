"""Guardrail tests: strict TOML semantics for FRBR generator.

- Unknown link_type must hard-fail (no silent default).
- many_to_many_non_exclusive must explicitly request the `type` column unless requested_columns='all'.
- Unknown requested_columns entries must hard-fail (no bespoke/ignored columns).
"""

from __future__ import annotations

import pathlib
import shutil
import sqlite3

import pytest

from LiuXin_alpha.databases.database_driver_plugins.SQL.database_generator_frbr import database_generator as frbr_gen


def _copy_frbr_resources(tmp_root: pathlib.Path) -> pathlib.Path:
    src_root = pathlib.Path(frbr_gen.__file__).resolve().parent

    for folder in ["table_sql", "trigger_sql"]:
        shutil.copytree(src_root / folder, tmp_root / folder)

    for rel in ["interlink_table_requests.toml", "intralink_table_requests.toml", "aggregate_tables.toml"]:
        shutil.copy2(src_root / rel, tmp_root / rel)

    return tmp_root


def test_interlink_unknown_link_type_fails(monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> None:
    root = _copy_frbr_resources(tmp_path / "frbr_specs_bad_link_type")
    toml_path = root / "interlink_table_requests.toml"

    toml_text = toml_path.read_text(encoding="utf-8", errors="replace")
    toml_text += (
        "\n\n[[interlinks]]\n"
        "left_table = 'works'\n"
        "right_table = 'expressions'\n"
        "link_type = 'totally_not_a_real_type'\n"
        "requested_columns = ['priority']\n"
    )
    toml_path.write_text(toml_text, encoding="utf-8")

    monkeypatch.setattr(frbr_gen, "__folder__", str(root), raising=True)

    conn = sqlite3.connect(":memory:")
    try:
        conn.execute("PRAGMA foreign_keys = ON;")
        with pytest.raises(TypeError, match=r"Unknown link_type|Unknown interlink link_type"):
            frbr_gen.create_new_database(conn)
    finally:
        conn.close()


def test_non_exclusive_requires_type_when_requested_columns_provided(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    root = _copy_frbr_resources(tmp_path / "frbr_specs_non_exclusive_missing_type")
    toml_path = root / "interlink_table_requests.toml"

    toml_text = toml_path.read_text(encoding="utf-8", errors="replace")
    toml_text += (
        "\n\n[[interlinks]]\n"
        "left_table = 'agents'\n"
        "right_table = 'works'\n"
        "link_type = 'many_to_many_non_exclusive'\n"
        "requested_columns = ['priority']\n"
    )
    toml_path.write_text(toml_text, encoding="utf-8")

    monkeypatch.setattr(frbr_gen, "__folder__", str(root), raising=True)

    conn = sqlite3.connect(":memory:")
    try:
        conn.execute("PRAGMA foreign_keys = ON;")
        with pytest.raises(TypeError, match=r"many_to_many_non_exclusive.*type"):
            frbr_gen.create_new_database(conn)
    finally:
        conn.close()


def test_interlink_unknown_requested_column_fails(monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> None:
    root = _copy_frbr_resources(tmp_path / "frbr_specs_bad_requested_col")
    toml_path = root / "interlink_table_requests.toml"

    toml_text = toml_path.read_text(encoding="utf-8", errors="replace")
    toml_text += (
        "\n\n[[interlinks]]\n"
        "left_table = 'works'\n"
        "right_table = 'expressions'\n"
        "link_type = 'many_to_many'\n"
        "requested_columns = ['priority', 'definitely_not_supported']\n"
    )
    toml_path.write_text(toml_text, encoding="utf-8")

    monkeypatch.setattr(frbr_gen, "__folder__", str(root), raising=True)

    conn = sqlite3.connect(":memory:")
    try:
        conn.execute("PRAGMA foreign_keys = ON;")
        with pytest.raises(TypeError, match=r"Unknown requested_columns entry"):
            frbr_gen.create_new_database(conn)
    finally:
        conn.close()


def test_intralink_unknown_requested_column_fails(monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> None:
    root = _copy_frbr_resources(tmp_path / "frbr_specs_bad_intralink_requested_col")
    toml_path = root / "intralink_table_requests.toml"

    toml_text = toml_path.read_text(encoding="utf-8", errors="replace")
    toml_text += (
        "\n\n[[intralinks]]\n"
        "table = 'works'\n"
        "requested_columns = ['type', 'not_a_real_column']\n"
    )
    toml_path.write_text(toml_text, encoding="utf-8")

    monkeypatch.setattr(frbr_gen, "__folder__", str(root), raising=True)

    conn = sqlite3.connect(":memory:")
    try:
        conn.execute("PRAGMA foreign_keys = ON;")
        with pytest.raises(TypeError, match=r"Unknown requested_cols entry"):
            frbr_gen.create_new_database(conn)
    finally:
        conn.close()
