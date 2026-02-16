"""Guardrail tests: TOML specs must not reference unknown tables."""

from __future__ import annotations

import pathlib
import shutil
import sqlite3

import pytest

from LiuXin_alpha.databases.database_driver_plugins.SQL.database_generator_frbr import database_generator as frbr_gen


def _copy_frbr_resources(tmp_root: pathlib.Path) -> pathlib.Path:
    """Copy FRBR generator resource folder into a temp directory."""
    src_root = pathlib.Path(frbr_gen.__file__).resolve().parent

    # Copy the folders needed for a full build.
    for folder in ["table_sql", "trigger_sql"]:
        shutil.copytree(src_root / folder, tmp_root / folder)

    # Copy TOML specs needed by the builder.
    for rel in ["interlink_table_requests.toml", "intralink_table_requests.toml", "aggregate_tables.toml"]:
        shutil.copy2(src_root / rel, tmp_root / rel)

    return tmp_root


def test_interlink_unknown_table_fails(monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> None:
    root = _copy_frbr_resources(tmp_path / "frbr_specs")
    toml_path = root / "interlink_table_requests.toml"
    toml_text = toml_path.read_text(encoding="utf-8", errors="replace")
    toml_text += "\n\n[[interlinks]]\nleft_table = 'does_not_exist'\nright_table = 'works'\nlink_type = 'many_to_many'\nrequested_columns = ['priority']\n"
    toml_path.write_text(toml_text, encoding="utf-8")

    monkeypatch.setattr(frbr_gen, "__folder__", str(root), raising=True)

    conn = sqlite3.connect(":memory:")
    try:
        conn.execute("PRAGMA foreign_keys = ON;")
        with pytest.raises(ValueError, match=r"Unknown table referenced in interlinks\[" ):
            frbr_gen.create_new_database(conn)
    finally:
        conn.close()


def test_intralink_unknown_table_fails(monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> None:
    root = _copy_frbr_resources(tmp_path / "frbr_specs2")
    toml_path = root / "intralink_table_requests.toml"
    toml_text = toml_path.read_text(encoding="utf-8", errors="replace")
    toml_text += "\n\n[[intralinks]]\ntable = 'does_not_exist'\n"
    toml_path.write_text(toml_text, encoding="utf-8")

    monkeypatch.setattr(frbr_gen, "__folder__", str(root), raising=True)

    conn = sqlite3.connect(":memory:")
    try:
        conn.execute("PRAGMA foreign_keys = ON;")
        with pytest.raises(ValueError, match=r"Unknown table referenced in intralinks\[" ):
            frbr_gen.create_new_database(conn)
    finally:
        conn.close()
