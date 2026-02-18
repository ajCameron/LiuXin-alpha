"""Guardrail tests: FRBR generator resources must exist as directories.

These used to be enforced with `assert ... is_dir()`. Those asserts can be stripped with
Python's -O / PYTHONOPTIMIZE, so we require real exceptions instead.
"""

from __future__ import annotations

import pathlib

import pytest

from LiuXin_alpha.databases.database_driver_plugins.SQL.database_generator_frbr import database_generator as frbr_gen


def test_missing_table_sql_folder_raises(monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> None:
    empty_root = tmp_path / "empty_frbr_specs"
    empty_root.mkdir()

    monkeypatch.setattr(frbr_gen, "__folder__", str(empty_root), raising=True)

    with pytest.raises(NotADirectoryError, match=r"table_sql"):
        frbr_gen.get_main_tables_sql_files()


def test_missing_trigger_sql_folder_raises(monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> None:
    root = tmp_path / "missing_trigger"
    root.mkdir()
    # create table_sql only
    (root / "table_sql").mkdir()

    monkeypatch.setattr(frbr_gen, "__folder__", str(root), raising=True)

    with pytest.raises(NotADirectoryError, match=r"trigger_sql"):
        frbr_gen.get_trigger_sql_files()
