"""
Require explicit directory errors when FRBR SQL resource folders are missing.

Each case redirects resource discovery to temporary folders using pytest patch
restoration. These cases check exception types/messages without starting an
optimized interpreter.

Example:
    Run with pytest::

        python -m pytest -q tests/databases/test_frbr_resource_folders_must_exist.py
"""

from __future__ import annotations

import pathlib

import pytest

from LiuXin_alpha.databases.database_driver_plugins.SQL.database_generator_frbr import (
    database_generator as frbr_gen,
)


def test_missing_table_sql_folder_raises(monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> None:
    """
    Require NotADirectoryError mentioning table_sql when its resource directory is absent.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/test_frbr_resource_folders_must_exist.py::test_missing_table_sql_folder_raises


    :param monkeypatch: Pytest patch fixture; restores replaced generator or lookup
        attributes after the test.
    :param tmp_path: Pytest-provided temporary directory for isolated database or TOML
        files.
    :return: None; failed expectations raise AssertionError.
    """
    empty_root = tmp_path / "empty_frbr_specs"
    empty_root.mkdir()

    monkeypatch.setattr(frbr_gen, "__folder__", str(empty_root), raising=True)

    with pytest.raises(NotADirectoryError, match=r"table_sql"):
        frbr_gen.get_main_table_sql_files()


def test_missing_trigger_sql_folder_raises(monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> None:
    """
    Require NotADirectoryError mentioning trigger_sql when only table_sql exists.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/test_frbr_resource_folders_must_exist.py::test_missing_trigger_sql_folder_raises


    :param monkeypatch: Pytest patch fixture; restores replaced generator or lookup
        attributes after the test.
    :param tmp_path: Pytest-provided temporary directory for isolated database or TOML
        files.
    :return: None; failed expectations raise AssertionError.
    """
    root = tmp_path / "missing_trigger"
    root.mkdir()
    # create table_sql only
    (root / "table_sql").mkdir()

    monkeypatch.setattr(frbr_gen, "__folder__", str(root), raising=True)

    with pytest.raises(NotADirectoryError, match=r"trigger_sql"):
        frbr_gen.get_trigger_sql_files()
