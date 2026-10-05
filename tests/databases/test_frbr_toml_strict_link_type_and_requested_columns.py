"""
Reject unknown cardinalities and metadata columns in copied FRBR TOML resources.

Each test appends one invalid spec to a temporary resource copy, redirects the
generator, suppresses main-table triggers and closes its in-memory connection in
finally.

Example:
    Run with pytest::

        python -m pytest -q tests/databases/test_frbr_toml_strict_link_type_and_requested_columns.py
"""

from __future__ import annotations

import pathlib
import shutil
import sqlite3

import pytest

from LiuXin_alpha.databases.database_driver_plugins.SQL.database_generator_frbr import database_generator as frbr_gen


def _copy_frbr_resources(tmp_root: pathlib.Path) -> pathlib.Path:
    """
    Copy SQL folders and three generator TOML files into a temporary resource tree.

    Uses copytree without merging existing destination folders and copy2 for TOML files.
    Filesystem errors propagate and may leave a partial copy; cleanup belongs to the
    caller.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/test_frbr_toml_strict_link_type_and_requested_columns.py


    :param tmp_root: Destination resource root whose table_sql and trigger_sql children
        must not already exist.
    :return: Original tmp_root Path after copying.
    """
    src_root = pathlib.Path(frbr_gen.__file__).resolve().parent

    for folder in ["table_sql", "trigger_sql"]:
        shutil.copytree(src_root / folder, tmp_root / folder)

    for rel in ["interlink_table_requests.toml", "intralink_table_requests.toml", "aggregate_tables.toml"]:
        shutil.copy2(src_root / rel, tmp_root / rel)

    return tmp_root


def _without_main_triggers(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Temporarily replace main-table trigger discovery with an empty file list.

    Requires the target attribute to exist; pytest owns restoration.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/test_frbr_toml_strict_link_type_and_requested_columns.py


    :param monkeypatch: Pytest patch fixture; restores replaced generator or lookup
        attributes after the test.
    :return: None; patches the generator through the provided monkeypatch fixture.
    """
    monkeypatch.setattr(frbr_gen, "get_trigger_sql_files", lambda: [], raising=True)


def test_interlink_unknown_link_type_fails(monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> None:
    """
    Require TypeError when a copied interlink spec names an unknown link_type.

    Accepts either supported wording of the unknown-cardinality diagnostic.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/test_frbr_toml_strict_link_type_and_requested_columns.py::test_interlink_unknown_link_type_fails


    :param monkeypatch: Pytest patch fixture; restores replaced generator or lookup
        attributes after the test.
    :param tmp_path: Pytest-provided temporary directory for isolated database or TOML
        files.
    :return: None; failed expectations raise AssertionError.
    """
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
    _without_main_triggers(monkeypatch)

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
    """
    Reject an explicit non-exclusive cardinality that requests priority without type.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/test_frbr_toml_strict_link_type_and_requested_columns.py::test_non_exclusive_requires_type_when_requested_columns_provided


    :param monkeypatch: Pytest patch fixture; restores replaced generator or lookup
        attributes after the test.
    :param tmp_path: Pytest-provided temporary directory for isolated database or TOML
        files.
    :return: None; failed expectations raise AssertionError.
    """
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
    _without_main_triggers(monkeypatch)

    conn = sqlite3.connect(":memory:")
    try:
        conn.execute("PRAGMA foreign_keys = ON;")
        with pytest.raises(TypeError, match=r"many_to_many_non_exclusive.*type"):
            frbr_gen.create_new_database(conn)
    finally:
        conn.close()


def test_interlink_unknown_requested_column_fails(monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> None:
    """
    Reject an unsupported interlink metadata column with the requested_columns diagnostic.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/test_frbr_toml_strict_link_type_and_requested_columns.py::test_interlink_unknown_requested_column_fails


    :param monkeypatch: Pytest patch fixture; restores replaced generator or lookup
        attributes after the test.
    :param tmp_path: Pytest-provided temporary directory for isolated database or TOML
        files.
    :return: None; failed expectations raise AssertionError.
    """
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
    _without_main_triggers(monkeypatch)

    conn = sqlite3.connect(":memory:")
    try:
        conn.execute("PRAGMA foreign_keys = ON;")
        with pytest.raises(TypeError, match=r"Unknown requested_columns entry"):
            frbr_gen.create_new_database(conn)
    finally:
        conn.close()


def test_intralink_unknown_requested_column_fails(monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> None:
    """
    Reject an unsupported intralink metadata column with the requested_cols diagnostic.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/test_frbr_toml_strict_link_type_and_requested_columns.py::test_intralink_unknown_requested_column_fails


    :param monkeypatch: Pytest patch fixture; restores replaced generator or lookup
        attributes after the test.
    :param tmp_path: Pytest-provided temporary directory for isolated database or TOML
        files.
    :return: None; failed expectations raise AssertionError.
    """
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
    _without_main_triggers(monkeypatch)

    conn = sqlite3.connect(":memory:")
    try:
        conn.execute("PRAGMA foreign_keys = ON;")
        with pytest.raises(TypeError, match=r"Unknown requested_cols entry"):
            frbr_gen.create_new_database(conn)
    finally:
        conn.close()
