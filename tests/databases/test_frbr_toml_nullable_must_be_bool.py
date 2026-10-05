"""
Reject string-valued nullable settings in interlink and intralink TOML.

Writes temporary specs and redirects the generator through pytest patches. These
cases use in-memory connections without explicit close calls and exercise parser
entry points rather than a complete build.

Example:
    Run with pytest::

        python -m pytest -q tests/databases/test_frbr_toml_nullable_must_be_bool.py
"""

from __future__ import annotations

import sqlite3

import pytest


@pytest.mark.usefixtures("tmp_path")
def test_interlinks_nullable_rejects_string(monkeypatch, tmp_path):
    """
    Reject nullable set to the string false during interlink input sanity checks.

    Requires TypeError identifying entry zero and the TOML-boolean requirement.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/test_frbr_toml_nullable_must_be_bool.py::test_interlinks_nullable_rejects_string


    :param monkeypatch: Pytest patch fixture; restores replaced generator or lookup
        attributes after the test.
    :param tmp_path: Pytest-provided temporary directory for isolated database or TOML
        files.
    :return: None; failed expectations raise AssertionError.
    """
    from LiuXin_alpha.databases.database_driver_plugins.SQL.database_generator_frbr import database_generator as dg

    # Point the generator at our temporary TOML spec directory.
    monkeypatch.setattr(dg, "__folder__", str(tmp_path))

    (tmp_path / "interlink_table_requests.toml").write_text(
        """
[[interlinks]]
left_table = "works"
right_table = "expressions"
requested_columns = ["priority"]
nullable = "false"
""".lstrip(),
        encoding="utf-8",
    )

    conn = sqlite3.connect(":memory:")
    builder = dg.SQLiteDatabaseGenerator(conn=conn)

    with pytest.raises(TypeError, match=r"interlinks\[0\]\.nullable must be a TOML boolean"):
        builder.sanity_check_interlink_inputs()


@pytest.mark.usefixtures("tmp_path")
def test_intralinks_nullable_rejects_string(monkeypatch, tmp_path):
    """
    Reject nullable set to the string false while reading requested intralinks.

    Creates a minimal works table first so the test reaches nullable validation, then
    checks the entry-zero diagnostic.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/test_frbr_toml_nullable_must_be_bool.py::test_intralinks_nullable_rejects_string


    :param monkeypatch: Pytest patch fixture; restores replaced generator or lookup
        attributes after the test.
    :param tmp_path: Pytest-provided temporary directory for isolated database or TOML
        files.
    :return: None; failed expectations raise AssertionError.
    """
    from LiuXin_alpha.databases.database_driver_plugins.SQL.database_generator_frbr import database_generator as dg

    monkeypatch.setattr(dg, "__folder__", str(tmp_path))

    # Minimal table so `works` is considered a valid intralink target.
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE works (work_id INTEGER PRIMARY KEY);")

    (tmp_path / "intralink_table_requests.toml").write_text(
        """
[[intralinks]]
table = "works"
requested_columns = ["type"]
nullable = "false"
""".lstrip(),
        encoding="utf-8",
    )

    builder = dg.SQLiteDatabaseGenerator(conn=conn)

    with pytest.raises(TypeError, match=r"intralinks\[0\]\.nullable must be a TOML boolean"):
        builder.get_requested_intralink_tables()
