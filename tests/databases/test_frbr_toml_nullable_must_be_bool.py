"""Regression: TOML `nullable` must be a *real* bool.

We intentionally refuse stringly-typed values like "true"/"false" for the `nullable` key,
so schema semantics can't silently flip via Python truthiness.
"""

from __future__ import annotations

import sqlite3

import pytest


@pytest.mark.usefixtures("tmp_path")
def test_interlinks_nullable_rejects_string(monkeypatch, tmp_path):
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
