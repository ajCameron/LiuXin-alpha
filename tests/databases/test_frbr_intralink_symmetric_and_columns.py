"""
Probe intralink ordering, type guards and optional metadata columns.

Tests use isolated file databases and close each connection in finally. The direct
builder test uses a minimal mixin host with identity table-name matching.

Example:
    Run with pytest::

        python -m pytest -q tests/databases/test_frbr_intralink_symmetric_and_columns.py
"""

from __future__ import annotations

import pathlib
import sqlite3

import pytest

from LiuXin_alpha.databases.database_driver_plugins.SQL.database_generator_frbr import database_generator as frbr_gen
from LiuXin_alpha.databases.database_driver_plugins.SQL.utility_mixins import SQLiteTableLinkingMixin


def _insert_two_works(conn: sqlite3.Connection) -> tuple[int, int]:
    """
    Insert two default works and return the two greatest work IDs.

    Queries all work IDs in ascending order and requires at least two rows. Does not
    commit; the returned IDs are selected by ordering rather than lastrowid.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/test_frbr_intralink_symmetric_and_columns.py


    :param conn: Caller-owned SQLite connection; this helper does not close it.
    :return: Two greatest integer work IDs in ascending order.
    """
    conn.execute("INSERT INTO works DEFAULT VALUES;")
    conn.execute("INSERT INTO works DEFAULT VALUES;")
    rows = conn.execute("SELECT work_id FROM works ORDER BY work_id;").fetchall()
    assert len(rows) >= 2
    return int(rows[-2][0]), int(rows[-1][0])


def test_frbr_intralink_symmetric_ordering_enforced(tmp_path: pathlib.Path) -> None:
    """
    Reject reversed and self work links, then commit an ascending symmetric pair.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/test_frbr_intralink_symmetric_and_columns.py::test_frbr_intralink_symmetric_ordering_enforced


    :param tmp_path: Pytest-provided temporary directory for isolated database or TOML
        files.
    :return: None; failed expectations raise AssertionError.
    """
    db_path = tmp_path / "frbr_intralink_symmetric.db"
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("PRAGMA foreign_keys = ON;")
        frbr_gen.create_new_database(conn)

        a, b = _insert_two_works(conn)
        assert a != b

        intralink_table = "work_work_intralinks"

        # Out-of-order (b,a) should fail when symmetric=True
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                f"""
                INSERT INTO `{intralink_table}` (`work_work_intralink_primary_id`, `work_work_intralink_secondary_id`, `work_work_intralink_type`)
                VALUES (?, ?, ?);
                """,
                (b, a, "user_marked_different"),
            )

        # Self-link should fail too
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                f"""
                INSERT INTO `{intralink_table}` (`work_work_intralink_primary_id`, `work_work_intralink_secondary_id`, `work_work_intralink_type`)
                VALUES (?, ?, ?);
                """,
                (a, a, "user_marked_different"),
            )

        # In-order should succeed
        conn.execute(
            f"""
            INSERT INTO `{intralink_table}` (`work_work_intralink_primary_id`, `work_work_intralink_secondary_id`, `work_work_intralink_type`)
            VALUES (?, ?, ?);
            """,
            (min(a, b), max(a, b), "user_marked_different"),
        )

        conn.commit()
    finally:
        conn.close()


def test_frbr_intralink_type_guard_works_with_fk_off(tmp_path: pathlib.Path) -> None:
    """
    Reject an unknown work-intralink type after requesting foreign keys off before generation.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/test_frbr_intralink_symmetric_and_columns.py::test_frbr_intralink_type_guard_works_with_fk_off


    :param tmp_path: Pytest-provided temporary directory for isolated database or TOML
        files.
    :return: None; failed expectations raise AssertionError.
    """
    db_path = tmp_path / "frbr_intralink_type_guard.db"
    conn = sqlite3.connect(str(db_path))
    try:
        # Intentionally leave FK OFF to ensure the trigger does the work
        conn.execute("PRAGMA foreign_keys = OFF;")
        frbr_gen.create_new_database(conn)

        a, b = _insert_two_works(conn)
        intralink_table = "work_work_intralinks"

        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                f"""
                INSERT INTO `{intralink_table}` (`work_work_intralink_primary_id`, `work_work_intralink_secondary_id`, `work_work_intralink_type`)
                VALUES (?, ?, ?);
                """,
                (min(a, b), max(a, b), "this_is_not_a_valid_type"),
            )
    finally:
        conn.close()


class _Dummy(SQLiteTableLinkingMixin):
    """
    Host the shared link SQL builder with a caller-owned connection and identity name matching.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/test_frbr_intralink_symmetric_and_columns.py
    """

    def __init__(self, conn: sqlite3.Connection) -> None:
        """
        Store the supplied connection without opening, committing or closing it.

        Example:
            >>> host = _Dummy(None)
            >>> host.conn is None
            True


        :param conn: Caller-owned SQLite connection; this helper does not close it.
        :return: None; initializes the conn attribute.
        """
        self.conn = conn

    def match_to_table_name(self, name: str) -> str:
        """
        Return the supplied table name unchanged without schema lookup.

        Example:
            >>> _Dummy(None).match_to_table_name('widgets')
            'widgets'


        :param name: Table name passed through unchanged.
        :return: Original name argument.
        """
        return name


def test_intralink_builder_optional_columns_and_symmetric_types(tmp_path: pathlib.Path) -> None:
    """
    Build widget intralinks with metadata extras and type-specific ordering.

    Checks optional columns and nullable source, rejects reversed equivalent links,
    permits reversed derived_from links and rejects an unknown type. The final
    foreign-key OFF request occurs inside a write transaction, so this case alone does
    not prove that pragma was applied.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/test_frbr_intralink_symmetric_and_columns.py::test_intralink_builder_optional_columns_and_symmetric_types


    :param tmp_path: Pytest-provided temporary directory for isolated database or TOML
        files.
    :return: None; failed expectations raise AssertionError.
    """

    conn = sqlite3.connect(str(tmp_path / "intralink_builder_cols.db"))
    try:
        conn.execute("PRAGMA foreign_keys = ON;")
        conn.execute("CREATE TABLE widgets (widget_id INTEGER PRIMARY KEY);")
        conn.execute("INSERT INTO widgets DEFAULT VALUES;")
        conn.execute("INSERT INTO widgets DEFAULT VALUES;")
        w1, w2 = [int(r[0]) for r in conn.execute("SELECT widget_id FROM widgets ORDER BY widget_id;").fetchall()]

        d = _Dummy(conn)

        # Build intralink with extra metadata columns and symmetric ordering only for one type.
        stmts = d.direct_build_intralink_table_sql(
            "widgets",
            allowed_types=["equivalent", "derived_from"],
            requested_cols={"type", "origin", "policy", "data", "priority"},
            nullable_fks=False,
            symmetric=False,
            symmetric_types=["equivalent"],
            use_reference_types_table=True,
        )

        for s in stmts:
            conn.execute(s)
        conn.commit()

        # Add the `{table}__types` reference table + guard triggers (FRBR style)
        d.direct_create_interlink_types_reference_table(
            interlink_table_name="widget_widget_intralinks",
            interlink_column_base="widget_widget_intralink",
            allowed_types=["equivalent", "derived_from"],
            connection=conn,
        )

        cols = [r[1] for r in conn.execute("PRAGMA table_info(`widget_widget_intralinks`);").fetchall()]
        assert "widget_widget_intralink_origin" in cols
        assert "widget_widget_intralink_policy" in cols
        assert "widget_widget_intralink_data" in cols
        assert "widget_widget_intralink_priority" in cols
        assert "widget_widget_intralink_source" in cols
        assert "widget_widget_intralink_type" in cols
        source_info = {
            r[1]: r
            for r in conn.execute("PRAGMA table_info(`widget_widget_intralinks`);").fetchall()
        }
        assert int(source_info["widget_widget_intralink_source"][3]) == 0

        # symmetric_types enforced for 'equivalent'
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """
                INSERT INTO widget_widget_intralinks
                  (widget_widget_intralink_primary_id, widget_widget_intralink_secondary_id, widget_widget_intralink_type)
                VALUES (?, ?, ?);
                """,
                (max(w1, w2), min(w1, w2), "equivalent"),
            )

        # but allowed for directional type 'derived_from' (no ordering enforced here)
        conn.execute(
            """
            INSERT INTO widget_widget_intralinks
              (widget_widget_intralink_primary_id, widget_widget_intralink_secondary_id, widget_widget_intralink_type)
            VALUES (?, ?, ?);
            """,
            (max(w1, w2), min(w1, w2), "derived_from"),
        )

        # invalid type rejected even with FK off
        conn.execute("PRAGMA foreign_keys = OFF;")
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """
                INSERT INTO widget_widget_intralinks
                  (widget_widget_intralink_primary_id, widget_widget_intralink_secondary_id, widget_widget_intralink_type)
                VALUES (?, ?, ?);
                """,
                (min(w1, w2), max(w1, w2), "nope"),
            )
    finally:
        conn.close()
