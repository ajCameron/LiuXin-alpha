"""
Maintain the legacy custom-column bulk-edit temporary ID tables.

These helpers use SQLite-style temp-schema SQL to create, populate and explicitly
destroy named one-column tables on a caller-selected connection. They validate simple
identifiers and bind inserted values, but do not own a transaction, close the connection
or provide automatic cleanup after a caller failure. Script execution retains the
connection implementation's commit behavior.
"""

from __future__ import annotations

from LiuXin_alpha.databases.database_driver_plugins._macro_state import MacroHostState
from typing import Any, Iterable
from LiuXin_alpha.databases.db_types.driver_types import SQLConnection
from LiuXin_alpha.errors import DatabaseIntegrityError

from LiuXin_alpha.databases.database_driver_plugins.SQL.macros.portable_macros_mixin import (
    _identifier,
    _quoted,
)


class TempTablesMacrosMixin(MacroHostState):
    """
    Supply the legacy temporary-table lifecycle used by custom-column bulk edits.

    A host normally provides db.driver.conn; passing conn explicitly lets each method
    operate without that host attribute. Creation replaces existing temporary tables
    with id INTEGER PRIMARY KEY tables, insertion appends bound values, and destruction
    drops only temp-schema tables. Callers must use the same connection throughout and
    arrange cleanup and transaction handling themselves.

    Example:
        >>> import sqlite3
        >>> from types import SimpleNamespace
        >>> conn = sqlite3.connect(":memory:")
        >>> macros = TempTablesMacrosMixin()
        >>> macros.db = SimpleNamespace(driver=SimpleNamespace(conn=conn))
        >>> macros.create_cc_temp_tables(("selected_ids",))
        >>> macros.insert_values_into_temp_table("selected_ids", (7, 9))
        >>> conn.execute('SELECT id FROM temp."selected_ids" ORDER BY id').fetchall()
        [(7,), (9,)]
        >>> macros.destroy_cc_temp_tables(("selected_ids",))
        >>> conn.close()
    """

    # ------------------------------------------------------------------------------------------------------------------
    #
    # - TEMP TABLES

    # Todo: Be nice to know what temp tables exist at any given point
    # Todo: Should not be possible to accidentally drop a temp table
    # Todo: Also can forsee a problem where the temp tables can be used to drop main tables if their names clash
    # Todo: conn can be protocoled
    def create_cc_temp_tables(
        self, temp_tables: Iterable[str], conn: SQLConnection | None = None
    ) -> None:
        """
        Replace named temporary tables with empty id INTEGER PRIMARY KEY tables.

        Resolve the connection, then materialize and validate every simple identifier
        before executing SQL. Strip surrounding name whitespace. Execute individual
        statements, preserving any caller-owned transaction. Drop all
        requested temp-schema tables before creating any replacements; same-name main
        tables are preserved. Existing temporary data is discarded. Duplicate names are
        not deduplicated, so a repeated CREATE can fail after earlier statements ran.

        Empty input performs no SQL. No BEGIN, commit or rollback is added here.
        Validation/iteration failures occur before SQL; later SQL failures remain
        inside the caller's transaction.

        Example:
            >>> import sqlite3
            >>> conn = sqlite3.connect(":memory:")
            >>> macros = TempTablesMacrosMixin()
            >>> macros.create_cc_temp_tables(("selected_ids",), conn=conn)
            >>> macros.insert_values_into_temp_table("selected_ids", (7,), conn=conn)
            >>> macros.create_cc_temp_tables(("selected_ids",), conn=conn)
            >>> conn.execute('SELECT COUNT(*) FROM temp."selected_ids"').fetchone()[0]
            0
            >>> conn.close()


        :param temp_tables: Iterable of simple table names, fully consumed and validated
            before the drop/create script runs.
        :param conn: Connection supporting execute, or None to use db.driver.conn;
            an explicit non-None connection is retained even if falsey.
        :return: None; statement cursors close and the connection remains open.
        """
        conn = conn if conn is not None else self._macro_database.driver.conn
        if conn is None:
            raise DatabaseIntegrityError(
                "Temporary-table macros have no active connection"
            )

        temp_tables = tuple(
            _identifier(table, kind="temporary table name") for table in temp_tables
        )
        for table in temp_tables:
            conn.execute(f"DROP TABLE IF EXISTS temp.{_quoted(table)}").close()
        for table in temp_tables:
            conn.execute(
                f"CREATE TEMP TABLE {_quoted(table)}(id INTEGER PRIMARY KEY)"
            ).close()

    def destroy_cc_temp_tables(
        self, temp_tables: Iterable[str], conn: SQLConnection | None = None
    ) -> None:
        """
        Drop named temp-schema tables while preserving same-name main-schema tables.

        Build the complete script before executing it, validating and trimming each
        simple name. IF EXISTS tolerates absent tables and repeated names. Invalid
        identifiers or iterable failures prevent the entire script from being sent;
        SQL failures may occur after earlier drops. Empty input performs no SQL.
        Execute individual statements without committing or closing the connection;
        the caller owns rollback on SQL failure.

        Example:
            >>> import sqlite3
            >>> conn = sqlite3.connect(":memory:")
            >>> _ = conn.execute("CREATE TABLE selected_ids (id INTEGER PRIMARY KEY)")
            >>> macros = TempTablesMacrosMixin()
            >>> macros.create_cc_temp_tables(("selected_ids",), conn=conn)
            >>> macros.destroy_cc_temp_tables(
            ...     ("selected_ids", "selected_ids"), conn=conn
            ... )
            >>> conn.execute(
            ...     "SELECT COUNT(*) FROM sqlite_temp_master WHERE name='selected_ids'"
            ... ).fetchone()[0]
            0
            >>> conn.execute(
            ...     "SELECT COUNT(*) FROM sqlite_master WHERE name='selected_ids'"
            ... ).fetchone()[0]
            1
            >>> conn.close()


        :param temp_tables: Iterable of simple temporary-table names; all names are
            processed before executing the drop script.
        :param conn: Connection supporting execute, or None to use db.driver.conn.
        :return: None; missing tables are ignored by SQL IF EXISTS.
        """
        conn = conn if conn is not None else self._macro_database.driver.conn
        if conn is None:
            raise DatabaseIntegrityError(
                "Temporary-table macros have no active connection"
            )

        tables = tuple(
            _identifier(table, kind="temporary table name") for table in temp_tables
        )
        for table in tables:
            conn.execute(f"DROP TABLE IF EXISTS temp.{_quoted(table)}").close()

    def insert_values_into_temp_table(
        self, temp_table: str, values: Iterable[Any], conn: SQLConnection | None = None
    ) -> None:
        """
        Append scalar values to an existing one-column temporary table with executemany.

        Validate and trim the table name, then materialize all values as one-item
        bindings before executing SQL. The table is addressed through temp and must
        already exist on the chosen connection. No Python integer conversion or
        deduplication occurs; the backend applies the table's constraints. For SQLite
        tables created by create_cc_temp_tables, None requests an automatically assigned
        INTEGER PRIMARY KEY and repeated IDs raise an integrity error.

        A failing input iterable prevents executemany from starting. A binding or
        constraint error during executemany can leave earlier rows inserted within the
        connection's transaction. This helper neither clears the table nor explicitly
        commits, rolls back or closes the connection.

        Example:
            >>> import sqlite3
            >>> conn = sqlite3.connect(":memory:")
            >>> macros = TempTablesMacrosMixin()
            >>> macros.create_cc_temp_tables(("selected_ids",), conn=conn)
            >>> macros.insert_values_into_temp_table(
            ...     "selected_ids", (1, 2, None), conn=conn
            ... )
            >>> conn.execute(
            ...     'SELECT id FROM temp."selected_ids" ORDER BY id'
            ... ).fetchall()
            [(1,), (2,), (3,)]
            >>> try:
            ...     macros.insert_values_into_temp_table(
            ...         "selected_ids", (4, 2), conn=conn
            ...     )
            ... except sqlite3.IntegrityError:
            ...     print("duplicate")
            duplicate
            >>> conn.execute(
            ...     'SELECT id FROM temp."selected_ids" ORDER BY id'
            ... ).fetchall()
            [(1,), (2,), (3,), (4,)]
            >>> conn.rollback()
            >>> conn.close()


        :param temp_table: Simple name of an existing one-column table in the temp
            schema.
        :param values: Iterable of scalar binding values, fully materialized into a
            list.
        :param conn: Connection supporting executemany, or None to use db.driver.conn.
        :return: None; the execution result is discarded.
        """
        conn = conn if conn is not None else self._macro_database.driver.conn
        if conn is None:
            raise DatabaseIntegrityError(
                "Temporary-table macros have no active connection"
            )

        temp_table = _identifier(temp_table, kind="temporary table name")
        stmt = f"INSERT INTO temp.{_quoted(temp_table)} VALUES (?)"
        conn.executemany(stmt, [(x,) for x in values])

    #
    # ------------------------------------------------------------------------------------------------------------------
