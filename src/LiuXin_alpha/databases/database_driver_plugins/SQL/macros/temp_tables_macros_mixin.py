
"""
Macros to manipulate temporary tables.

This is a legacy calibre option - maintained for support.
"""

from __future__ import annotations

from typing import Any, TYPE_CHECKING, Iterable

from LiuXin_alpha.databases.database_driver_plugins.SQL.macros.portable_macros_mixin import (
    _identifier,
    _quoted,
)

if TYPE_CHECKING:

    from LiuXin_alpha.databases.api.database_api import DatabaseAPI


class TempTablesMacrosMixin:
    """
    Macros mixin for the temp tables.
    """

    db: "DatabaseAPI"

    # ------------------------------------------------------------------------------------------------------------------
    #
    # - TEMP TABLES

    # Todo: Be nice to know what temp tables exist at any given point
    # Todo: Should not be possible to accidentally drop a temp table
    # Todo: Also can forsee a problem where the temp tables can be used to drop main tables if their names clash
    # Todo: conn can be protocoled
    def create_cc_temp_tables(
            self,
            temp_tables: Iterable[str],
            conn: Any = None) -> None:
        """
        Create temp tables for bulk addition.

        :param temp_tables: An itterable of temporary table names to create.
        :param conn: Allows an override connection to be provided.
        :return:
        """
        conn = conn if conn is not None else self.db.driver.conn

        temp_tables = tuple(
            _identifier(table, kind="temporary table name")
            for table in temp_tables
        )
        drops = "\n".join(
            f"DROP TABLE IF EXISTS temp.{_quoted(table)};"
            for table in temp_tables
        )
        creates = "\n".join(
            f"CREATE TEMP TABLE {_quoted(table)}(id INTEGER PRIMARY KEY);"
            for table in temp_tables
        )
        conn.executescript(drops + "\n" + creates)

    def destroy_cc_temp_tables(
            self,
            temp_tables: Iterable[str],
            conn: Any = None) -> None:
        """
        Destroy temp tables which have been used for bulk addition.

        :param temp_tables:
        :param conn:
        :return:
        """
        conn = conn if conn is not None else self.db.driver.conn

        drops = "\n".join(
            f"DROP TABLE IF EXISTS temp.{_quoted(_identifier(table, kind='temporary table name'))};"
            for table in temp_tables
        )
        conn.executescript(drops)

    def insert_values_into_temp_table(
            self,
            temp_table: str,
            values: Iterable[Any],
            conn: Any = None) -> None:
        """
        Insert values into a given temp table.

        :param temp_table:
        :param values:
        :param conn:
        :return:
        """
        conn = conn if conn is not None else self.db.driver.conn

        temp_table = _identifier(temp_table, kind="temporary table name")
        stmt = f"INSERT INTO temp.{_quoted(temp_table)} VALUES (?)"
        conn.executemany(stmt, [(x,) for x in values])

    #
    # ------------------------------------------------------------------------------------------------------------------
