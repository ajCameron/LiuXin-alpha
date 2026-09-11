"""
Provide a small SQLite/Row fixture for storage configuration tests.

The wrapper borrows connections, infers tables from column names, and commits
mutations immediately. It deliberately stubs relation discovery and does not
implement the complete production Database or durable storage-manager boundary.
SQL identifiers are trusted fixture/schema values; row values use parameters.
"""

from __future__ import annotations

import sqlite3

from pathlib import Path
from typing import Any

from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.databases.database_driver_plugins.SQL.database_generator_frbr import database_generator as frbr_gen


class MiniDriverWrapper:
    """
    Adapt a borrowed SQLite connection to the small driver surface needed by Row tests. Table
    selection uses column-name heuristics rather than domain metadata. Schema reads are live;
    relation discovery is deliberately stubbed. Mutations commit the connection immediately,
    including unrelated pending work, and add no rollback handling. Table and column identifiers
    must come from trusted test/schema input; only SQL values are parameterized.

    Example:
        >>> connection = sqlite3.connect(":memory:")
        >>> driver = MiniDriverWrapper(connection)
        >>> driver.get_allowed_tables_snapshot()
        set()
        >>> connection.close()
    """
    def __init__(self, conn: sqlite3.Connection) -> None:
        """
        Retain the supplied SQLite connection without configuring it, copying state, or taking
        responsibility for closing it.

        Example:
            >>> connection = sqlite3.connect(":memory:")
            >>> driver = MiniDriverWrapper(connection)
            >>> driver.conn is connection
            True
            >>> connection.close()


        :param conn: Borrowed open SQLite connection used for every schema and row operation.
        :return: None after retaining the connection.
        """
        self.conn = conn

    def get_allowed_tables_snapshot(self):
        """
        Read the current user-table names and return a fresh set. The wrapper neither caches this
        snapshot nor updates an earlier returned set when the schema changes.

        Example:
            >>> tables = driver.get_allowed_tables_snapshot()  # doctest: +SKIP


        :return: A new set of table-name strings from _tables; SQLite failures propagate.
        """
        return set(self._tables())

    def _tables(self) -> list[str]:
        """
        Query sqlite_master for tables, excluding names matching the sqlite_% pattern, and order by
        name. Views are omitted; each call observes the connection's current schema.

        Example:
            >>> tables = driver._tables()  # doctest: +SKIP


        :return: An alphabetically ordered list of table-name strings; database errors propagate.
        """
        rows = self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name;"
        ).fetchall()
        return [str(r[0]) for r in rows]

    def _columns(self, table: str) -> list[str]:
        """
        Read PRAGMA table_info for a trusted table identifier and project column names in schema
        order. An absent table normally yields an empty list; the identifier is interpolated without
        escaping embedded backticks.

        Example:
            >>> columns = driver._columns("stores")  # doctest: +SKIP


        :param table: Trusted table name interpolated into the schema query.
        :return: A list of column-name strings, possibly empty; SQLite errors propagate.
        """
        return [str(row[1]) for row in self.conn.execute(f"PRAGMA table_info(`{table}`);").fetchall()]

    def identify_table_from_row_dict(self, row_dict: dict[str, Any]):
        """
        Infer a table whose columns contain every supplied dictionary key. Prefer the candidate with
        the fewest columns, keeping alphabetical table order for ties. An empty dictionary can match
        any table. Values, primary keys, and foreign-key relations do not participate in selection,
        so an ambiguous partial row can select a different table from the caller's intent.

        Example:
            >>> table = driver.identify_table_from_row_dict({"store_name": "books"})  # doctest: +SKIP


        :param row_dict: Dictionary whose column-name keys are matched; its values are ignored.
        :return: Selected table name; no containing table raises KeyError and schema errors propagate.
        """
        keys = set(row_dict.keys())
        matches = [table for table in self._tables() if keys.issubset(set(self._columns(table)))]
        if not matches:
            raise KeyError(f"Could not identify table from row_dict keys: {sorted(keys)}")
        if len(matches) == 1:
            return matches[0]
        # Prefer the table with the smallest column superset.
        matches.sort(key=lambda table: len(self._columns(table)))
        return matches[0]

    def identify_table_from_column(self, column: str, error: bool = True):
        """
        Find a table containing the column, using a test-only ambiguity heuristic. Prefer names
        ending in s, then fewer columns, then initial alphabetical order. This spelling rule does
        not actually classify link/helper tables.

        Example:
            >>> table = driver.identify_table_from_column("store_name")  # doctest: +SKIP


        :param column: Exact column name looked up in each current table.
        :param error: Whether a missing match raises KeyError; false returns None instead.
        :return: Selected table-name string, or None for an allowed missing match; schema errors propagate.
        """
        matches = [table for table in self._tables() if column in self._columns(table)]
        if not matches:
            if error:
                raise KeyError(column)
            return None
        if len(matches) == 1:
            return matches[0]
        # Prefer non-link/non-helper tables only as a convenience.
        matches.sort(key=lambda table: (table.endswith('s') is False, len(self._columns(table))))
        return matches[0]

    def get_id_column(self, table: str) -> str:
        """
        Find the column whose SQLite primary-key ordinal is one. For a composite primary key this
        selects only its first component; this adapter does not implement composite row identities.

        Example:
            >>> identity_column = driver.get_id_column("stores")  # doctest: +SKIP


        :param table: Trusted table name inspected through PRAGMA table_info.
        :return: First primary-key column name; no ordinal-one column raises KeyError.
        """
        for row in self.conn.execute(f"PRAGMA table_info(`{table}`);").fetchall():
            if int(row[5]) == 1:
                return str(row[1])
        raise KeyError(f"No primary key id column found for {table!r}")

    def add_row(self, row_dict: dict[str, Any]):
        """
        Infer the table, insert dictionary columns in their iteration order, and commit. Values are
        bound parameters; identifiers are trusted interpolated text. The commit applies to the whole
        connection, not just this insert. Empty columns receive no DEFAULT VALUES special case, and
        failures add no rollback.

        Example:
            >>> row_id = driver.add_row({"store_name": "books"})  # doctest: +SKIP


        :param row_dict: Column/value dictionary used both to infer the table and to build the insert.
        :return: Integer SQLite lastrowid after commit; table inference, SQL, commit, or conversion errors propagate.
        """
        table = self.identify_table_from_row_dict(row_dict)
        columns = list(row_dict.keys())
        placeholders = ", ".join(["?"] * len(columns))
        quoted_cols = ", ".join(f"`{col}`" for col in columns)
        sql = f"INSERT INTO `{table}` ({quoted_cols}) VALUES ({placeholders})"
        cur = self.conn.execute(sql, [row_dict[col] for col in columns])
        self.conn.commit()
        return int(cur.lastrowid)

    def get_row_from_id(self, table: str, row_id: int):
        """
        Select the first row matching the int-converted identity and pair all values with
        schema-order column names. This uses only the ordinal-one primary-key column and adds no
        composite-key matching.

        Example:
            >>> row = driver.get_row_from_id("stores", 1)  # doctest: +SKIP


        :param table: Trusted table name whose primary key and columns are inspected.
        :param row_id: Int-convertible value matched against the selected primary-key column.
        :return: A new complete row dictionary or None when no row matches; lookup, SQL, and strict zip errors propagate.
        """
        id_col = self.get_id_column(table)
        cur = self.conn.execute(f"SELECT * FROM `{table}` WHERE `{id_col}` = ?", (int(row_id),))
        row = cur.fetchone()
        if row is None:
            return None
        cols = self._columns(table)
        return dict(zip(cols, row, strict=True))

    def update_row(self, row_dict: dict[str, Any]):
        """
        Infer the table from all keys and update the non-ID fields where the selected primary key
        equals the supplied ID. Commit the whole connection even if no row matched. ID-only input
        produces no assignments and is not specially handled; failures add no rollback.

        Example:
            >>> driver.update_row({"store_id": 1, "store_name": "renamed"})  # doctest: +SKIP


        :param row_dict: Column/value dictionary containing the inferred table's primary key and fields to update.
        :return: None after executing and committing; no affected-row count or existence check is returned.
        """
        table = self.identify_table_from_row_dict(row_dict)
        id_col = self.get_id_column(table)
        row_id = row_dict[id_col]
        columns = [col for col in row_dict.keys() if col != id_col]
        assignments = ", ".join(f"`{col}` = ?" for col in columns)
        sql = f"UPDATE `{table}` SET {assignments} WHERE `{id_col}` = ?"
        params = [row_dict[col] for col in columns] + [row_id]
        self.conn.execute(sql, params)
        self.conn.commit()

    def get_blank_row(self, table: str):
        """
        Create a fresh dictionary mapping every schema column to None. SQL defaults are not
        evaluated and no row is inserted; an absent table can yield an empty dictionary.

        Example:
            >>> row = driver.get_blank_row("stores")  # doctest: +SKIP


        :param table: Trusted table name whose current columns define the dictionary.
        :return: New column-to-None dictionary; schema errors propagate.
        """
        return {col: None for col in self._columns(table)}

    def ensure_row_has_id(self, row_dict: dict[str, Any]):
        """
        Insert an ID-less dictionary or return an already identified dictionary unchanged. Infer the
        table and its ID column first. If that field is absent or None, remove it before calling
        add_row, then return a copy with the generated ID. The reduced dictionary is independently
        re-inferred by add_row and can pick another table when ambiguous. A non-None ID, including
        zero, returns the original dictionary without verifying that any database row exists.

        Example:
            >>> identified = driver.ensure_row_has_id({"store_name": "books"})  # doctest: +SKIP


        :param row_dict: Row dictionary inspected for its inferred primary-key field; input is not mutated here.
        :return: The original dictionary when already identified, otherwise a new dictionary after insertion/commit; delegated errors propagate.
        """
        table = self.identify_table_from_row_dict(row_dict)
        id_col = self.get_id_column(table)
        if row_dict.get(id_col) is None:
            new_id = self.add_row({k: v for k, v in row_dict.items() if k != id_col})
            row_dict = dict(row_dict)
            row_dict[id_col] = new_id
        return row_dict

    def get_interlinked_tables(self, table: str):
        """
        Return the empty relation-discovery stub used by these Row tests. The table argument is
        ignored, so this says nothing about actual schema links.

        Example:
            >>> tables = driver.get_interlinked_tables("stores")  # doctest: +SKIP


        :param table: Ignored table name retained for driver-wrapper compatibility.
        :return: A fresh empty list.
        """
        return []

    def check_for_intralink_table(self, table: str):
        """
        Return the fixed false intralink stub without querying the connection or validating the
        table. Actual self-link tables, if present, are not discovered.

        Example:
            >>> has_self_links = driver.check_for_intralink_table("stores")  # doctest: +SKIP


        :param table: Ignored table name retained for driver-wrapper compatibility.
        :return: False for every input.
        """
        return False


class MiniDB:
    """
    Expose a minimal Row-compatible database over a borrowed SQLite connection. Reads wrap
    dictionaries in the real Row class; driver_wrapper supplies schema and write helpers. This
    fixture has no full Database lifecycle, portable macros, cache integration, or durable-manager
    protocol. Its UUID starts as None, and the caller owns connection cleanup.

    Example:
        >>> connection = sqlite3.connect(":memory:")
        >>> database = MiniDB(connection)
        >>> database.get_tables()
        []
        >>> connection.close()
    """
    def __init__(self, conn: sqlite3.Connection) -> None:
        """
        Retain the connection, create its MiniDriverWrapper, and set the compatibility UUID to None.
        No schema, foreign-key policy, or close hook is installed.

        Example:
            >>> connection = sqlite3.connect(":memory:")
            >>> database = MiniDB(connection)
            >>> database.driver_wrapper.conn is connection
            True
            >>> connection.close()


        :param conn: Borrowed SQLite connection shared with the new driver wrapper.
        :return: None after initializing the three fixture attributes.
        """
        self.conn = conn
        self.driver_wrapper = MiniDriverWrapper(conn)
        self.uuid = None

    def get_tables(self, force_refresh: bool = False):
        """
        Return live table names through the wrapper. force_refresh is accepted for call
        compatibility and ignored because there is no schema cache.

        Example:
            >>> tables = database.get_tables(force_refresh=True)  # doctest: +SKIP


        :param force_refresh: Ignored cache-refresh flag.
        :return: The wrapper's alphabetically ordered table-name list.
        """
        return self.driver_wrapper._tables()

    def get_column_headings(self, table: str):
        """
        Delegate schema-column enumeration to the driver wrapper without caching or copying its
        returned list.

        Example:
            >>> columns = database.get_column_headings("stores")  # doctest: +SKIP


        :param table: Trusted table name forwarded to the PRAGMA-based column lookup.
        :return: Schema-order column-name list; delegated errors propagate.
        """
        return self.driver_wrapper._columns(table)

    def get_row_from_id(self, table: str, row_id: int):
        """
        Fetch a row dictionary through the wrapper and bind it to a real Row object. A missing row
        returns None without constructing Row; initialization follows Row's normal schema/identity
        behavior.

        Example:
            >>> row = database.get_row_from_id("stores", 1)  # doctest: +SKIP


        :param table: Trusted table name selecting the row.
        :param row_id: Identity forwarded for int conversion and primary-key lookup.
        :return: A Row bound to this MiniDB or None; lookup/construction errors propagate.
        """
        row_dict = self.driver_wrapper.get_row_from_id(table, row_id)
        if row_dict is None:
            return None
        return Row(database=self, row_dict=row_dict)

    def search(self, table: str, column: str, search_term: Any):
        """
        Select all rows whose column equals the bound search value and wrap each in Row. No ordering
        is requested. None remains a parameter to equality, so SQL NULL rows do not match it as they
        would an IS NULL predicate.

        Example:
            >>> rows = database.search("stores", "store_name", "books")  # doctest: +SKIP


        :param table: Trusted table identifier interpolated into SELECT.
        :param column: Trusted column identifier interpolated into the equality predicate.
        :param search_term: SQLite-bindable value compared using SQL equality.
        :return: An eager list of bound Row objects, possibly empty; SQL, shape, and Row errors propagate.
        """
        cur = self.conn.execute(f"SELECT * FROM `{table}` WHERE `{column}` = ?", (search_term,))
        cols = self.driver_wrapper._columns(table)
        return [Row(database=self, row_dict=dict(zip(cols, row, strict=True))) for row in cur.fetchall()]

    def delete(self, row: Row) -> None:
        """
        Delete the row selected by its table and primary-key value, then commit the entire
        connection. No affected-row count is checked and no rollback handling is added. Foreign-key
        behavior follows the supplied connection; physical Store bytes are not involved.

        Example:
            >>> database.delete(row)  # doctest: +SKIP


        :param row: Row supplying a trusted table name and its primary-key value.
        :return: None after executing and committing, including a zero-row deletion; errors propagate.
        """
        table = row.table
        id_col = self.driver_wrapper.get_id_column(table)
        self.conn.execute(f"DELETE FROM `{table}` WHERE `{id_col}` = ?", (row[id_col],))
        self.conn.commit()



def build_mini_db(db_path: Path) -> MiniDB:
    """
    Open a SQLite connection, enable foreign keys, run the FRBR schema generator, and return its
    MiniDB wrapper. Parent directories are not created here. The caller must close the returned
    connection; a failure before return has no explicit connection cleanup in this helper.

    Example:
        >>> database = build_mini_db(tmp_path / "mini.sqlite")  # doctest: +SKIP
        >>> database.conn.close()  # doctest: +SKIP


    :param db_path: Path stringified for sqlite3.connect; use a suitable test database destination.
    :return: MiniDB wrapping the initialized open connection; connection, PRAGMA, or schema-generation errors propagate.
    """
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys = ON;")
    frbr_gen.create_new_database(conn)
    return MiniDB(conn)
