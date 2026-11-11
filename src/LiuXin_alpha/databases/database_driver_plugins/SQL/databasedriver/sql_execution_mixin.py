"""
Execute SQL statements, batches and scripts with explicit driver connection policies.
"""

import sqlite3
from collections.abc import Iterable
from typing import Optional
from contextlib import closing

from LiuXin_alpha.databases.db_types.driver_types import (
    QueryCursor,
    SQLParameters,
    RowSQLConnection,
)
from LiuXin_alpha.databases.sql_statements import sqlite_row_operation
from LiuXin_alpha.databases.transactions import transaction_active
from LiuXin_alpha.errors import DatabaseDriverError, InputIntegrityError
from LiuXin_alpha.utils.libraries.liuxin_six import basestring, force_unicode
from LiuXin_alpha.utils.logging import default_log

from .write_result import WriteResult

from LiuXin_alpha.databases.database_driver_plugins.SQL.databasedriver._state import (
    SQLDriverMixinState,
    driver_operations,
)


class SQLExecutionMixin(SQLDriverMixinState):
    """
    Provide low-level SQL execution helpers for concrete drivers.

    Helpers remove a legacy leading backslash/newline but otherwise execute trusted SQL.
    Script execution is not guaranteed atomic by these wrappers.

    Example:
        ``driver.direct_execute("SELECT 1")`` returns the primary connection's result
        cursor.
    """

    # Todo: This should be something like "execute sql script" - to distinguish it from the execute method in the conn
    def direct_execute_sql_script(self, script: str) -> None:
        """
        Run a script on a fresh connection and close it after success.

        The supplied script must be text; SQL backend errors propagate. The owned
        connection closes even when execution fails. No cache refresh is requested.

        Example:
            ``driver.direct_execute_sql_script("CREATE TABLE example (id INTEGER);")``
            runs a script.


        :param script: SQL script text, not a list of statement fragments.
        :return: ``None``.
        """
        # Defensive: legacy code sometimes used raw triple-quoted strings beginning with "\\\n"
        # (intended to suppress the first newline). In raw strings that backslash becomes literal,
        # and SQLite fails with "unrecognized token: \\".
        if isinstance(script, str):
            if script.startswith("\\\r\n"):
                script = script[3:]

            elif script.startswith("\\\n"):
                script = script[2:]

        with closing(driver_operations(self).get_connection()) as conn:
            conn.executescript(script)

    # Todo: Check that the return is correctly typed
    def direct_execute_sql(
        self, sql: str, parameters: SQLParameters | None = None
    ) -> Optional[int]:
        """
        Execute one statement on a fresh connection and commit.

        Return lastrowid, closing the owned cursor and connection on every path.
        Backend execution errors propagate; failed writes are not committed.

        Example:
            ``driver.direct_execute_sql("INSERT INTO example DEFAULT VALUES")`` returns
            the cursor lastrowid.


        :param sql: SQL text to execute, with placeholders when bindings are supplied.
        :param parameters: Optional sequence of values bound to SQL placeholders.
        :return: The backend cursor lastrowid, whose meaning depends on the statement.
        """
        # Defensive: tolerate legacy raw triple-quoted strings beginning with "\\\n".
        if isinstance(sql, str):
            if sql.startswith("\\\r\n"):
                sql = sql[3:]
            elif sql.startswith("\\\n"):
                sql = sql[2:]

        with closing(driver_operations(self).get_connection()) as conn:
            with closing(conn.execute(sql) if parameters is None else conn.execute(sql, parameters)) as cursor:
                last_row_id = cursor.lastrowid
            conn.commit()
            return last_row_id

    def direct_get_table_sqlite(
        self, table: str, conn: RowSQLConnection | None = None
    ) -> str | None:
        """
        Read a table's stored CREATE statement from sqlite_master.

        Bind the name as a value. Missing tables, including view-only matches, raise
        InputIntegrityError. Close an acquired connection; preserve a supplied one.

        Example:
            ``driver.direct_get_table_sqlite("books")`` retrieves a physical table
            definition.


        :param table: Exact table name used in the bound sqlite_master lookup.
        :param conn: Optional existing connection; ``None`` acquires a new one.
        :return: The stored SQL text, which may be null for internal tables.
        """
        if conn is None:
            with closing(driver_operations(self).get_connection()) as owned:
                return self.direct_get_table_sqlite(table, owned)

        # Parameterize table name to avoid quoting issues and accidental injection.
        stmt = "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?;"
        with closing(conn.execute(stmt, (table,))) as cursor:
            row = cursor.fetchone()
            if row is not None:
                return row[0]
        raise InputIntegrityError("Table name was probably not found")

    # Todo: Add zero methods for all the data caches after any of these are used
    # Ideally these would never be used. They are here for testing,
    def direct_execute(
        self, sql: str, values: SQLParameters | int | None = None
    ) -> QueryCursor:
        """
        Execute one statement using a live primary connection and its transaction
        context.

        Open a missing primary handle; propagate a failed liveness probe without
        replacing an existing handle or completing its transaction. Convert an integer
        binding to a one-element text tuple. Statement execution still uses the
        connection context and its commit/rollback semantics, completing any
        earlier primary work. Row writes start a transaction explicitly. Their
        RETURNING results are buffered and the statement closed before commit;
        SELECT results remain native streaming cursors. Execution failures
        become DatabaseDriverError; finally attempt a cache refresh.

        Example:
            ``cursor = driver.direct_execute("SELECT ?", (3,))`` returns the query
            result cursor.


        :param sql: SQL text to execute, with placeholders when bindings are supplied.
        :param values: Optional bindings; a bare integer is converted to a one-element
            string tuple.
        :return: A query cursor; the caller owns its cursor lifetime. Write results
            remain readable after the operation has committed.
        """
        if isinstance(values, int):
            values = (force_unicode(values),)

        # Defensive: some legacy code used raw triple-quoted strings beginning with "\\\n"
        # (intended to suppress the first newline). In raw strings that backslash becomes literal,
        # and SQLite fails with "unrecognized token: \\".
        if isinstance(sql, str):
            if sql.startswith("\\\r\n"):
                sql = sql[3:]
            elif sql.startswith("\\\n"):
                sql = sql[2:]

        # Ensure we have a usable primary connection.
        conn = getattr(self, "conn", None)
        if conn is None:
            conn = driver_operations(self).get_connection()
            self.conn = conn
        else:
            with closing(conn.execute("SELECT 1")):
                pass

        try:
            with conn:
                writing = sqlite_row_operation(sql)[0] in {
                    "INSERT", "UPDATE", "DELETE", "REPLACE"
                }
                if writing and not transaction_active(conn):
                    conn.execute("BEGIN").close()
                if values is not None:
                    query_results = conn.execute(sql, values)
                else:
                    query_results = conn.execute(sql)
                if writing and query_results.description is not None:
                    with closing(query_results):
                        result = WriteResult(query_results)
                    return result
            return query_results

        except sqlite3.OperationalError as e:
            err_str = "Attempting to execute that SQL caused an operational error."
            err_str = default_log.log_exception(
                err_str, e, "ERROR", ("sql", sql), ("values", values)
            )
            raise DatabaseDriverError(err_str)

        except ValueError as e:
            err_str = "Attempting to execute that SQL caused a ValueError"
            err_str = default_log.log_exception(
                err_str, e, "ERROR", ("sql", sql), ("values", values)
            )
            raise DatabaseDriverError(err_str)

        except Exception as e:
            err_str = "Attempting to execute that SQL threw an Exception"
            err_str = default_log.log_exception(
                err_str, e, "ERROR", ("sql", sql), ("values", values)
            )
            raise DatabaseDriverError(err_str)

        # Todo: This seems to be a good idea. Do it more?
        finally:
            # Invalidate any driver-side caches without forcibly replacing the primary connection.
            try:
                driver_operations(self).refresh()
            except Exception:
                pass

    def direct_executemany(
        self, sql: str, values: Iterable[SQLParameters] | None = None
    ) -> None:
        """
        Execute a batch on the primary connection, or delegate an unbound script.

        Normalize scalar entries in an outer tuple before SQL execution. Consume
        other binding iterables once, without retrying failures. The connection
        context explicitly starts a transaction if idle, including for CTE writes,
        and rolls back on input, adaptation or SQL errors; DatabaseDriverError
        retains the original cause. As with direct_execute, this context also
        completes earlier primary work. Refresh caches after success. With
        values=None, delegate to direct_executescript.

        Example:
            ``driver.direct_executemany("INSERT INTO example(value) VALUES (?)", ("a",
            "b"))`` inserts two scalar values.


        :param sql: SQL text to execute, with placeholders when bindings are supplied.
        :param values: Binding rows, an outer tuple of scalar values, or ``None`` for
            script execution.
        :return: ``None``.
        """
        # Defensive normalization for the same leading "\\\n" raw-string trap as direct_execute().
        if isinstance(sql, str):
            if sql.startswith("\\\r\n"):
                sql = sql[3:]
            elif sql.startswith("\\\n"):
                sql = sql[2:]

        # sqlite3 can only run one statement per execute()/executemany() call.
        # If no bindings are supplied, treat this as a multi-statement script.
        # (Used by custom-column cleanup: DROP INDEX; DROP TABLE; ...).
        if values is None:
            return self.direct_executescript(sql)

        conn = getattr(self, "conn", None)
        if conn is None:
            conn = driver_operations(self).get_connection()
            self.conn = conn

        try:
            with conn:
                if not transaction_active(conn):
                    conn.execute("BEGIN").close()
                # Preserve the documented scalar-tuple shorthand before any SQL.
                # Iterators remain streaming; errors must leave this transaction.
                if isinstance(values, tuple):
                    values = tuple(
                        (value,) if isinstance(value, (basestring, int, float)) else value
                        for value in values
                    )
                conn.executemany(sql, values).close()

        except Exception as e:
            err_str = "direct_executemany has failed"
            err_str = default_log.log_exception(
                err_str, e, "ERROR", ("sql", sql), ("values", values)
            )
            raise DatabaseDriverError(err_str) from e

        try:
            driver_operations(self).refresh()
        except Exception:
            pass

    # Todo: Merged with the above, and deprecate one
    def direct_executescript(self, sqlscript: str) -> None:
        """
        Run trusted multi-statement SQL on the primary connection and refresh caches.

        Create a primary handle if absent, wrap execution failures as
        DatabaseDriverError and attempt refresh in finally. Backend executescript
        transaction behavior still applies.

        Example:
            ``driver.direct_executescript("CREATE TABLE example(id INTEGER); INSERT INTO
            example VALUES (1);")`` runs both statements.


        :param sqlscript: SQL script text passed to the backend executescript method.
        :return: ``None``.
        """
        # Defensive normalization for the same leading "\\\n" raw-string trap as direct_execute().
        if isinstance(sqlscript, str):
            if sqlscript.startswith("\\\r\n"):
                sqlscript = sqlscript[3:]
            elif sqlscript.startswith("\\\n"):
                sqlscript = sqlscript[2:]

        conn = getattr(self, "conn", None)
        if conn is None:
            conn = driver_operations(self).get_connection()
            self.conn = conn

        try:
            with conn:
                conn.executescript(sqlscript)
        except Exception as e:
            err_str = "Executing a script has failed"
            err_str = default_log.log_exception(
                err_str, e, "ERROR", ("sql_script", sqlscript)
            )
            raise DatabaseDriverError(err_str)
        finally:
            try:
                driver_operations(self).refresh()
            except Exception:
                pass
