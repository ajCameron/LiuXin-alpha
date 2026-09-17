
"""
Execute SQL statements, batches and scripts with explicit driver connection policies.
"""

import sqlite3

from typing import Union, Optional

from LiuXin_alpha.errors import InputIntegrityError

from LiuXin_alpha.utils.libraries.liuxin_six import basestring, force_unicode

from LiuXin_alpha.utils.logging import default_log
from LiuXin_alpha.errors import DatabaseDriverError


class SQLExecutionMixin:
    """
    Provide low-level SQL execution helpers for concrete drivers.

    Helpers remove a legacy leading backslash/newline but otherwise execute trusted SQL. Script execution is not guaranteed atomic by these wrappers.

    Example:
        ``driver.direct_execute("SELECT 1")`` returns the primary connection's result cursor.
    """

    # Todo: This should be something like "execute sql script" - to distinguish it from the execute method in the conn
    def direct_execute_sql_script(
            self,
            script: Union[str, list[str]]) -> None:
        """
        Run a script on a fresh connection and close it after success.

        No cache refresh or finally cleanup is performed. Despite the annotation, list input is passed directly to the backend and is not joined.

        Example:
            ``driver.direct_execute_sql_script("CREATE TABLE example (id INTEGER);")`` runs a script.


        :param script: SQL script text; the backend must accept any non-string input.
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

        conn = self.get_connection()
        conn.executescript(script)
        conn.close()

    # Todo: Check that the return is correctly typed
    def direct_execute_sql(self, sql: str, parameters: Optional[tuple[str, ...]] = None) -> Optional[int]:
        """
        Execute one statement on a fresh connection and commit.

        Return lastrowid without explicitly closing the connection or refreshing caches; backend execution errors propagate.

        Example:
            ``driver.direct_execute_sql("INSERT INTO example DEFAULT VALUES")`` returns the cursor lastrowid.


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

        conn = self.get_connection()
        if parameters is None:
            last_row_id = conn.execute(sql).lastrowid
        else:
            last_row_id = conn.execute(sql, parameters).lastrowid
        conn.commit()
        return last_row_id

    def direct_get_table_sqlite(self, table, conn=None):
        """
        Read a table's stored CREATE statement from sqlite_master.

        Bind the name as a value. Missing tables, including view-only matches, raise InputIntegrityError; neither supplied nor acquired connections are closed here.

        Example:
            ``driver.direct_get_table_sqlite("books")`` retrieves a physical table definition.


        :param table: Exact table name used in the bound sqlite_master lookup.
        :param conn: Optional existing connection; ``None`` acquires a new one.
        :return: The stored SQL text, which may be null for internal tables.
        """
        if conn is None:
            conn = self.get_connection()

        # Parameterize table name to avoid quoting issues and accidental injection.
        stmt = "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?;"
        for row in conn.execute(stmt, (table,)):
            return row[0]
        else:
            raise InputIntegrityError("Table name was probably not found")

    # Todo: Add zero methods for all the data caches after any of these are used
    # Ideally these would never be used. They are here for testing,
    def direct_execute(
            self,
            sql: Union[str, tuple[str, ...], list[str]],
            values: Optional[tuple[str]] = None) -> None:
        """
        Execute one statement using a live primary connection and its transaction context.

        Replace a missing/broken primary handle; convert an integer binding to a one-element text tuple. Execution failures become DatabaseDriverError. Finally attempt a cache refresh while preserving a usable handle, so TEMP objects remain available.

        Example:
            ``cursor = driver.direct_execute("SELECT ?", (3,))`` returns the query result cursor.


        :param sql: SQL text to execute, with placeholders when bindings are supplied.
        :param values: Optional bindings; a bare integer is converted to a one-element string tuple.
        :return: The backend execution result, normally a cursor, despite the None annotation.
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
            conn = self.get_connection()
            self.conn = conn
        else:
            try:
                conn.execute("SELECT 1")
            except Exception:
                try:
                    conn.close()
                except Exception:
                    pass
                conn = self.get_connection()
                self.conn = conn

        try:
            with conn:
                if values is not None:
                    query_results = conn.execute(sql, values)
                else:
                    query_results = conn.execute(sql)
            return query_results

        except sqlite3.OperationalError as e:
            err_str = "Attempting to execute that SQL caused an operational error."
            err_str = default_log.log_exception(err_str, e, "ERROR", ("sql", sql), ("values", values))
            raise DatabaseDriverError(err_str)

        except ValueError as e:
            err_str = "Attempting to execute that SQL caused a ValueError"
            err_str = default_log.log_exception(err_str, e, "ERROR", ("sql", sql), ("values", values))
            raise DatabaseDriverError(err_str)

        except Exception as e:
            err_str = "Attempting to execute that SQL threw an Exception"
            err_str = default_log.log_exception(err_str, e, "ERROR", ("sql", sql), ("values", values))
            raise DatabaseDriverError(err_str)

        # Todo: This seems to be a good idea. Do it more?
        finally:
            # Invalidate any driver-side caches without forcibly replacing the primary connection.
            try:
                self.refresh()
            except Exception:
                pass

    def direct_executemany(self, sql: str, values: Optional[tuple[str, ...]] = None) -> None:
        """
        Execute a batch on the primary connection, or delegate an unbound script.

        Only outer tuples receive scalar-to-one-tuple normalization. ValueError retries try alternate binding shapes; execution failures become DatabaseDriverError. Refresh caches after success. With values=None, delegate to direct_executescript.

        Example:
            ``driver.direct_executemany("INSERT INTO example(value) VALUES (?)", ("a", "b"))`` inserts two scalar values.


        :param sql: SQL text to execute, with placeholders when bindings are supplied.
        :param values: Binding rows, an outer tuple of scalar values, or ``None`` for script execution.
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

        # Preflight the values to try and transform them into something that'll behave as expected
        if isinstance(values, tuple):
            new_values = list()
            for update_val in values:
                if isinstance(update_val, (basestring, int, float)):
                    new_values.append((update_val,))
                else:
                    new_values.append(update_val)
            values = tuple(new_values)

        # Todo: Tests! Check this.
        # Todo: Theoretically possible to fool the database into doing manifestly stupid stuff here by feeding in the
        conn = getattr(self, "conn", None)
        if conn is None:
            conn = self.get_connection()
            self.conn = conn

        try:
            with conn:

                if values is not None:
                    try:
                        conn.executemany(sql, values)
                    except ValueError:
                        try:
                            conn.executemany(sql, tuple(values))
                        except ValueError:
                            values = tuple([(v,) for v in values])
                            conn.executemany(sql, values)
                else:
                    conn.executemany(sql, ())

        except Exception as e:
            err_str = "direct_executemany has failed"
            err_str = default_log.log_exception(err_str, e, "ERROR", ("sql", sql), ("values", values))
            raise DatabaseDriverError(err_str)

        try:
            self.refresh()
        except Exception:
            pass

    # Todo: Merged with the above, and deprecate one
    def direct_executescript(self, sqlscript):
        """
        Run trusted multi-statement SQL on the primary connection and refresh caches.

        Create a primary handle if absent, wrap execution failures as DatabaseDriverError and attempt refresh in finally. Backend executescript transaction behavior still applies.

        Example:
            ``driver.direct_executescript("CREATE TABLE example(id INTEGER); INSERT INTO example VALUES (1);")`` runs both statements.


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
            conn = self.get_connection()
            self.conn = conn

        try:
            with conn:
                conn.executescript(sqlscript)
        except Exception as e:
            err_str = "Executing a script has failed"
            err_str = default_log.log_exception(err_str, e, "ERROR", ("sql_script", sqlscript))
            raise DatabaseDriverError(err_str)
        finally:
            try:
                self.refresh()
            except Exception:
                pass
