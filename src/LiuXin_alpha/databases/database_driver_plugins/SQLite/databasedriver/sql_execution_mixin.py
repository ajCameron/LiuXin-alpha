
import sqlite3

from LiuXin_alpha.errors import InputIntegrityError

from LiuXin_alpha.utils.libraries.liuxin_six import basestring, force_unicode

from LiuXin_alpha.utils.logging import default_log
from LiuXin_alpha.errors import DatabaseDriverError


class SQLExecutionMixin:
    """
    Direct execution methods on the database.
    """

    # Todo: This should be something like "execute sql script" - to distinguish it from the execute method in the conn
    def executescript(self, script):
        """
        Allows arbitrary scripts to be executed on the database.
        Try not to shoot yourself in the foot.
        :param script: This will be executed directly on the database.
        :return:
        """
        conn = self.get_connection()
        conn.executescript(script)
        conn.close()

    def execute_sql(self, sql, parameters=None):
        """
        Execute the given sql using a new conn, which will be closed after the execution.
        :param sql:
        :param parameters:
        :return:
        """
        conn = self.get_connection()
        last_row_id = conn.execute(sql, parameters).lastrowid
        conn.commit()
        return last_row_id


    def get_table_sqlite(self, table, conn=None):
        """
        Gets the SQLite for the given table. Useful for debugging.
        :param table:
        :param conn: Allows passing in a connection - provided as this is intended to be used for debugging.
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
    def direct_execute(self, sql, values=None):
        """Execute SQL directly on the database.

        Historically this method opened a fresh connection for every call and then called driver.refresh(), which
        (also historically) closed and replaced the driver's primary connection. That combination made it easy for
        long-lived helper objects to hold stale/closed connection references.

        We now prefer executing against the driver's primary connection. This avoids leaking connection handles,
        preserves SQLite TEMP objects on that connection, and keeps helper classes (like CustomColumns/CalibreCache)
        from being surprised by connection replacement.

        :param sql: SQL code to execute on the database
        :param values: The values to execute with the code.
        """
        if isinstance(values, int):
            values = (force_unicode(values),)

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
        finally:
            # Invalidate any driver-side caches without forcibly replacing the primary connection.
            try:
                self.refresh()
            except Exception:
                pass

    def execute_sql(self, sql, values=None):
        """
        Front end for the direct_execute method.
        :param sql:
        :param values:
        :return:
        """
        self.direct_execute(sql=sql, values=values)

    def direct_executemany(self, sql, values=None):
        """
        Executes many statements on the database.
        Tries to preform sensible input transforms on the values before executing them. This might lead to some problems
        but I can't immediately think of cases where they would, and it's a bit more convenient this way.
        e.g. if values=("Some string", "Another string") these will be transformed to
                       (("Some string", ), ("Another string", )) before any attempt is made to execute them directly.
        (As, usually, you don't supply bindings in the form of chars in a string - which seems to be the default
         assumption SQLite makes)
        :param sql:
        :param values:
        :return:
        """
        # Preflight the values to try and transform them into something that'll behave as expected
        if isinstance(values, tuple):
            new_values = list()
            for update_val in values:
                if isinstance(update_val, (basestring, int, float)):
                    new_values.append((update_val,))
                else:
                    new_values.append(update_val)
            values = tuple(new_values)

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


    def direct_executescript(self, sqlscript):
        """
        Execute a script on the database
        :param sqlscript: A series of statements to execute. Seperated by ;
        """
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