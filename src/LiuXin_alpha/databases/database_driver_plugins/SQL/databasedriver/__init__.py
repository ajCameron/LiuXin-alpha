
"""
Common base driver class for any SQL implementation.
"""

from __future__ import print_function, annotations

import pathlib
import shutil
import os
import time
import gc
import pprint

from typing import TYPE_CHECKING, Union, Optional, Any

from LiuXin_alpha.utils.ptempfiles import get_scratch_folder
from LiuXin_alpha.utils.storage.local.file_backup import backup_local_file

from LiuXin_alpha.utils.logging import default_log
from LiuXin_alpha.errors import DatabaseDriverError, DatabaseIntegrityError, InputIntegrityError

from LiuXin_alpha.utils.paths import path_ok

from LiuXin_alpha.utils.logging import LiuXin_print, LiuXin_debug_print

from LiuXin_alpha.preferences import preferences

from LiuXin_alpha.constants import VERBOSE_DEBUG

if TYPE_CHECKING:

    from LiuXin_alpha.databases.api.macros_api import MacrosAPI
    from LiuXin_alpha.databases.api.database_api import DatabaseAPI



class SQLBaseDriver:
    """
    Common core for any SQL driver system.
    """
    db: "DatabaseAPI"

    @property
    def macros(self) -> "MacrosAPI":
        """
        Returns the macros helper class.

        :return:
        """
        return self._macros

    def exists(self) -> bool:
        """
        Checks to see if the database file exists - returns True if it does, false if it doesn't.

        :return:
        """
        return os.path.exists(self.database_path)

    def make_scratch(self) -> str:
        """
        Makes a scratch copy of the database - shifts over to using that instead of the main one.

        :return:
        """
        scratch_folder = get_scratch_folder()
        scratch_db_path = os.path.join(scratch_folder, "scratch.db")
        shutil.copyfile(src=self.database_path, dst=scratch_db_path)
        self.database_path = scratch_db_path

        return self.database_path

    def zero_prop_cache(self) -> None:
        self._zero_prop_cache()

    def _zero_prop_cache(self):
        """
        Zero any cached properties - used when significant changes have.may have been made to the database.

        :return:
        """
        self.tables = None
        self.tables_and_columns = None
        self.categorized_tables = None
        self.all_column_names = set()

        self.locations = None

        try:
            self.db.refresh_db_metadata()
        except Exception as e:
            pass

    def _register_open_connection(self, conn):
        """
        Register a newly created connection so we can close all open handles later.

        We intentionally keep this simple (a plain list) so it works with both sqlite3 and APSW-backed
        connection objects.
        """
        try:
            self._open_connections.append(conn)
        except Exception as e:
            # If tracking fails for any reason, do not break core DB functionality.
            default_log.log_exception("Couldn't store connection to log.", exc=e)
        return conn

    def _close_all_open_connections(self) -> None:
        """
        Best-effort close of every connection opened by this driver instance.

        On Windows, an open SQLite connection keeps the database file locked, preventing deletion.
        """
        conns = []
        try:
            conns.extend(list(getattr(self, "_open_connections", []) or []))
        except Exception:
            pass

        primary = getattr(self, "conn", None)
        if primary is not None:
            conns.append(primary)

        for c in conns:
            try:
                c.close()
            except Exception:
                pass

        try:
            self._open_connections = []
        except Exception:
            pass
        self.conn = None

    def close(self) -> None:
        """
        Shutdown the connection to the database - but leave the driver class in existence so it can be re-opened.

        On Windows, failing to close SQLite connections will keep the database file locked.

        :return:
        """
        self._close_all_open_connections()

    def refresh(self, reconnect: bool = False):
        """
        Invalidate cached driver state.

        Historically, refresh() would close and reopen the driver's primary connection. That behaviour makes
        long-lived helper objects fragile (they can hold a reference to a connection that gets closed behind their
        back) and it breaks SQLite TEMP objects (e.g. temp triggers) which are scoped to a single connection.

        The driver already has a deterministic close()/reopen() path for tests and Windows file-lock concerns.
        For day-to-day cache invalidation, we keep the primary connection alive unless it is missing/broken or
        a reconnect is explicitly requested.

        :param reconnect: If True, force closing and reopening the primary connection.

        :return:
        """
        conn = getattr(self, "conn", None)

        # Ensure we have a usable connection.
        needs_reconnect = reconnect or conn is None
        if not needs_reconnect and conn is not None:
            try:
                # Fast liveness probe; sqlite3 raises ProgrammingError on closed connections.
                conn.execute("SELECT 1")
            except Exception:
                needs_reconnect = True

        if needs_reconnect:
            old = getattr(self, "conn", None)
            if old is not None:
                try:
                    old.close()
                except Exception:
                    pass
            self.conn = self.get_connection()

        self._zero_prop_cache()

    def reopen(self) -> None:
        """
        Re-opens the connection to the database.

        Important: close any existing connection before replacing it to avoid leaking file handles.

        :return:
        """
        old = getattr(self, 'conn', None)
        if old is not None:
            try:
                old.close()
            except Exception:
                pass
        self.conn = self.get_connection()

    def direct_backup(self, path: Optional[Union[str, pathlib.Path]]=None):
        """
        Backup the DatabasePing.

        :param path: The path to back up the database to - if none is provided, autogenerated
        :return:
        """
        # Acquire a conn object - use it to lock the DatabasePing
        conn = self.get_connection()
        with conn:

            # Preform the backup
            backup_status = backup_local_file(self.database_path, override_path=path)
            if backup_status:
                info_str = "DatabasePing backup successfully complete.\n"
                default_log.log_variables(
                    info_str,
                    "INFO",
                    ("database_path", self.database_path),
                    ("database_backup_path", backup_status),
                )
            else:
                wrn_str = "DatabasePing backup failed.\n"
                default_log.log_variables(
                    wrn_str,
                    "WARN",
                    ("database_path", self.database_path),
                    ("database_backup_path", backup_status),
                )
                raise DatabaseDriverError(wrn_str)

    def direct_self_delete(self):

        """

        Delete the on_disc database file.


        On Windows, SQLite keeps database files locked while any connection is open.

        This method therefore closes all connections created by this driver instance

        before attempting to delete the underlying file.

        """

        # Ensure we release all file handles held by this driver (especially important on Windows).

        self._close_all_open_connections()


        # Check that the file can be accessed and the process has the privileges to run the delete

        if not path_ok(self.database_path):
            err_str = 'DatabasePing file cannot be accessed for delete.\n'
            err_str += 'database_file_path: {}\n'.format(self.database_path)
            default_log.error(err_str)
            raise DatabaseDriverError(err_str)


        # Remove the database file (retry a couple of times on Windows to allow handle release).
        attempts = 6 if os.name == 'nt' else 1

        last_err = None

        for i in range(attempts):
            try:
                os.remove(self.database_path)
                last_err = None
                break
            except FileNotFoundError:
                last_err = None
                break
            except PermissionError as e:
                last_err = e
                if os.name == 'nt' and i < attempts - 1:
                    gc.collect()
                    time.sleep(0.05 * (i + 1))
                    continue
            except OSError as e:
                last_err = e
                break


        if last_err is not None:
            err_str = 'DatabasePing cannot be deleted.\n'
            err_str += 'database_path: {}\n'.format(self.database_path)
            err_str += 'error: {}\n'.format(last_err)
            raise DatabaseDriverError(err_str)

        # Check that the delete has gone through i.e. the path no longer exists.
        if os.path.exists(self.database_path):
            err_str = 'DatabasePing cannot be deleted - process failed silently.\n'
            err_str += 'database_path: {}\n'.format(self.database_path)
            raise DatabaseDriverError(err_str)

        # With the database gone the caches should also be emptied
        self._zero_prop_cache()


    def simple_print_progress_handler(self) -> None:
        """
        The most basic progress handler - prints the number of events every hundred million events.

        :return:
        """
        if self.event_count % 100000000 == 0:
            LiuXin_print(self.event_count)
            self.event_count += 1
        else:
            self.event_count += 1

    def direct_identify_table_from_row(self, row_dict: dict[str, Any]) -> Optional[str]:
        """
        Takes a row. Attempts to identify which row it came from.

        :param row_dict: The row (dict) to be parsed
        :return table_name: The table name (string)
        """
        if "table" in row_dict.keys():
            del row_dict["table"]

        tables_and_columns = self.direct_get_tables_and_columns()
        table = tables_and_columns.keys()
        row_columns_set = set(key for key in row_dict.keys())

        if VERBOSE_DEBUG:
            err_str = "Calling direct_identify_table_from_row.\n"
            err_str += "Table_and_columns: " + repr(tables_and_columns) + "\n"
            err_str += "Tables: " + repr(table) + "\n"
            LiuXin_debug_print(err_str)

        # if this method is called with a null row it will complain. If warn is true
        if len(row_dict) == 0:
            info_str = "Warning - direct_identify_table_from_row called with empty row."
            default_log.info(info_str)
            return False

        for table in tables_and_columns.keys():

            column_heading_set = set(heading for heading in tables_and_columns[table])
            if row_columns_set.issubset(column_heading_set):
                return table

        # If this point in the algorithm has been reached then something has gone wrong.
        # Searching for partial matches - tables with some, but not all of the column names
        partial_match_tables = set()
        unmatched_columns = set()

        for column_heading in row_dict.keys():
            try:
                column_table = self.direct_identify_table_from_column(column_heading)
                partial_match_tables.add(column_table)
            except InputIntegrityError:
                unmatched_columns.add(column_heading)

        err_str = "SQLite:databasedriver:direct_identify_table_from_row unable to find matching table.\n"
        if len(partial_match_tables) > 0:
            err_str += "partial matches found for some column_headings.\n"
            err_str += "partial_match_tables: " + pprint.pformat(partial_match_tables) + "\n"

        if len(unmatched_columns) > 0:
            err_str += "some column_headings could not be matched.\n"
            err_str += "unmatched_columns: " + pprint.pformat(unmatched_columns) + "\n"
        err_str += "row_dict: " + pprint.pformat(row_dict) + "\n"

        if preferences.parse(
            "include_full_rep_if_row_cant_be_identified",
            rtn_value_type="bool",
            default=False,
        ):
            err_str += "tables_and_columns: " + pprint.pformat(tables_and_columns) + "\n"
        default_log.error(err_str)
        raise DatabaseIntegrityError(err_str)

    def get_id_from_row_dict(self, row_dict):
        """
        Takes a row. Extracts an id from it if possible. If not returns False
        :param row_dict:
        """
        row_table = self.direct_identify_table_from_row(row_dict)
        row_id_column = self.direct_get_id_column(row_table)

        if row_id_column not in row_dict.keys():
            return False
        else:
            return row_dict[row_id_column]





    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - CUSTOM COLUMN CREATION METHODS

    # Todo: Merge with zero_prop_cache - they do the same thing
    def call_after_table_changes(self) -> None:
        """
        Call after any operations which might change the table content of the database.

        :return:
        """
        self._zero_prop_cache()
        self.tables_and_columns = None

    #
    # ----------------------------------------------------------------------------------------------------------------------

    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - DB CREATION METHODS

    def direct_create_new_database(self) -> None:
        """
        Creates a new database using the SQL and other instructions present in the database_generator

        :return None:
        """
        if not os.path.exists(os.path.dirname(self.database_path)):
            os.makedirs(os.path.dirname(self.database_path))

        conn = self.get_connection()
        self._create_new_database(conn)

        # Defensive: ensure FRBR constant tables are populated/locked.
        # (Safe no-op on non-FRBR schemas.)
        try:
            from LiuXin_alpha.utils.language_tools import ensure_languages_seeded_and_locked

            ensure_languages_seeded_and_locked(conn)
        except Exception:
            pass

        conn.commit()
        conn.close()

    #
    # ----------------------------------------------------------------------------------------------------------------------


def _create_new_database(conn):
    from LiuXin_alpha.databases.database_driver_plugins.SQL.database_generator_frbr.database_generator import (
        create_new_database,
    )

    return create_new_database(conn)
