
"""
Share SQL driver lifecycle, schema-cache invalidation and row identification helpers.
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
    Provide lifecycle helpers for concrete drivers that supply connections and schema methods.

    The host initializes connection tracking, cached schema attributes and the database wrapper.

    Example:
        A concrete SQLite driver inherits ``SQLBaseDriver`` and supplies ``get_connection()``.
    """
    db: "DatabaseAPI"

    @property
    def macros(self) -> "MacrosAPI":
        """
        Expose the macro helper already installed on this driver.

        Example:
            ``driver.macros`` accesses the configured helper without creating one.


        :return: The stored ``_macros`` object.
        """
        return self._macros

    def exists(self) -> bool:
        """
        Check whether the configured database path exists, without opening it.

        Example:
            ``driver.exists()`` also returns true for a directory at that path.


        :return: Whether ``os.path.exists(database_path)`` succeeds.
        """
        return os.path.exists(self.database_path)

    def make_scratch(self) -> str:
        """
        Copy the database file into a scratch directory and switch its configured path.

        Existing connection handles are not replaced; callers must manage their lifecycle. Copy errors propagate.

        Example:
            ``driver.make_scratch()`` sets the path to a new ``scratch.db`` copy.


        :return: The new database path.
        """
        scratch_folder = get_scratch_folder()
        scratch_db_path = os.path.join(scratch_folder, "scratch.db")
        shutil.copyfile(src=self.database_path, dst=scratch_db_path)
        self.database_path = scratch_db_path

        return self.database_path

    def zero_prop_cache(self) -> None:
        """
        Invoke the internal cache invalidation and metadata refresh routine.

        Example:
            ``driver.zero_prop_cache()`` refreshes metadata after direct database work.


        :return: ``None``.
        """
        self._zero_prop_cache()

    def _zero_prop_cache(self):
        """
        Invalidate schema caches when the schema version changed or cannot be checked.

        Locations are always invalidated. Wrapper-derived cache clearing and metadata refresh are best effort; their exceptions are suppressed.

        Example:
            ``driver._zero_prop_cache()`` retains schema caches if the cached schema version still matches.


        :return: ``None``.
        """
        schema_changed = True
        schema_version = getattr(self, "direct_get_schema_version", None)
        cached_schema_version = getattr(
            self,
            "_schema_version_cached",
            None,
        )
        if callable(schema_version) and cached_schema_version is not None:
            try:
                current_schema_version = schema_version()
                schema_changed = (
                    current_schema_version is None
                    or current_schema_version != cached_schema_version
                )
            except Exception:
                schema_changed = True

        if schema_changed:
            self.tables = None
            self.tables_and_columns = None
            self.categorized_tables = None
            self.all_column_names = set()

        self.locations = None

        # Schema mutations can leave wrapper-side derived caches stale when callers use the raw driver API.
        if schema_changed:
            try:
                driver_wrapper = getattr(self.db, "driver_wrapper", None)
                clear_derived = getattr(
                    driver_wrapper,
                    "_clear_derived_schema_caches",
                    None,
                )
                if clear_derived is not None:
                    clear_derived()
            except Exception:
                pass

        try:
            self.db.refresh_db_metadata()
        except Exception as e:
            pass

    def _register_open_connection(self, conn):
        """
        Track a connection for later bulk closing and return it unchanged.

        Tracking failures are logged without preventing connection use.

        Example:
            ``conn = driver._register_open_connection(conn)`` records an opened handle.


        :param conn: Open connection supplied by the caller.
        :return: The supplied connection.
        """
        try:
            self._open_connections.append(conn)
        except Exception as e:
            # If tracking fails for any reason, do not break core DB functionality.
            default_log.log_exception("Couldn't store connection to log.", exc=e)
        return conn

    def _close_all_open_connections(self) -> None:
        """
        Best-effort close tracked handles and the primary connection.

        Individual close errors are suppressed; tracking is cleared and ``conn`` becomes ``None``.

        Example:
            ``driver._close_all_open_connections()`` releases handles before deleting a database file.


        :return: ``None``.
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
        Close all known connections while retaining the driver object for reuse.

        Example:
            ``driver.close(); driver.reopen()`` replaces the primary connection.


        :return: ``None``.
        """
        self._close_all_open_connections()

    def refresh(self, reconnect: bool = False):
        """
        Invalidate cached state while preserving a usable primary connection.

        Probe failures or an explicit reconnect replace the primary handle; retaining it preserves connection-local TEMP objects.

        Example:
            ``driver.refresh(reconnect=True)`` closes the old primary handle before opening another.


        :param reconnect: Force a new primary connection even when the current one responds to a liveness probe.
        :return: ``None``.
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
        Replace the primary connection, suppressing errors from closing the old handle.

        Opening failures propagate; caches are not refreshed here.

        Example:
            ``driver.reopen()`` acquires a fresh primary handle after ``close()``.


        :return: ``None``.
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
        Copy the database file using the local backup helper inside a connection context.

        A falsy helper result raises DatabaseDriverError. This is a file-copy backup, and this method does not explicitly close the acquired connection.

        Example:
            ``driver.direct_backup(path=backup_path)`` selects a destination for the copy.


        :param path: Optional backup destination; ``None`` lets the file helper choose one.
        :return: The truthy backup result, normally the destination path.
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
                return backup_status
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
        Close known handles and remove the configured database file.

        Access checks and removal failures raise DatabaseDriverError. Windows permission failures receive bounded retries; successful deletion invalidates caches.

        Example:
            ``driver.direct_self_delete()`` removes a disposable test database.


        :return: ``None``.
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
        Print the counter at multiples of one hundred million, then increment it.

        Example:
            With ``event_count = 0``, the first call prints zero and advances the counter to one.


        :return: ``None``.
        """
        if self.event_count % 100000000 == 0:
            LiuXin_print(self.event_count)
            self.event_count += 1
        else:
            self.event_count += 1

    def direct_identify_table_from_row(self, row_dict: dict[str, Any]) -> Optional[str]:
        """
        Find the first table whose columns contain every supplied row key.

        Remove a ``table`` key from the input. An empty remaining mapping returns False; no match raises DatabaseIntegrityError with partial-match diagnostics. Values do not participate in matching.

        Example:
            ``driver.direct_identify_table_from_row({"book_id": 3})`` matches by column names.


        :param row_dict: Column-to-value mapping; table inference removes any ``table`` key in place.
        :return: The first matching table name, or ``False`` for an empty mapping.
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
        Infer the row table and extract its ID column when present.

        Table inference can mutate the input and raise; an absent ID is the only explicit False result.

        Example:
            ``driver.get_id_from_row_dict({"book_id": 3})`` returns 3 on a matching schema.


        :param row_dict: Column-to-value mapping; table inference removes any ``table`` key in place.
        :return: The stored ID value, or ``False`` when that column is absent.
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
        Refresh metadata and force the combined table/column cache to be rebuilt.

        Example:
            ``driver.call_after_table_changes()`` follows direct schema changes.


        :return: ``None``.
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
        Create parent directories and build the schema at the configured path.

        The backend builder supplies the schema. Language seeding/locking is best effort; the connection commits and closes on success, with no finally cleanup on failure.

        Example:
            ``driver.direct_create_new_database()`` initializes a new database path.


        :return: ``None``.
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
    """
    Delegate schema creation on a supplied connection to the FRBR builder.

    Example:
        ``_create_new_database(conn)`` initializes an empty SQLite connection.


    :param conn: Open connection supplied by the caller.
    :return: The FRBR builder result, currently ``None``.
    """
    from LiuXin_alpha.databases.database_driver_plugins.SQL.database_generator_frbr.database_generator import (
        create_new_database,
    )

    return create_new_database(conn)
