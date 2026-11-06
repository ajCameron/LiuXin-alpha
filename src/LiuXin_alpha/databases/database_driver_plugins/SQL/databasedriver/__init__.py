"""
Share SQL driver lifecycle, schema-cache invalidation and row identification helpers.
"""

from __future__ import print_function, annotations

import pathlib
import os
from os import PathLike
import time
import gc
import pprint
from collections.abc import Callable, Mapping
from contextlib import closing
from threading import RLock

from typing import TYPE_CHECKING, Any, Literal, TypeVar, cast

from LiuXin_alpha.databases.db_types.driver_types import RowSQLConnection, SQLConnection

from LiuXin_alpha.utils.storage.local.file_backup import make_backup_path

from LiuXin_alpha.utils.logging import default_log
from LiuXin_alpha.errors import (
    DatabaseDriverError,
    DatabaseIntegrityError,
    InputIntegrityError,
)

from LiuXin_alpha.utils.paths import path_ok

from LiuXin_alpha.utils.logging import LiuXin_print, LiuXin_debug_print

from LiuXin_alpha.preferences import preferences

from LiuXin_alpha.constants import VERBOSE_DEBUG

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.portable_macros_api import DatabaseMacrosAPI
    from LiuXin_alpha.databases.api.driver_api import DatabaseDriverAPI
    from LiuXin_alpha.databases.maintenance.runtime import MaintenanceRuntime


from LiuXin_alpha.databases.database_driver_plugins.SQL.databasedriver._state import (
    SQLDriverMixinState,
    driver_operations,
)


ConnectionT = TypeVar("ConnectionT", bound=SQLConnection)


class SQLBaseDriver(SQLDriverMixinState):
    """
    Provide lifecycle helpers for concrete drivers that supply connections and schema
    methods.

    The host initializes connection tracking, cached schema attributes and the database
    wrapper.

    Example:
        A concrete SQLite driver inherits ``SQLBaseDriver`` and supplies
        ``get_connection()``.
    """

    _macros: DatabaseMacrosAPI
    database_path: str
    db_metadata: dict[str, Any]
    _open_connections: list[SQLConnection]
    event_count: int
    _create_new_database: Callable[[SQLConnection], None]

    @property
    def macros(self) -> "DatabaseMacrosAPI":
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

    def zero_prop_cache(self) -> None:
        """
        Invoke the internal cache invalidation and metadata refresh routine.

        Example:
            ``driver.zero_prop_cache()`` refreshes metadata after direct database work.


        :return: ``None``.
        """
        self._zero_prop_cache()

    def _zero_prop_cache(self) -> None:
        """
        Invalidate schema caches when the schema version changed or cannot be checked.

        Locations are always invalidated. Wrapper-derived cache clearing and metadata
        refresh are best effort; their exceptions are suppressed.

        Example:
            ``driver._zero_prop_cache()`` retains schema caches if the cached schema
            version still matches.


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
            if self.db is not None:
                self.db.refresh_db_metadata()
        except Exception:
            pass

    def _register_open_connection(self, conn: ConnectionT) -> ConnectionT:
        """
        Track a connection for later bulk closing and return it unchanged.

        Built-in connections deregister when closed. Foreign raw connections
        without callback attributes retain close-at-driver-shutdown ownership.

        Example:
            ``conn = driver._register_open_connection(conn)`` records an opened handle.


        :param conn: Open connection supplied by the caller.
        :return: The supplied connection.
        """
        runtime: MaintenanceRuntime | None = vars(self).get("_maintenance_runtime")
        if runtime is not None:
            if runtime.in_handler:
                conn.close()
                raise RuntimeError(
                    "Maintenance handlers must use their supplied connection"
                )
            runtime.bind(conn)
        elif hasattr(conn, "create_function"):
            # This optional backend capability is resolved at the connection boundary.
            register_function: Callable[[str, int, Callable[..., str | int]], None] = (
                getattr(conn, "create_function")
            )
            # Persistent capture remains safe when optional maintenance code is absent.
            register_function("LIUXIN_MAINTENANCE_READY", 2, lambda *_: 0)
            register_function("LIUXIN_MAINTENANCE_ORIGIN", 0, lambda: "")
            register_function("LIUXIN_MAINTENANCE_ANCESTRY", 0, lambda: "[]")
        registry = self._open_connections
        lock: RLock = vars(self).setdefault("_connection_tracking_lock", RLock())
        with lock:
            if not any(existing is conn for existing in registry):
                registry.append(conn)
        try:
            attributes = vars(conn)
        except TypeError:
            return conn

        def forget(closed: SQLConnection) -> None:
            """Remove by identity without retaining the closed connection itself."""
            with lock:
                for index, existing in enumerate(registry):
                    if existing is closed:
                        del registry[index]
                        break

        attributes["_liuxin_on_close"] = forget
        return conn

    def _read_row_for_update(
        self,
        connection: RowSQLConnection,
        table: str,
        id_column: str,
        row_id: Any,
        *,
        lock_row: bool = False,
    ) -> dict[str, Any] | None:
        """Read omitted display values on the connection that owns the update.

        :param connection: Active write connection; this helper never closes it.
        :param table: Inferred table whose headings come from the driver.
        :param id_column: Validated identity column for the target row.
        :param row_id: Bound identity value.
        :param lock_row: Request FOR UPDATE on backends supporting row locks.
        :return: Raw stored values, or None when the row no longer exists.
        """

        def quote(name: str) -> str:
            """Quote a schema identifier without interpreting its contents."""
            return '"' + str(name).replace('"', '""') + '"'

        columns = tuple(driver_operations(self).direct_get_column_headings(table))
        qualify = getattr(self, "_table_sql", None)
        table_sql = qualify(table) if callable(qualify) else quote(table)
        lock_sql = " FOR UPDATE" if lock_row else ""
        row = connection.execute(
            f"SELECT {', '.join(quote(column) for column in columns)} "
            f"FROM {table_sql} WHERE {quote(id_column)} = ?{lock_sql}",
            (row_id,),
        ).fetchone()
        if row is None:
            return None
        if isinstance(row, Mapping):
            return dict(row)
        return dict(zip(columns, row))

    def _close_all_open_connections(self) -> None:
        """Close owned handles, retaining failed closes for a subsequent attempt.

        Closing never commits. Built-in handles remove themselves from tracking;
        foreign raw handles are removed after successful close. A failed close
        remains owned even though the primary reference is detached.
        """
        connections = list(self._open_connections)
        primary = self.conn
        if primary is not None and not any(conn is primary for conn in connections):
            connections.append(primary)
        failed = []
        for connection in connections:
            try:
                connection.close()
            except Exception:
                failed.append(connection)
        self._open_connections[:] = failed
        self.conn = None

    def close(self) -> None:
        """
        Close all known connections while retaining the driver object for reuse.

        Example:
            ``driver.close(); driver.reopen()`` replaces the primary connection.


        :return: ``None``.
        """
        self._close_all_open_connections()

    def refresh(self, reconnect: bool = False) -> None:
        """
        Invalidate cached state while preserving a usable primary connection.

        Probe failures propagate without replacing or completing the primary handle.
        Only an explicit reconnect discards it and its connection-local TEMP objects.

        Example:
            ``driver.refresh(reconnect=True)`` closes the old primary handle before
            opening another.


        :param reconnect: Force a new primary connection even when the current one
            responds to a liveness probe.
        :return: ``None``.
        """
        conn = getattr(self, "conn", None)

        needs_reconnect = reconnect or conn is None
        if not needs_reconnect and conn is not None:
            with closing(conn.execute("SELECT 1")):
                pass

        if needs_reconnect:
            old = getattr(self, "conn", None)
            if old is not None:
                try:
                    old.close()
                except Exception:
                    pass
            self.conn = driver_operations(self).get_connection()

        self._zero_prop_cache()

    def reopen(self) -> None:
        """
        Replace the primary connection, suppressing errors from closing the old handle.

        Opening failures propagate; caches are not refreshed here.

        Example:
            ``driver.reopen()`` acquires a fresh primary handle after ``close()``.


        :return: ``None``.
        """
        old = getattr(self, "conn", None)
        if old is not None:
            try:
                old.close()
            except Exception:
                pass
        self.conn = driver_operations(self).get_connection()

    def direct_backup(
        self, path: str | PathLike[str] | None = None
    ) -> str | PathLike[str]:
        """Publish a verified, create-only SQLite backup of committed database state.

        Include WAL data through SQLite's backup API. A separate reader owns the
        snapshot; the primary connection and its pending writes are untouched.
        Existing destinations and source/sidecar destinations are refused.
        """
        from LiuXin_alpha.databases.database_driver_plugins.SQL.databasedriver.sqlite_files import (
            backup_sqlite_file,
        )

        destination = make_backup_path(self.database_path) if path is None else path
        backup_sqlite_file(self.database_path, destination)
        return destination

    def direct_self_delete(self) -> None:
        """
        Close known handles and remove the configured database file.

        Access checks and removal failures raise DatabaseDriverError. Windows permission
        failures receive bounded retries; successful deletion invalidates caches.

        Example:
            ``driver.direct_self_delete()`` removes a disposable test database.


        :return: ``None``.
        """

        # Ensure we release all file handles held by this driver (especially important on Windows).

        self._close_all_open_connections()

        # Check that the file can be accessed and the process has the privileges to run the delete

        if not path_ok(self.database_path):
            err_str = "DatabasePing file cannot be accessed for delete.\n"
            err_str += "database_file_path: {}\n".format(self.database_path)
            default_log.error(err_str)
            raise DatabaseDriverError(err_str)

        # Remove the database file (retry a couple of times on Windows to allow handle release).
        attempts = 6 if os.name == "nt" else 1

        last_err: OSError | None = None

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
                if os.name == "nt" and i < attempts - 1:
                    gc.collect()
                    time.sleep(0.05 * (i + 1))
                    continue
            except OSError as e:
                last_err = e
                break

        if last_err is not None:
            err_str = "DatabasePing cannot be deleted.\n"
            err_str += "database_path: {}\n".format(self.database_path)
            err_str += "error: {}\n".format(last_err)
            raise DatabaseDriverError(err_str)

        # Check that the delete has gone through i.e. the path no longer exists.
        if os.path.exists(self.database_path):
            err_str = "DatabasePing cannot be deleted - process failed silently.\n"
            err_str += "database_path: {}\n".format(self.database_path)
            raise DatabaseDriverError(err_str)

        # With the database gone the caches should also be emptied
        self._zero_prop_cache()

    def simple_print_progress_handler(self) -> None:
        """
        Print the counter at multiples of one hundred million, then increment it.

        SQLite drivers retain this observer for explicit registration through
        set_progress_callbacks; it is not installed by default. The counter counts
        callback invocations, matching VM instructions only at interval one.

        Example:
            With ``event_count = 0``, the first call prints zero and advances the
            counter to one.


        :return: ``None``.
        """
        if self.event_count % 100000000 == 0:
            LiuXin_print(str(self.event_count))
            self.event_count += 1
        else:
            self.event_count += 1

    def direct_identify_table_from_row(
        self, row_dict: dict[str, Any]
    ) -> str | Literal[False]:
        """
        Find the first table whose columns contain every supplied row key.

        Remove a ``table`` key from the input. An empty remaining mapping returns False;
        no match raises DatabaseIntegrityError with partial-match diagnostics. Values do
        not participate in matching.

        Example:
            ``driver.direct_identify_table_from_row({"book_id": 3})`` matches by column
            names.


        :param row_dict: Column-to-value mapping; table inference removes any ``table``
            key in place.
        :return: The first matching table name, or ``False`` for an empty mapping.
        """
        if "table" in row_dict.keys():
            del row_dict["table"]

        tables_and_columns = driver_operations(self).direct_get_tables_and_columns()
        table_names = tables_and_columns.keys()
        row_columns_set = set(key for key in row_dict.keys())

        if VERBOSE_DEBUG:
            err_str = "Calling direct_identify_table_from_row.\n"
            err_str += "Table_and_columns: " + repr(tables_and_columns) + "\n"
            err_str += "Tables: " + repr(table_names) + "\n"
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
                column_table = driver_operations(
                    self
                ).direct_identify_table_from_column(column_heading)
                partial_match_tables.add(column_table)
            except InputIntegrityError:
                unmatched_columns.add(column_heading)

        err_str = "SQLite:databasedriver:direct_identify_table_from_row unable to find matching table.\n"
        if len(partial_match_tables) > 0:
            err_str += "partial matches found for some column_headings.\n"
            err_str += (
                "partial_match_tables: " + pprint.pformat(partial_match_tables) + "\n"
            )

        if len(unmatched_columns) > 0:
            err_str += "some column_headings could not be matched.\n"
            err_str += "unmatched_columns: " + pprint.pformat(unmatched_columns) + "\n"
        err_str += "row_dict: " + pprint.pformat(row_dict) + "\n"

        if preferences.parse(
            "include_full_rep_if_row_cant_be_identified",
            rtn_value_type="bool",
            default=False,
        ):
            err_str += (
                "tables_and_columns: " + pprint.pformat(tables_and_columns) + "\n"
            )
        default_log.error(err_str)
        raise DatabaseIntegrityError(err_str)

    def get_id_from_row_dict(self, row_dict: dict[str, Any]) -> Any:
        """
        Infer the row table and extract its ID column when present.

        Table inference can mutate the input and raise; an absent ID is the only
        explicit False result.

        Example:
            ``driver.get_id_from_row_dict({"book_id": 3})`` returns 3 on a matching
            schema.


        :param row_dict: Column-to-value mapping; table inference removes any ``table``
            key in place.
        :return: The stored ID value, or ``False`` when that column is absent.
        """
        row_table = self.direct_identify_table_from_row(row_dict)
        if row_table is False:
            raise InputIntegrityError(
                "Cannot extract an ID without an identified table"
            )
        row_id_column = driver_operations(self).direct_get_id_column(row_table)

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
        """Build privately and publish to an absent path, rejecting existing files.

        Construct the driver with ``set_conn=False`` so it does not create the target
        before this operation. Preparation closes its connections on every exit.
        """
        from LiuXin_alpha.databases.creation import create_sqlite_file

        create_sqlite_file(self.database_path, self._build_database_file)

    def direct_replace_database(
        self, *, backup_path: str | os.PathLike[str]
    ) -> pathlib.Path:
        """Replace an offline database with a required, create-only recovery backup.

        All users of the target must be stopped. This unconnected driver owns only
        preparation resources; it does not close or invalidate borrowed connections.
        """
        from LiuXin_alpha.databases.creation import replace_sqlite_file

        if getattr(self, "conn", None) is not None or self._open_connections:
            raise RuntimeError(
                "Close database connections before explicit replacement."
            )
        return replace_sqlite_file(
            self.database_path, backup_path=backup_path, build=self._build_database_file
        )

    def _build_database_file(self, path: pathlib.Path) -> None:
        """
        Build and check a candidate using a separate driver with owned connections.
        """
        metadata = {**self.db_metadata, "database_path": str(path)}
        # Backends supply the canonical constructor; this partial lifecycle
        # mixin deliberately has no constructor of its own.
        driver_type = cast("type[DatabaseDriverAPI]", type(self))
        builder = driver_type(metadata, db=None, set_conn=False)
        try:
            conn = builder.get_connection()
            self._create_new_database(conn)
            from LiuXin_alpha.utils.language_tools import (
                ensure_languages_seeded_and_locked,
            )

            ensure_languages_seeded_and_locked(conn)
            conn.commit()
            if conn.execute("PRAGMA integrity_check").fetchone() != ("ok",):
                raise DatabaseIntegrityError("New database failed its integrity check.")
        finally:
            builder.close()

    #
    # ----------------------------------------------------------------------------------------------------------------------


def _create_new_database(conn: SQLConnection) -> None:
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
