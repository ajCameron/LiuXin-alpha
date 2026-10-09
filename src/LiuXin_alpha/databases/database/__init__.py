
"""
Compose the live database facade from drivers, row helpers and runtime services.

Database selects a backend, attaches its wrapper/macros, classifies schema metadata and optionally starts maintenance and storage. Constructors and helper methods can write to the database. Its context manager performs cleanup rather than defining a rollback transaction; concurrent callers must coordinate access according to the selected driver and operation.
"""

from __future__ import annotations, unicode_literals

import os
import pprint
import re
from copy import deepcopy
from typing import TYPE_CHECKING, Optional
from urllib.parse import urlsplit

from LiuXin_alpha.constants.paths import LiuXin_default_database
from LiuXin_alpha.databases.api import (
    DatabaseAPI,
    DatabaseDriverAPI,
    DatabaseDriverWrapperAPI,
)
from LiuXin_alpha.databases.database.constants import (
    HELPER_TABLES,
    OPTIONAL_HELPER_TABLES,
)
from LiuXin_alpha.databases.database.custom_columns_mixin import (
    CustomColumnDatabaseMixin,
)
from LiuXin_alpha.databases.database.dirtied_mixin import (
    DatabaseDirtiedRecordsMixin,
    DatabaseWriteTelemetry,
    ObservedDirtyRecordsQueue,
    TelemetryMaintainerProxy,
)
from LiuXin_alpha.databases.database.interlink_mixin import DatabaseInterlinkRowsMixin
from LiuXin_alpha.databases.database.intralink_mixin import DatabaseIntralinkRowsMixin
from LiuXin_alpha.databases.database.linked_rows_mixin import DatabaseLinkedRowsMixin
from LiuXin_alpha.databases.database.metadata_mixin import DatabaseMetadataMixin
from LiuXin_alpha.databases.database.null_rows_mixin import DatabaseNullRowsMixin
from LiuXin_alpha.databases.database.rating_mixin import DatabaseRatingMixin
from LiuXin_alpha.databases.database.search_mixin import DatabaseSearchMixin
from LiuXin_alpha.databases.database.tree_mixin import DatabaseTreeMixin
from LiuXin_alpha.databases.database_driver_plugins.registry import load_database_driver
from LiuXin_alpha.databases.driver_wrapper import DriverWrapper
from LiuXin_alpha.databases.maintenance import Maintainer
from LiuXin_alpha.databases.metadata_sql import MetadataSQL
from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.errors import DatabaseIntegrityError, InputIntegrityError
from LiuXin_alpha.preferences import preferences

# Py2/Py3 compatibility layer
from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode
from LiuXin_alpha.utils.logging import default_log

if TYPE_CHECKING:
    from LiuXin_alpha.storage.durable_manager import StorageBootstrapReport


# Todo: Embed this version number in the database - so that we can check the version of the code used to produce each
#       test database
__object_version__ = (1, 2, 0)

# Todo: Point uuid requests to the library_id instead


class _NoopMaintainerCallback:
    """
    Accept and discard driver maintenance events when the service is disabled.

    Each method returns None without inspecting inputs, queuing work or invoking telemetry. This replaces the driver callback only; it does not disable every other write or dirty-record path on Database.

    Example:
        >>> sink = _NoopMaintainerCallback()
        >>> sink.dirty_record("works", 1)
    """

    def dirty_record(self, table, row_id):  # noqa: ANN001 - driver callback compatibility
        """
        Discard a dirty-row callback without examining its table or ID.

        Example:
            >>> _NoopMaintainerCallback().dirty_record("works", 1)


        :param table: Ignored table name.
        :param row_id: Ignored row ID.
        :return: None.
        """

        pass

    def new_dirty_record(self, table, row_id):  # noqa: ANN001 - driver callback compatibility
        """
        Discard a newly dirty-row callback.

        Example:
            >>> _NoopMaintainerCallback().new_dirty_record("works", 1)


        :param table: Ignored table name.
        :param row_id: Ignored row ID.
        :return: None.
        """

        pass

    def dirty_interlink_record(self, update_type, table1, table2, table1_id, table2_id):  # noqa: ANN001
        """
        Discard a relationship-change callback for two endpoint rows.

        Example:
            >>> _NoopMaintainerCallback().dirty_interlink_record("add", "works", "agents", 1, 2)


        :param update_type: Ignored update classification.
        :param table1: Ignored first table name.
        :param table2: Ignored second table name.
        :param table1_id: Ignored first endpoint ID.
        :param table2_id: Ignored second endpoint ID.
        :return: None.
        """

        pass


def _metadata_uses_server_database(metadata, db_type: str) -> bool:
    """
    Recognize PostgreSQL-oriented metadata before applying file-creation rules.

    Backend aliases win even without metadata. Service keys are postgres_service and database_service; URL candidates are postgres_url, database_url, dsn, url and database_path. Generic service alone is ignored. URL parsing errors are skipped. This does not connect, validate credentials, choose a driver or recognize every possible server DSN; keyword-only PostgreSQL DSNs need another supported hint.

    Example:
        >>> _metadata_uses_server_database({"database_path": "postgresql://db.example/library"}, "SQLite")
        True
        >>> _metadata_uses_server_database({"database_path": "/tmp/library.db"}, "SQLite")
        False
        >>> _metadata_uses_server_database(None, " pg ")
        True


    :param metadata: Mapping of connection hints, or a false value for no hints.
    :param db_type: Backend name stringified, stripped and casefolded before checking postgres/postgresql/pg aliases.
    :return: True for a recognized backend alias, nonblank PostgreSQL service hint, or postgres/postgresql URL scheme; otherwise False.
    :raises AttributeError: Truthy metadata lacks a usable get method when hints must be inspected.
    """

    db_type_text = str(db_type or "").strip().casefold()
    if db_type_text in {"postgres", "postgresql", "pg"}:
        return True

    if not metadata:
        return False

    for key in ("postgres_service", "database_service"):
        value = metadata.get(key)
        text = str(value or "").strip()
        if text:
            return True

    for key in ("postgres_url", "database_url", "dsn", "url", "database_path"):
        value = metadata.get(key)
        text = str(value or "").strip()
        if not text:
            continue
        try:
            if urlsplit(text).scheme.casefold() in {"postgres", "postgresql"}:
                return True
        except ValueError:
            continue
    return False


class Database(
    CustomColumnDatabaseMixin,
    DatabaseRatingMixin,
    DatabaseNullRowsMixin,
    DatabaseMetadataMixin,
    DatabaseDirtiedRecordsMixin,
    DatabaseSearchMixin,
    DatabaseInterlinkRowsMixin,
    DatabaseIntralinkRowsMixin,
    DatabaseTreeMixin,
    DatabaseLinkedRowsMixin,
    DatabaseAPI,
):
    """
    Own a database driver, its row facade and optional runtime services.

    Construction may create a schema, repair sentinel rows, start maintenance and load Stores. Most row helpers delegate backend work through driver_wrapper. Use close or a context manager to release database handles; closing attempts to commit pending work, including after a context-body exception. Backend operations and construction are not one atomic transaction.

    Example:
        With metadata pointing at a disposable existing library, use with Database(metadata=metadata, enable_maintenance=False) as db: and perform reads inside the block. Disabling maintenance alone does not disable bootstrap writes.
    """

    _driver: DatabaseDriverAPI
    _driver_wrapper: DatabaseDriverWrapperAPI

    # Legacy convenience aliases (kept for backwards compatibility / test contracts):
    # - conn: primary driver connection
    # - lock: wrapper lock connection
    # - execute / executemany / executescript / get: direct-SQL escape hatches
    #   NOTE: these are meaningful only for SQL-like backends (SQLite/sqlite3/apsw, etc.).
    #   Non-SQL drivers may leave them unset.
    # These are *attributes* (not methods) so Database.close() can clear them to
    # help release handles on Windows.
    conn = None
    lock = None
    execute = None
    executemany = None
    executescript = None
    get = None
    shell = None
    get_connection = None
    storage = None
    storage_bootstrap_report = None

    # Todo: Split some of these out into factory methods and slim this down
    def __init__(
        self,
        metadata=None,
        db_type: str = "SQLite",
        create: bool = False,
        backup: bool = True,
        existing_driver: Optional[DatabaseDriverAPI] = None,
        enable_storage_manager: bool = True,
        strict_storage_manager_bootstrap: bool = False,
        storage_startup_on_add: bool = False,
        repair_bootstrap_rows: bool = True,
        enable_maintenance: bool = True,
    ) -> None:
        """
        Open or create a backend and attach the shared queues and runtime services.

        The existing-driver branch leaves facade metadata and type as None and assumes the database exists. The standard branch records metadata/type and checks backend existence. Storage initialization follows queue wiring; a failure can leave earlier resources initialized.

        Example:
            Given an existing SQLite library path, db = Database(metadata={"database_path": path}, repair_bootstrap_rows=False, enable_maintenance=False, enable_storage_manager=False) omits those optional bootstrap services; close db when finished.


        :param metadata: Connection metadata; None selects the configured default database when no driver is supplied.
        :param db_type: Backend registry name, default SQLite.
        :param create: Explicitly request schema creation; an existing file may be replaced.
        :param backup: Back up an existing path before explicit recreation when True.
        :param existing_driver: Already constructed driver; requires metadata=None.
        :param enable_storage_manager: Load configured Stores after database initialization when True.
        :param strict_storage_manager_bootstrap: Propagate storage loading failures and reject failed reports when True.
        :param storage_startup_on_add: Request Store startup while loading configurations when True.
        :param repair_bootstrap_rows: Allow rating and null-row repair writes during initialization.
        :param enable_maintenance: Start maintenance and attach its callback when True.
        :return: None; the instance owns the resulting database connections.
        :raises AssertionError: Both existing_driver and metadata are supplied.
        """
        self.metadata = None
        self.type = None

        self._macros = None
        self._metadata_sql = None
        self.write_telemetry = DatabaseWriteTelemetry()
        self.dirty_records_queue = ObservedDirtyRecordsQueue(telemetry=self.write_telemetry)
        self._maintainer_callback_proxy = None

        # Fundamental constants for this database
        if existing_driver is None:
            self.standard_init(
                metadata=metadata,
                db_type=db_type,
                create=create,
                backup=backup,
                repair_bootstrap_rows=repair_bootstrap_rows,
                enable_maintenance=enable_maintenance,
            )
        else:
            assert metadata is None, "driver is provided - it's assumed that the db metadata is contained within"
            self.existing_driver_init(
                existing_driver,
                repair_bootstrap_rows=repair_bootstrap_rows,
                enable_maintenance=enable_maintenance,
            )
        # Used as a lookup cache for if the link table in question has a priority column
        # Keyed with the table, value with True or False
        self._link_has_priority = dict()

        # Persistent helper table for metadata sidecar write-out (historic name: metadata_dirtied_books)
        self._metadata_dirtied_table = "metadata_dirtied_books"


        self.driver.dirty_records_queue = self.dirty_records_queue
        self.driver_wrapper.dirty_records_queue = self.dirty_records_queue

        self.storage = None
        self.storage_bootstrap_report = None
        if enable_storage_manager:
            self.bootstrap_storage_manager(
                startup_on_add=storage_startup_on_add,
                strict=strict_storage_manager_bootstrap,
            )

    @property
    def driver(self) -> DatabaseDriverAPI:
        """
        Return the currently bound backend driver.

        Example:
            For an open db, db.driver.conn accesses the primary backend connection when the driver exposes one.


        :return: Driver object, or None after cleanup.
        """
        return self._driver

    @property
    def driver_wrapper(self) -> DatabaseDriverWrapperAPI:
        """
        Return the wrapper that provides facade operations and the lock connection.

        Example:
            For an open SQL db, db.driver_wrapper.lock is the wrapper connection rather than the primary driver connection.


        :return: Current wrapper, or None after cleanup.
        """
        return self._driver_wrapper

    @property
    def macros(self):
        """
        Return the currently bound schema-specific SQL macros.

        Example:
            After construction, db.macros is the macros object taken from the bound driver.


        :return: Macros collaborator, or None before binding or after cleanup.
        """
        return self._macros

    def set_macros(self, new_macros) -> None:
        """
        Replace the facade macros reference after rejecting None.

        This does not update the driver, wrapper or collaborator back-references.

        Example:
            When assembling a compatible facade, db.set_macros(macros) stores that object without rebuilding other collaborators.


        :param new_macros: Macros object to retain directly.
        :return: None.
        :raises AssertionError: new_macros is None.
        """
        assert new_macros is not None, "Need to set macros to something that exists"
        self._macros = new_macros

    @property
    def metadata_sql(self):
        """
        Return the helper used for metadata-aware SQL operations.

        Example:
            For an initialized db, db.metadata_sql.db refers back to the owning facade.


        :return: Current MetadataSQL collaborator, or None before binding or after cleanup.
        """
        return self._metadata_sql

    def set_metadata_sql(self, new_metadata_sql) -> None:
        """
        Replace the metadata SQL reference after rejecting None.

        No back-reference or other collaborator is updated here.

        Example:
            During custom assembly, db.set_metadata_sql(helper) retains helper; the caller must arrange its db binding.


        :param new_metadata_sql: Metadata SQL helper to retain directly.
        :return: None.
        :raises AssertionError: new_metadata_sql is None.
        """
        assert new_metadata_sql is not None, "Need to set metadata_sql to something that exists"
        self._metadata_sql = new_metadata_sql

    def existing_driver_init(
        self,
        existing_driver: DatabaseDriverAPI,
        *,
        repair_bootstrap_rows: bool = True,
        enable_maintenance: bool = True,
    ) -> None:
        """
        Bind an existing driver, classify its schema and initialize runtime helpers.

        This internal constructor path assumes existence, refreshes metadata and shares table categories before optional repairs. It does not copy the driver metadata or backend type to the facade. Constructor telemetry and queue setup must already exist.

        Example:
            Database(existing_driver=driver, enable_storage_manager=False) invokes this path after setting up the facade state.


        :param existing_driver: Constructed driver with a compatible existing schema.
        :param repair_bootstrap_rows: Run rating/null-row repair when True.
        :param enable_maintenance: Create the maintenance service when True.
        :return: None; facade and wrapper schema state is populated.
        """
        # Load the driver constructor - use this to make the driver instance for this database
        self.set_driver(existing_driver)
        self.set_macros(existing_driver.macros)

        # Load the backend with the driver.
        self.lock = self.driver_wrapper.lock

        # Check to see if the database currently exists
        self._exists = True

        # categorized tables - sets of the names of each table in each category
        # all_tables - The names of every table known to the database

        # main_tables - the basic unit - titles, creators, series e.t.c - visible to the GUI and store the book
        #               metadata

        # custom_tables - Tables created by the user
        # custom_column_tables - Tables created to hold custom column data

        # interlink_tables - tables used to link the main tables together - what creators are associated to a title
        # intralink_tables - tables used to link the main tables back to themselves

        # dirtiable_tables - tables which can be dirtied - i.e. the maintenance bot should be informed when changes
        #                  - are made to them

        # helper_tables - data is stored in the database - for convenience - but isn't book or asset metadata
        self.all_tables = None

        self._main_tables = None

        self.custom_tables = None

        self._interlink_tables = None
        self.intralink_tables = None

        self.dirtiable_tables = None

        self.helper_tables = HELPER_TABLES

        self.refresh_db_metadata()

        self.driver_wrapper.all_tables = self.all_tables
        self.driver_wrapper.main_tables = self.main_tables
        self.driver_wrapper.interlink_tables = self.interlink_tables
        self.driver_wrapper.intralink_tables = self.intralink_tables
        self.driver_wrapper.helper_tables = self.helper_tables
        self.driver_wrapper.dirtiable_tables = self.dirtiable_tables

        # The rating table should be in a particular form - check that it is
        if repair_bootstrap_rows:
            self.check_rating_table()
            self.ensure_null_rows()

        self._initialise_runtime_collaborators(enable_maintenance=enable_maintenance)

    def standard_init(
        self,
        metadata=None,
        db_type="SQLite",
        create=False,
        backup=True,
        repair_bootstrap_rows: bool = True,
        enable_maintenance: bool = True,
    ):
        """
        Select a backend and create its schema when explicit or required for a new file.

        Capture path existence before loading the driver. Server metadata bypasses file-based automatic creation; explicit create still requests schema creation. Recreating an existing file can back it up and delete it; a new path skips those steps. Reload the driver after creation. Table metadata and repairs require a positive existence check, but runtime wiring is attempted regardless.

        Example:
            The constructor calls this method for Database(metadata={"database_path": new_path}); a missing file path triggers creation even with create=False.


        :param metadata: Connection hints, defaulting to the configured library path.
        :param db_type: Backend registry name.
        :param create: Force schema creation even when the backend already exists.
        :param backup: Back up an existing file before recreation.
        :param repair_bootstrap_rows: Repair rating and null rows after schema discovery.
        :param enable_maintenance: Enable the background maintenance service.
        :return: None; binds the driver and runtime collaborators.
        """
        if metadata is None:
            metadata = {"database_path": LiuXin_default_database}

        db_path = metadata.get("database_path")
        server_database = _metadata_uses_server_database(metadata, db_type)
        path_backed = not server_database
        path_existed = bool(db_path) and path_backed and db_path != ":memory:" and os.path.exists(db_path)

        self.metadata = metadata
        self.type = db_type
        self.set_driver(load_database_driver(db_type)(self.metadata, self))

        if create or (path_backed and not path_existed and db_path not in (None, ":memory:")):
            if path_existed:
                self.create_new_database(blank=True, backup=backup)
            else:
                self.create_new_database(blank=False, backup=False)

            # reload driver after schema creation
            self.set_driver(load_database_driver(db_type)(self.metadata, self))
            self.lock = self.driver_wrapper.lock

        # Check to see if the database currently exists
        self._exists = self.check_exists()

        if self._exists:
            # categorized tables - sets of the names of each table in each category
            # main_tables - the basic unit - titles, creators, series e.t.c - visible to the GUI and store the book
            #               metadata
            # interlink_tables - tables used to link the main tables together - what creators are associated to a title
            # intralink_tables - tables used to link the main tables back to themselves
            # dirtiable_tables - tables which can be dirtied - i.e. the maintenance bot should be informed when changes
            #                  - are made to them
            # helper_tables - data is stored in the database - for convenience - but isn't book or asset metadata
            self.all_tables = None
            self._main_tables = None
            self.custom_tables = None
            self._interlink_tables = None
            self.intralink_tables = None

            self.dirtiable_tables = None

            self.allowed_type_tables = None

            self.helper_tables = HELPER_TABLES


            # DatabasePing uuid - unique identifier given to the database
            self._uuid = None

            self.refresh_db_metadata()

            self.driver_wrapper.all_tables = self.all_tables
            self.driver_wrapper.main_tables = self.main_tables
            self.driver_wrapper.interlink_tables = self.interlink_tables
            self.driver_wrapper.intralink_tables = self.intralink_tables
            self.driver_wrapper.helper_tables = self.helper_tables
            self.driver_wrapper.dirtiable_tables = self.dirtiable_tables

            # The rating/null sentinel helpers can write to the database. Most
            # callers want that repair, but read-only probes should be able to
            # open an existing database without taking a write lock.
            if repair_bootstrap_rows:
                self.check_rating_table()
                self.ensure_null_rows()

        self._initialise_runtime_collaborators(enable_maintenance=enable_maintenance)

    def _initialise_runtime_collaborators(self, *, enable_maintenance: bool = True) -> None:
        """
        Bind maintenance callbacks, preferences and collaborator back-references.

        Assign the global preferences object directly. Wire db on the driver, wrapper, macros and metadata helper. Repeated initialization does not stop a previous maintainer and is not an atomic replacement.

        Example:
            Database(enable_maintenance=False, metadata=metadata) uses the no-op callback path while still binding its existing collaborators.


        :param enable_maintenance: Start a Maintainer and telemetry proxy when True; otherwise install no-op callbacks.
        :return: None.
        """
        if enable_maintenance:
            from LiuXin_alpha.databases.maintenance.service import Maintainer

            self.maintenance = Maintainer(self)
            self.maintainer = self.maintenance
            self._maintainer_callback_proxy = TelemetryMaintainerProxy(self.maintenance, self.write_telemetry)
            self.driver.maintainer_callback = self._maintainer_callback_proxy
            self.clean = self.maintenance.clean
        else:
            self.maintenance = None
            self.maintainer = None
            self._maintainer_callback_proxy = None
            self.driver.maintainer_callback = _NoopMaintainerCallback()
            self.clean = lambda *args, **kwargs: None

        # Global database preferences - just a copy of the main program preferences, but can be overridden if needed
        self.preferences = preferences

        # As this probably hasn't been done for the existing driver - load a reference to this database into the macros
        # and the driver - the two places that it should be needed
        # Todo: This should be handled by properties
        self.driver_wrapper.db = self
        self.driver.db = self
        self.macros.db = self
        self.metadata_sql.db = self

    def bootstrap_storage_manager(
        self,
        *,
        startup_on_add: bool = False,
        include_offline: bool = False,
        clear_existing: bool = True,
        strict: bool = False,
    ) -> StorageBootstrapReport:
        """
        Delegate Store loading and retain the resulting manager and report.

        The runtime helper creates a manager if needed or rebinds the existing one. Non-strict loading errors become failed reports, but manager construction errors propagate. Partial loading effects are not rolled back.

        Example:
            For an open db, report = db.bootstrap_storage_manager(startup_on_add=False) refreshes Store registrations; inspect report.issues for failures or skips.


        :param startup_on_add: Request backend startup during loading.
        :param include_offline: Include offline configurations in loading.
        :param clear_existing: Replace existing registered facades and remove absent configurations when True.
        :param strict: Propagate loading exceptions and reject reports with counted failures.
        :return: StorageBootstrapReport also retained on the facade.
        :raises StorageManagementError: Strict loading reports failures, or manager construction rejects configuration.
        """
        from LiuXin_alpha.databases.runtime import bootstrap_storage_manager

        return bootstrap_storage_manager(
            self,
            startup_on_add=startup_on_add,
            include_offline=include_offline,
            clear_existing=clear_existing,
            strict=strict,
        )


    def set_driver(self, new_driver: DatabaseDriverAPI) -> None:
        """
        Replace the driver and rebuild wrapper, macros and metadata SQL bindings.

        Close the old wrapper first. For a different driver, try committing its primary connection, roll back only if commit fails, then close it. Cleanup errors are suppressed. Install the new wrapper and convenience aliases, including storage aliases, which may become None. This does not rerun schema discovery, queue wiring or all runtime back-references; failures during new setup leave partial state.

        Example:
            For a compatible open replacement driver, db.set_driver(replacement) releases the previous database connections before constructing its new wrapper.


        :param new_driver: Driver to install; supplying the current driver retains its primary connection.
        :return: None.
        """
        # Close any existing wrapper (it holds its own SQLite connection for locking)
        old_wrapper = getattr(self, "_driver_wrapper", None)
        if old_wrapper is not None:
            try:
                old_wrapper.close()
            except Exception:
                pass

        # Close any existing driver connection
        old_driver = getattr(self, "_driver", None)
        if old_driver is not None and old_driver is not new_driver:
            try:
                conn = getattr(old_driver, "conn", None)
                if conn is not None:
                    try:
                        conn.commit()
                    except Exception:
                        try:
                            conn.rollback()
                        except Exception:
                            pass
                    try:
                        conn.close()
                    except Exception:
                        pass
            except Exception:
                pass
            try:
                old_driver.conn = None
            except Exception:
                pass

        self._driver = new_driver
        # The wrapper is coupled to the driver and provides a locking connection.
        self._driver_wrapper = DriverWrapper(self._driver)
        self.set_macros(self._driver.macros)
        self.set_metadata_sql(MetadataSQL(self))

        # Convenience lock handle
        try:
            self.lock = self._driver_wrapper.lock
        except Exception:
            pass

        # Legacy convenience aliases used across the codebase and relied upon by tests.
        # NOTE: these are intentionally attribute bindings to the wrapper/driver so they can
        # be cleared on close() to help break reference cycles and release SQLite file locks.
        try:
            self.conn = getattr(self._driver, "conn", None)
        except Exception:
            self.conn = None

        for name in (
            "execute",
            "executemany",
            "executescript",
            "get",
            "shell",
            "get_connection",
            "storage_bootstrap_report",
            "storage",
        ):
            try:
                setattr(self, name, getattr(self._driver_wrapper, name))
            except Exception:
                try:
                    setattr(self, name, None)
                except Exception:
                    pass

    def set_driver_wrapper(self, new_driver_wrapper: DatabaseDriverWrapperAPI) -> None:
        """
        Assign a wrapper and replace its macros reference with the facade macros.

        Assignment precedes validation. This neither closes the previous wrapper nor refreshes convenience aliases, locks or back-references.

        Example:
            During controlled assembly, db.set_driver_wrapper(wrapper) requires wrapper.macros already set before copying db.macros.


        :param new_driver_wrapper: Wrapper whose existing macros attribute must be non-None.
        :return: None.
        :raises AssertionError: The newly assigned wrapper has macros=None.
        """
        self._driver_wrapper = new_driver_wrapper
        assert self._driver_wrapper.macros is not None
        self._driver_wrapper.macros = self.macros

    def __del__(self):
        """
        Attempt close during finalization and suppress ordinary cleanup exceptions.

        Finalization timing is not deterministic; explicit close or a context manager is required for predictable resource release.

        Example:
            Prefer db.close() before discarding the last reference to an open database.


        :return: None.
        """
        try:
            self.close()
        except Exception:
            # Never raise from __del__
            pass

    def close(self) -> None:
        """
        Stop maintenance and release database connections with best-effort cleanup.

        Request maintenance stop and wait at most one second for its thread. Close the wrapper or fallback lock, then attempt to commit and close the primary connection; roll back only if that commit fails. Suppress ordinary cleanup errors. Repeated calls tolerate cleared state. This does not call StorageManager.close or the driver-wide close method, and cannot guarantee every reference cycle is removed.

        Example:
            Use db.close() in a finally block to release an open facade; pending primary-connection changes may be committed.


        :return: None; connection aliases and selected collaborator references are cleared.
        """
        # Capture references early so break_cycles() can't erase them before we close.
        driver = getattr(self, "_driver", None)
        wrapper = getattr(self, "_driver_wrapper", None)
        maintenance = getattr(self, "maintenance", None)

        # Stop the background maintenance thread (if it exists)
        try:
            maint_thread = getattr(maintenance, "maintainer", None)
            if maint_thread is not None and hasattr(maint_thread, "stop"):
                maint_thread.stop()
                # The thread is daemon=True, but joining briefly helps tests release resources promptly.
                try:
                    maint_thread.join(timeout=1)
                except Exception:
                    pass
        except Exception:
            pass

        # Close the wrapper's lock connection
        try:
            if wrapper is not None and hasattr(wrapper, "close"):
                wrapper.close()
            else:
                lock = getattr(self, "lock", None)
                if lock is not None:
                    try:
                        lock.commit()
                    except Exception:
                        pass
                    try:
                        lock.close()
                    except Exception:
                        pass
        except Exception:
            pass

        # Close the driver's primary connection
        try:
            if driver is not None:
                conn = getattr(driver, "conn", None)
                if conn is not None:
                    try:
                        conn.commit()
                    except Exception:
                        # If commit fails, try rollback then close
                        try:
                            conn.rollback()
                        except Exception:
                            pass
                    try:
                        conn.close()
                    except Exception:
                        pass
                try:
                    driver.conn = None
                except Exception:
                    pass
        except Exception:
            pass

        # Remove convenience aliases that can keep connections alive
        for attr in (
            "conn",
            "lock",
            "execute",
            "executemany",
            "executescript",
            "get",
            "shell",
            "get_connection",
        ):
            try:
                setattr(self, attr, None)
            except Exception:
                pass

        # Break reference cycles last
        try:
            self.break_cycles()
        except Exception:
            pass

    def __enter__(self):
        """
        Return this facade for use inside a cleanup context.

        No new transaction is opened.

        Example:
            For an open db, with db as active: binds active to db and closes it when the block exits.


        :return: This Database instance.
        """

        return self

    def __exit__(self, exc_type, exc, tb):
        """
        Close the facade and allow a context-body exception to propagate.

        Exception details are ignored by close; pending work may be committed even on an exceptional exit.

        Example:
            An exception raised inside with db: propagates after cleanup; use an explicit transaction API when rollback is required.


        :param exc_type: Exception class passed by the context protocol, or None.
        :param exc: Exception instance passed by the context protocol, or None.
        :param tb: Traceback passed by the context protocol, or None.
        :return: False, so exceptions are not suppressed.
        """

        self.close()
        return False

    # Todo: Might actually want to delete these objects - and this might be an internal method
    def break_cycles(self):
        """
        Clear selected runtime and connection references without closing resources.

        Each assignment is best effort, including on partially initialized instances. Storage, preferences and the clean alias are not cleared. Call close first when handles still need releasing; clearing references alone does not perform shutdown.

        Example:
            Normal db.close() invokes this after attempting to close its captured connections.


        :return: None.
        """
        # These may not exist if __init__ failed partway through.
        for attr in (
            "_driver_wrapper",
            "_driver",
            "_macros",
            "_metadata_sql",
            "maintenance",
            "maintainer",
            "write_telemetry",
            "_maintainer_callback_proxy",
            "dirty_records_queue",
            "_link_has_priority",
            # Convenience aliases (see set_driver)
            "conn",
            "lock",
            "execute",
            "executemany",
            "executescript",
            "get",
            "shell",
            "get_connection",
        ):
            try:
                setattr(self, attr, None)
            except Exception:
                pass

    @property
    def main_tables(self) -> frozenset[str]:
        """
        Snapshot the cached main-table category as an immutable set.

        Example:
            For an initialized db, "works" in db.main_tables checks its cached main-table classification.


        :return: New frozenset of main-table names.
        :raises TypeError: The underlying category is still None.
        """
        return frozenset(self._main_tables)

    @property
    def interlink_tables(self) -> frozenset[str]:
        """
        Snapshot the cached inter-table relationship category as an immutable set.

        Example:
            For an initialized db, sorted(db.interlink_tables) lists the cached relationship tables.


        :return: New frozenset of interlink-table names.
        :raises TypeError: The underlying category is still None.
        """
        return frozenset(self._interlink_tables)

    def check_exists(self):
        """
        Ask the bound driver whether its database exists.

        This does not update the cached _exists field or independently validate schema completeness.

        Example:
            For an open db, db.check_exists() delegates the existence check to its driver.


        :return: The driver exists result, whose meaning depends on the backend.
        """
        return self.driver.exists()

    # Todo: These methods also private?
    def refresh_db_metadata(self):
        """
        Rebuild cached table categories and validate required helper tables.

        Optional compatibility tables may be absent. Missing required helpers raise after category state has been reset, with close-name suggestions. Classification is name-based. A missing UUID requests set_uuid without re-reading it. sqlite_sequence is removed from main_tables after forming dirtiable_tables, so it can remain in that union. Wrapper category aliases are not refreshed by this method itself.

        Example:
            During initialization, db.refresh_db_metadata() classifies the discovered schema before its categories are copied onto the wrapper.


        :return: None; category sets, dirtiable_tables and the cached UUID are assigned.
        :raises DatabaseIntegrityError: Required non-optional helper tables are missing.
        """
        self.all_tables = set([t for t in self.get_tables()])
        self._main_tables = set()
        self.custom_tables = set()
        self._interlink_tables = set()
        self.intralink_tables = set()
        self.allowed_type_tables = set()
        # Check required helper tables exist. Compatibility catalogs are
        # optional so older databases can open and use their inferred read
        # fallbacks until explicitly migrated.
        missing_helpers = sorted(
            set(self.helper_tables)
            - set(OPTIONAL_HELPER_TABLES)
            - set(self.all_tables)
        )
        if missing_helpers:
            import difflib

            suggestions = {
                t: difflib.get_close_matches(t, sorted(self.all_tables), n=3, cutoff=0.6)
                for t in missing_helpers
            }

            err_str = "Unable to find required helper table(s) in the database"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("missing_helper_tables", pprint.pformat(missing_helpers)),
                ("suggestions", pprint.pformat(suggestions)),
                ("all_tables", pprint.pformat(self.all_tables)),
            )
            raise DatabaseIntegrityError(err_str)
        # Populate the individual categories
        for table in self.all_tables:
            table_cat = self.categorize_table(table)

            if table_cat == "main":
                self._main_tables.add(table)
                continue

            if table_cat == "interlink":
                self._interlink_tables.add(table)
                continue

            if table_cat == "intralink":
                self.intralink_tables.add(table)
                continue

            if table_cat == "helper":
                continue

            if table_cat == "custom":
                self.custom_tables.add(table)
                continue

            if table_cat == "allowed_types":
                self.allowed_type_tables.add(table)
                continue

            err_str = "This position should never be reached"
            err_str = default_log.log_variables(err_str, "ERROR", ("table_cat", table_cat), ("table", table))
            raise NotImplementedError(err_str)

        # The dirtiable tables are a union of the main tables and the two types of link table
        self.dirtiable_tables = deepcopy(self.main_tables.union(self.interlink_tables).union(self.intralink_tables))

        self._uuid = self.driver_wrapper.get_uuid()
        if six_unicode(self._uuid).lower().strip() == "none":
            self.driver_wrapper.set_uuid()

        # Remove SQLite sequence from the main tables - if present - this is for internal use only
        self._main_tables.discard("sqlite_sequence")

    # Todo: Backup is somewhat useless if there is no way to restore
    def backup(self):
        """
        Request a backend-specific backup of the current database.

        Example:
            For a configured open db, db.backup() invokes driver.direct_backup(); the backend chooses the backup destination and behavior.


        :return: None; any driver return value is discarded.
        """
        self.driver.direct_backup()

    # Todo: This is not, actually, write locking. This just makes a throwaway copy of the database
    def lock_writing(self):
        """
        Request a scratch copy through the driver make_scratch hook.

        This method does not acquire a write lock. The SQL base implementation copies the database file and changes its path attribute without reopening existing connections.

        Example:
            Call db.lock_writing() only when the selected backend scratch-copy behavior is suitable; it is not a transaction lock.


        :return: None; any scratch path returned by the driver is discarded.
        """
        self.driver.make_scratch()

    def create_new_database(self, blank: bool = True, backup: bool = True) -> None:
        """
        Optionally back up and delete the database before requesting schema creation.

        Operations run in backup, deletion, creation order. No rollback, driver reload or metadata refresh occurs here; backend failures propagate after any earlier side effects.

        Example:
            On a disposable configured backend, db.create_new_database(blank=False, backup=False) requests creation without first backing up or deleting it.


        :param blank: Delete the existing database through the driver before creation.
        :param backup: Request a backup before any deletion or creation.
        :return: None.
        """
        if backup:
            self.driver.direct_backup()

        if blank:
            self.driver.direct_self_delete()

        self.driver.direct_create_new_database()

    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - METHODS TO OUTPUT BASIC INFORMATION ABOUT THE DATABASE START HERE

    def __unicode__(self):
        """
        Render backend type and the complete metadata mapping as text.

        Example:
            >>> db = object.__new__(Database)
            >>> db.type, db.metadata = "SQLite", {"database_path": ":memory:"}
            >>> "SQLite" in db.__unicode__()
            True


        :return: Multiline string, including unredacted metadata.
        """
        rtn_str = "LiuXin DatabasePing - Type:{}\n".format(self.type)
        rtn_str += "metadata:\n"
        rtn_str += pprint.pformat(self.metadata) + "\n"
        return rtn_str

    def __str__(self):
        """
        Encode the legacy textual representation as UTF-8 bytes.

        Calling this method directly returns bytes; str(db) raises TypeError in Python 3.

        Example:
            >>> db = object.__new__(Database)
            >>> db.type, db.metadata = "SQLite", {}
            >>> isinstance(db.__str__(), bytes)
            True


        :return: Bytes, which do not satisfy the Python 3 __str__ protocol.
        """

        return self.__unicode__().encode("utf-8")

    def __repr__(self):
        """
        Format the facade type and metadata database_path in a short label.

        Existing-driver construction can leave metadata=None; service-only metadata may omit database_path.

        Example:
            >>> db = object.__new__(Database)
            >>> db.type, db.metadata = "SQLite", {"database_path": ":memory:"}
            >>> repr(db)
            '[ LX_database - type - SQLite at :memory: ]'


        :return: String containing the backend type and path.
        :raises KeyError: The metadata mapping lacks database_path.
        :raises TypeError: Metadata is not subscriptable, including None.
        """
        db_type = self.type
        db_path = self.metadata["database_path"]
        return "[ LX_database - type - " + six_unicode(db_type) + " at " + six_unicode(db_path) + " ]"

    # Todo: Might want to consider renaming this to full_repr, for consistency
    def full_rep(self):
        """
        Render cached identity, metadata and table categories for inspection.

        Metadata is unredacted. The legacy Helper_tables section repeats intralink_tables rather than rendering helper_tables.

        Example:
            For a fully initialized db, report = db.full_rep() captures its current category caches as text.


        :return: Multiline string; this method does not print it.
        """
        ans = list()
        ans.append("LiuXin_Database")
        ans.append("database_uuid: {}".format(self.uuid))

        ans.append("DatabasePing MetaData")
        ans.append(pprint.pformat(self.metadata, indent=2))
        ans.append("")

        ans.append("Main_tables")
        ans.append(pprint.pformat(self.main_tables, indent=2))
        ans.append("")

        ans.append("Interlink_tables")
        ans.append(pprint.pformat(self.interlink_tables, indent=2))
        ans.append("")

        ans.append("Intralink_tables")
        ans.append(pprint.pformat(self.intralink_tables, indent=2))
        ans.append("")

        ans.append("Helper_tables")
        ans.append(pprint.pformat(self.intralink_tables, indent=2))
        ans.append("")

        return "\n".join(ans)

    #
    # ----------------------------------------------------------------------------------------------------------------------
    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - METHODS TO DEAL WITH TABLE CATEGORIES START HERE

    def categorize_table(self, table_name):
        """
        Classify a known table name using ordered naming conventions.

        Precedence is allowed_types__ prefix, configured helper names, custom-column patterns, interlink pattern, intralink pattern, then main. Regex checks accept matching prefixes; classification does not inspect columns or foreign keys.

        Example:
            >>> db = object.__new__(Database)
            >>> db.all_tables, db.helper_tables = {"works", "allowed_types__works"}, set()
            >>> db.categorize_table("allowed_types__works")
            'allowed_types'
            >>> db.categorize_table("works")
            'main'


        :param table_name: Name converted to text and checked against all_tables.
        :return: One of allowed_types, helper, custom, interlink, intralink or main.
        :raises InputIntegrityError: The text name is absent from all_tables.
        """
        table_name = six_unicode(table_name)
        if table_name not in self.all_tables:
            err_str = "Error - categorize_table has been passed an invalid table name."
            err_str = default_log.log_variables(err_str, "ERROR", ("table_name", table_name))
            raise InputIntegrityError(err_str)

        if table_name.startswith("allowed_types__"):
            return "allowed_types"

        # Helper tables should have been specified before this function is first called
        if table_name in self.helper_tables:
            return "helper"

        cc_pattern = re.compile(r"[a-zA-Z0-9_]+_custom_column_[0-9]+_link")
        cc_match = cc_pattern.match(table_name)
        if table_name.startswith("custom_column_") or cc_match is not None:
            return "custom"

        interlink_pattern = re.compile(r"[\sa-zA-Z0-9_]+_links")
        interlink_match = interlink_pattern.match(table_name)
        if interlink_match is not None:
            return "interlink"

        intralink_pattern = re.compile(r"[\sa-zA-Z0-9_]+_intralink")
        intralink_match = intralink_pattern.match(table_name)
        if intralink_match is not None:
            return "intralink"

        return "main"

    #
    # ----------------------------------------------------------------------------------------------------------------------
    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - METHODS TO ADD TO THE DATABASE START HERE

    def dupe_row(self, row):
        """
        Insert a blank row, copy source fields and synchronize under the new ID.

        Restore the new ID after copying the source row_dict. If sync raises DatabaseIntegrityError, delete the new row and reraise; cleanup errors can mask that failure. Other errors do not trigger this cleanup. Unique source values may prevent duplication.

        Example:
            In a table whose copied values satisfy its constraints, duplicate = db.dupe_row(row) allocates a separate row ID.


        :param row: Row whose table and field values supply the duplicate.
        :return: New persisted Row with its allocated identity.
        :raises DatabaseIntegrityError: Synchronization violates a database constraint.
        """
        row_table = row.table
        row_table_id_col = self.driver_wrapper.get_id_column(row_table)

        new_row = self.get_blank_row(row_table)
        new_row_id = new_row.row_id

        # Store the old row id - replace the row dict in the new blank row - replace the row_id - sync
        new_row.row_dict = row.row_dict
        new_row[row_table_id_col] = new_row_id
        try:
            new_row.sync()
        except DatabaseIntegrityError:
            # Probably a violation of a unique constraint - abort and tidy up
            self.delete(new_row)
            raise

        return new_row

    #
    # ----------------------------------------------------------------------------------------------------------------------
    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - METHODS TO DELETE FROM THE DATABASE START HERE

    def delete(self, row):
        """
        Delete the supplied row identity through this facade wrapper.

        Ownership is not checked: deletion uses this database with the supplied table and ID. The passed Row object is not cleared.

        Example:
            For a Row belonging to db, db.delete(row) removes its persistent record through delete_by_id.


        :param row: Row-like object providing table and a non-None row_id.
        :return: None.
        :raises InputIntegrityError: The supplied row_id is None.
        """
        row_table = row.table
        row_id = row.row_id
        if row_id is None:
            err_str = "Unable to delete given row - row_id is not found."
            err_str = default_log.log_variables(err_str, "ERROR", ("row", row))
            raise InputIntegrityError(err_str)
        self.driver_wrapper.delete_by_id(target_table=row_table, row_id=row_id)

    #
    # ----------------------------------------------------------------------------------------------------------------------

    def get_blank_row(self, table):
        """
        Insert a backend blank record and wrap its generated fields in a Row.

        This performs a write. A Row-construction failure can occur after the backend has inserted the record.

        Example:
            On a writable database, row = db.get_blank_row("works") allocates a record before further field assignments.


        :param table: Target table name understood by the wrapper.
        :return: Row bound to this facade, carrying the inserted identity.
        """
        blank_row_dict = self.driver_wrapper.get_blank_row(table)
        return Row(database=self, row_dict=blank_row_dict)

    # ------------------------------------------------------------------------------------------------------------------
    # - TRIGGER HELPERS
    # ------------------------------------------------------------------------------------------------------------------

    def get_triggers(self):
        """
        Retrieve trigger names through the wrapper and selected driver.

        Example:
            For an open db, names = db.get_triggers() discovers triggers before selective removal.


        :return: Backend trigger-name list.
        """

        return self.driver_wrapper.get_triggers()

    def drop_triggers(self, triggers):
        """
        Delegate removal of the supplied trigger names.

        The backend determines validation and transaction behavior; this facade adds no rollback.

        Example:
            On a disposable database, db.drop_triggers(names) removes the selected names obtained from db.get_triggers().


        :param triggers: Trigger-name collection accepted by the backend.
        :return: Backend removal result; the shared SQL driver returns True on success.
        """

        return self.driver_wrapper.drop_triggers(triggers)

    def drop_all_triggers(self):
        """
        Discover current trigger names and delegate their removal.

        Example:
            On a disposable database, db.drop_all_triggers() removes every trigger discovered by the selected backend.


        :return: Backend removal result; the shared SQL driver returns True on success.
        """

        return self.driver_wrapper.drop_all_triggers()

    #
    # ----------------------------------------------------------------------------------------------------------------------
    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - SPECIALIZED UPDATE METHODS START HERE

    def update_columns(self, values_map, field=None, table=None):
        """
        Forward a bulk value mapping and optional column/table hints to the wrapper.

        Example:
            For a backend-supported values_map, db.update_columns(values_map, field=column, table=table) forwards those arguments unchanged.


        :param values_map: Value mapping in the format accepted by the selected driver.
        :param field: Optional target field hint.
        :param table: Optional target table hint.
        :return: None; the wrapper return value is discarded.
        """
        self.driver_wrapper.update_columns(values_map=values_map, field=field, table=table)

    #
    # ----------------------------------------------------------------------------------------------------------------------
    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - MAGIC METHODS START HERE

    def __eq__(self, other):
        """
        Compare facade metadata without comparing type, UUID or connections.

        No type guard is applied; metadata equality alone defines the result.

        Example:
            >>> from types import SimpleNamespace
            >>> db = object.__new__(Database)
            >>> db.metadata = {"database_path": ":memory:"}
            >>> db == SimpleNamespace(metadata={"database_path": ":memory:"})
            True


        :param other: Object exposing metadata for comparison.
        :return: True when the metadata values compare equal, otherwise False.
        :raises AttributeError: other has no metadata attribute.
        """
        if self.metadata == other.metadata:
            return True
        else:
            return False

    #  Todo: We need a names methods mixin

    def get_table_from_column(self, column_name: str) -> str:
        """
        Search an iterable of table/column pairs for the first containing table.

        The loop iterates get_tables_and_columns directly. The normal implementation returns a dictionary, so table-name keys are unpacked rather than key/value pairs; ordinary names raise ValueError before lookup. This legacy method does not currently adapt that mapping with items().

        Example:
            With the normal mapping-returning wrapper, use explicit iteration over db.get_tables_and_columns().items() when looking up a column; this method retains its legacy unpacking limitation.


        :param column_name: Column name to find in each yielded collection.
        :return: First matching table name if pair iteration succeeds.
        :raises ValueError: A yielded table-name key cannot unpack into two values.
        :raises InputIntegrityError: Iteration finishes without finding the column.
        """
        for tab, col_set in self.get_tables_and_columns():
            if column_name in col_set:
                return tab
        raise InputIntegrityError("The column is not in a table - as far as we can tell.")

#
# ----------------------------------------------------------------------------------------------------------------------
########################################################################################################################
########################################################################################################################
