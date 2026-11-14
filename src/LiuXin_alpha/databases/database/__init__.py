"""
Compose the live database facade from drivers, row helpers and runtime services.

Database selects a backend, attaches its wrapper/macros, classifies schema metadata and
optionally starts maintenance and storage. Constructors and helper methods can write to
the database. Those writes enforce backend representation and integrity, not general
metadata validity; supported metadata changes belong through Catalog. Its context
manager performs cleanup rather than defining a rollback transaction; concurrent callers
must coordinate access according to the selected driver and operation.
"""

from __future__ import annotations, unicode_literals

import os
import pprint
import pathlib
from contextlib import nullcontext
from copy import deepcopy
from collections.abc import Callable, Iterable, Mapping, Set
from types import TracebackType
from LiuXin_alpha.databases.db_types.driver_types import (
    QueryConnection,
    SQLConnection,
    SQLExecuteOperation,
    SQLExecuteManyOperation,
    SQLGetOperation,
)
from typing import TYPE_CHECKING, Optional, Any, Literal
from urllib.parse import urlsplit

from LiuXin_alpha.constants.paths import LiuXin_default_database
from LiuXin_alpha.databases.api import (
    DatabaseAPI,
    DatabaseDriverAPI,
    DatabaseDriverWrapperAPI,
    DatabaseMacrosAPI,
    MetadataSQLAPI,
)
from LiuXin_alpha.databases.database.constants import (
    HELPER_TABLES,
    OPTIONAL_HELPER_TABLES,
)
from LiuXin_alpha.databases.database._driver_binding import (
    close_driver,
    connection_metadata,
    same_catalogue,
    schema_bindings,
    table_category,
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
from LiuXin_alpha.configuration import current_preferences

# Py2/Py3 compatibility layer
from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode
from LiuXin_alpha.utils.logging import default_log

if TYPE_CHECKING:
    from LiuXin_alpha.storage.durable_manager import (
        StorageBootstrapReport,
        StorageManager,
    )
    from LiuXin_alpha.databases.api.row_api import RowAPI
    from LiuXin_alpha.databases.api.maintenance_api import MaintenanceCallbackSinkAPI
    from _thread import RLock


# Todo: Embed this version number in the database - so that we can check the version of the code used to produce each
#       test database
__object_version__ = (1, 2, 0)

# Todo: Point uuid requests to the library_id instead


class _NoopMaintainerCallback:
    """
    Accept and discard driver maintenance events when the service is disabled.

    Each method returns None without inspecting inputs, queuing work or invoking
    telemetry. This replaces the driver callback only; it does not disable every other
    write or dirty-record path on Database.

    Example:
        >>> sink = _NoopMaintainerCallback()
        >>> sink.dirty_record("works", 1)
    """

    def dirty_record(self, table: str, row_id: int) -> None:
        """
        Discard a dirty-row callback without examining its table or ID.

        Example:
            >>> _NoopMaintainerCallback().dirty_record("works", 1)


        :param table: Ignored table name.
        :param row_id: Ignored row ID.
        :return: None.
        """

        pass

    def new_dirty_record(self, table: str, row_id: int) -> None:
        """
        Discard a newly dirty-row callback.

        Example:
            >>> _NoopMaintainerCallback().new_dirty_record("works", 1)


        :param table: Ignored table name.
        :param row_id: Ignored row ID.
        :return: None.
        """

        pass

    def dirty_interlink_record(
        self, update_type: str, table1: str, table2: str, table1_id: int, table2_id: int
    ) -> None:
        """
        Discard a relationship-change callback for two endpoint rows.

        Example:
            >>> _NoopMaintainerCallback().dirty_interlink_record(
            ...     "add", "works", "agents", 1, 2
            ... )


        :param update_type: Ignored update classification.
        :param table1: Ignored first table name.
        :param table2: Ignored second table name.
        :param table1_id: Ignored first endpoint ID.
        :param table2_id: Ignored second endpoint ID.
        :return: None.
        """

        pass


def _metadata_uses_server_database(
    metadata: Mapping[str, Any] | None, db_type: str
) -> bool:
    """
    Recognize PostgreSQL-oriented metadata before applying file-creation rules.

    Backend aliases win even without metadata. Service keys are postgres_service and
    database_service; URL candidates are postgres_url, database_url, dsn, url and
    database_path. Generic service alone is ignored. URL parsing errors are skipped.
    This does not connect, validate credentials, choose a driver or recognize every
    possible server DSN; keyword-only PostgreSQL DSNs need another supported hint.

    Example:
        >>> _metadata_uses_server_database(
        ...     {"database_path": "postgresql://db.example/library"}, "SQLite"
        ... )
        True
        >>> _metadata_uses_server_database(
        ...     {"database_path": "/tmp/library.db"}, "SQLite"
        ... )
        False
        >>> _metadata_uses_server_database(None, " pg ")
        True


    :param metadata: Mapping of connection hints, or a false value for no hints.
    :param db_type: Backend name stringified, stripped and casefolded before checking
        postgres/postgresql/pg aliases.
    :return: True for a recognized backend alias, nonblank PostgreSQL service hint, or
        postgres/postgresql URL scheme; otherwise False.
    :raises AttributeError: Truthy metadata lacks a usable get method when hints must be
        inspected.
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

    Construction may create a schema, repair sentinel rows, start maintenance and load
    Stores. Most row helpers delegate backend work through driver_wrapper. Use close or
    a context manager to release database handles; cleanup rolls back pending work.
    Explicit semantic write units own their commits. Backend operations and construction
    are not one atomic transaction.

    Database is intentionally not a semantic metadata boundary. Direct row,
    macro, or driver writes may persist any value accepted by storage constraints.
    Use Catalog repositories or ``catalog.mutations.writer`` when metadata meaning
    and operation validity must be guaranteed.

    Example:
        With metadata pointing at a disposable existing library, use with
        Database(metadata=metadata, enable_maintenance=False) as db: and perform reads
        inside the block. Disabling maintenance alone does not disable bootstrap writes.
    """

    _driver: DatabaseDriverAPI | None = None
    _driver_wrapper: DatabaseDriverWrapperAPI | None = None
    _macros: DatabaseMacrosAPI | None = None
    dirtiable_tables: Set[str] | None
    helper_tables: Set[str]
    write_telemetry: DatabaseWriteTelemetry | None
    clean: Callable[[str, Iterable[int]], None]

    # Legacy convenience aliases (kept for backwards compatibility / test contracts):
    # - conn: primary driver connection
    # - lock: wrapper's reentrant mutex; it does not control SQL transactions
    # - execute / executemany / executescript / get: direct-SQL escape hatches
    #   NOTE: these are meaningful only for SQL-like backends (SQLite/sqlite3/apsw, etc.).
    #   Non-SQL drivers may leave them unset.
    # These are *attributes* (not methods) so Database.close() can clear them to
    # help release handles on Windows.
    conn: SQLConnection | None = None
    lock: RLock | None = None
    execute: SQLExecuteOperation | None = None
    executemany: SQLExecuteManyOperation | None = None
    executescript: Callable[[str], None] | None = None
    get: SQLGetOperation | None = None
    shell: Callable[[], None] | None = None
    get_connection: Callable[[], QueryConnection] | None = None
    storage: StorageManager | None = None
    storage_bootstrap_report: StorageBootstrapReport | None = None

    # Todo: Split some of these out into factory methods and slim this down
    _main_tables: set[str] | None
    _interlink_tables: set[str] | None
    _metadata_sql: MetadataSQLAPI | None
    _maintainer_callback_proxy: MaintenanceCallbackSinkAPI | None
    maintenance: Maintainer | None
    maintainer: Maintainer | None
    _failed_runtime_maintainers: list[Maintainer]
    _retired_drivers: list[DatabaseDriverAPI]
    _link_has_priority: dict[str, bool] | None
    _lifecycle_state: Literal["initializing", "open", "closing", "closed"] = (
        "initializing"
    )
    _owned_storage_managers: list[StorageManager]

    def __init__(
        self,
        metadata: dict[str, Any] | None = None,
        db_type: str = "SQLite",
        create: bool = False,
        backup: bool = True,
        existing_driver: Optional[DatabaseDriverAPI] = None,
        enable_storage_manager: bool = False,
        strict_storage_manager_bootstrap: bool = False,
        storage_startup_on_add: bool = False,
        repair_bootstrap_rows: bool = True,
        enable_maintenance: bool = False,
        application_preferences: Mapping[str, Any] | None = None,
    ) -> None:
        """
        Open or create a backend and attach the shared queues and runtime services.

        The existing-driver branch copies connection metadata/type and assumes
        the database exists. The standard branch records metadata/type and checks
        backend existence. Failed construction closes acquired resources and preserves
        the original exception, with notes for any cleanup failure.

        Example:
            Given an existing SQLite library path, db =
            Database(metadata={"database_path": path}, repair_bootstrap_rows=False,
            enable_maintenance=False, enable_storage_manager=False) omits those optional
            bootstrap services; close db when finished.


        :param metadata: Connection metadata; None selects the configured default
            database when no driver is supplied.
        :param db_type: Backend registry name, default SQLite.
        :param create: Explicitly request new schema creation; existing targets are
            rejected.
        :param backup: Retained compatibility argument; creation never replaces or backs
            up an existing target.
        :param existing_driver: Already constructed driver; requires metadata=None.
        :param enable_storage_manager: Load configured Stores after database
            initialization when True.
        :param strict_storage_manager_bootstrap: Propagate storage loading failures and
            reject failed reports when True.
        :param storage_startup_on_add: Request Store startup while loading
            configurations when True.
        :param repair_bootstrap_rows: Allow rating and null-row repair writes during
            initialization.
        :param enable_maintenance: Start maintenance and attach its callback when True.
        :param application_preferences: Store selected before construction; omitted
            values use the lazy process view.
        :return: None; the instance owns the resulting database connections.
        :raises AssertionError: Both existing_driver and metadata are supplied.
        """
        self.preferences = (
            current_preferences()
            if application_preferences is None
            else application_preferences
        )
        self.metadata = None
        self.type = None

        self._driver = None
        self._driver_wrapper = None
        self._macros = None
        self.storage = None
        self.storage_bootstrap_report = None
        self._owned_storage_managers = []
        self._lifecycle_state = "initializing"
        self._metadata_sql = None
        self.write_telemetry = DatabaseWriteTelemetry()
        self.dirty_records_queue = ObservedDirtyRecordsQueue[tuple[str, int, str]](
            telemetry=self.write_telemetry
        )
        self._maintainer_callback_proxy = None
        self.maintenance = self.maintainer = None
        self._failed_runtime_maintainers = []
        self._retired_drivers = []
        self.clean = lambda *args, **kwargs: None

        try:
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
                assert metadata is None, (
                    "driver is provided - it's assumed that the db metadata is contained within"
                )
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

            if enable_storage_manager:
                self.bootstrap_storage_manager(
                    startup_on_add=storage_startup_on_add,
                    strict=strict_storage_manager_bootstrap,
                )
            self._lifecycle_state = "open"
        except BaseException as error:
            try:
                self.close()
            except BaseException as cleanup_error:
                error.add_note(
                    f"Database construction cleanup failed: {cleanup_error!r}"
                )
            raise

    @property
    def driver(self) -> DatabaseDriverAPI:
        """
        Return the currently bound backend driver.

        Example:
            For an open db, db.driver.conn accesses the primary backend connection when
            the driver exposes one.


        :return: Bound driver object.
        :raises DatabaseIntegrityError: The facade is unbound or closed.
        """
        if self._driver is None:
            raise DatabaseIntegrityError("Database driver is not bound or is closed")
        return self._driver

    @property
    def driver_wrapper(self) -> DatabaseDriverWrapperAPI:
        """
        Return the wrapper that provides facade operations and the reentrant mutex.

        Example:
            For an open db, db.driver_wrapper.lock is shared with db.lock; use
            get_connection() for caller-owned SQL.


        :return: Bound wrapper object.
        :raises DatabaseIntegrityError: The facade is unbound or closed.
        """
        if self._driver_wrapper is None:
            raise DatabaseIntegrityError("Database wrapper is not bound or is closed")
        return self._driver_wrapper

    @property
    def macros(self) -> DatabaseMacrosAPI:
        """
        Return the currently bound schema-specific SQL macros.

        Example:
            After construction, db.macros is the macros object taken from the bound
            driver.


        :return: Bound common macro provider; legacy capabilities are backend-specific.
        :raises DatabaseIntegrityError: The facade is unbound or closed.
        """
        if self._macros is None:
            raise DatabaseIntegrityError("Database macros are not bound or are closed")
        return self._macros

    @property
    def metadata_sql(self) -> MetadataSQLAPI | None:
        """Return physical-table SQL helpers, or None before binding/after cleanup.

        Catalog owns bibliographic interpretation and compatibility projections;
        these helpers operate on actual stored rows using portable transactions.
        """
        return self._metadata_sql

    def existing_driver_init(
        self,
        existing_driver: DatabaseDriverAPI,
        *,
        repair_bootstrap_rows: bool = True,
        enable_maintenance: bool = False,
    ) -> None:
        """
        Bind an existing driver, classify its schema and initialize runtime helpers.

        This internal constructor path assumes existence and binds the driver/macros
        before schema discovery and transactional bootstrap repairs. It copies
        the driver metadata and recognized backend type to the facade. Constructor
        telemetry and queue setup must already exist.

        Example:
            Database(existing_driver=driver, enable_storage_manager=False) invokes this
            path after setting up the facade state.


        :param existing_driver: Constructed driver with a compatible existing schema.
        :param repair_bootstrap_rows: Run rating/null-row repair when True.
        :param enable_maintenance: Create the maintenance service when True.
        :return: None; facade and wrapper schema state is populated.
        """
        # Load the driver constructor - use this to make the driver instance for this database
        self.set_driver(existing_driver)
        # A standalone driver's macros have no facade yet. Bootstrap repair uses
        # the facade's transaction owner, so bind it before any repair can run.
        self.driver.db = self
        self.macros.db = self

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
        metadata: dict[str, Any] | None = None,
        db_type: str = "SQLite",
        create: bool = False,
        backup: bool = True,
        repair_bootstrap_rows: bool = True,
        enable_maintenance: bool = False,
    ) -> None:
        """
        Select a backend and create its schema when explicit or required for a new file.

        Capture path existence before opening a driver. Build missing files privately
        and publish create-only, so a competing creation cannot be overwritten. Explicit
        server creation requires an absent or empty schema. Only after creation succeeds
        open the ordinary driver and attach its wrapper.

        Example:
            The constructor calls this method for Database(metadata={"database_path":
            new_path}); a missing file path triggers creation even with create=False.


        :param metadata: Connection hints, defaulting to the configured library path.
        :param db_type: Backend registry name.
        :param create: Request new schema creation, rejecting existing targets.
        :param backup: Compatibility argument; use replace_database for intentional
            replacement.
        :param repair_bootstrap_rows: Repair rating and null rows after schema
            discovery.
        :param enable_maintenance: Enable the background maintenance service.
        :return: None; binds the driver and runtime collaborators.
        """
        if metadata is None:
            metadata = {"database_path": LiuXin_default_database}

        db_path = metadata.get("database_path")
        server_database = _metadata_uses_server_database(metadata, db_type)
        path_backed = not server_database
        path_existed = (
            db_path is not None
            and bool(db_path)
            and path_backed
            and db_path != ":memory:"
            and os.path.lexists(db_path)
        )

        self.metadata = metadata
        self.type = db_type
        driver_type = load_database_driver(db_type)

        if create or (
            path_backed and not path_existed and db_path not in (None, ":memory:")
        ):
            initializer = driver_type(self.metadata, self, set_conn=False)
            try:
                initializer.direct_create_new_database()
            finally:
                initializer.close()

        candidate = driver_type(self.metadata, self)
        try:
            self.set_driver(candidate)
        except BaseException as error:
            # set_driver leaves rejected candidates with their caller. Here the
            # constructor created that candidate, so it must release it itself.
            if self._driver is not candidate:
                try:
                    candidate.close()
                except BaseException as cleanup_error:
                    error.add_note(
                        f"Candidate driver cleanup failed: {cleanup_error!r}"
                    )
            raise

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

    def _initialise_runtime_collaborators(
        self,
        *,
        enable_maintenance: bool = False,
        callback_proxy_cls: Callable[
            [Maintainer, DatabaseWriteTelemetry | None], MaintenanceCallbackSinkAPI
        ] = TelemetryMaintainerProxy,
    ) -> None:
        """
        Bind maintenance callbacks, preferences and collaborator back-references.

        Retain the application preferences selected at construction. Wire db on the
        driver, wrapper, macros and metadata helper. Repeated enabled initialization
        reuses its worker. New workers start only after wiring succeeds; failures
        restore the previous graph. Disabling stops and joins the existing worker
        before releasing it. Callers must serialize initialization with database use.

        Example:
            Database(enable_maintenance=False, metadata=metadata) uses the no-op
            callback path while still binding its existing collaborators.


        :param enable_maintenance: Start a Maintainer and telemetry proxy when True;
            otherwise install no-op callbacks.
        :param callback_proxy_cls: Factory for the maintenance callback sink.
        :return: None.
        """
        self._require_runtime_mutable()
        # Resolve required owners before starting a background service. Public
        # collaborator access rejects use before binding or after cleanup.
        driver, wrapper, macros = self.driver, self.driver_wrapper, self.macros
        metadata_sql = self.metadata_sql
        if metadata_sql is None:
            raise RuntimeError("Table SQL helpers are not initialized")
        previous = self.maintenance
        candidate = previous if enable_maintenance else None
        created = False
        old_proxy = self._maintainer_callback_proxy
        old_callback = driver.maintainer_callback
        old_clean = self.clean
        old_owners = (wrapper.db, driver.db, macros.db, metadata_sql.db)
        try:
            if enable_maintenance:
                from LiuXin_alpha.databases.maintenance.service import Maintainer

                if candidate is None:
                    candidate = Maintainer(self, autostart=False)
                    created = True
                proxy = callback_proxy_cls(candidate, self.write_telemetry)
            else:
                proxy = None

            # Bind required owners before a new thread can observe the graph.
            wrapper.db = self
            driver.db = self
            macros.db = self
            metadata_sql.db = self
            driver.maintainer_callback = proxy or _NoopMaintainerCallback()
            self._maintainer_callback_proxy = proxy
            self.maintenance = self.maintainer = candidate
            self.clean = (
                candidate.clean
                if candidate is not None
                else lambda *args, **kwargs: None
            )
            if created and candidate is not None:
                candidate.start()
            if not enable_maintenance and previous is not None:
                previous.close()
        except BaseException as error:
            if created and candidate is not None:
                try:
                    candidate.close()
                except Exception as cleanup_error:
                    self._failed_runtime_maintainers.append(candidate)
                    error.add_note(
                        f"Maintenance cleanup failed; retained for close(): {cleanup_error}"
                    )
            self.maintenance = self.maintainer = previous
            self._maintainer_callback_proxy = old_proxy
            self.clean = old_clean
            driver.maintainer_callback = old_callback
            wrapper.db, driver.db, macros.db, metadata_sql.db = old_owners
            raise

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

        The runtime helper creates an owned manager if needed or rebinds the existing
        attachment without taking ownership. Close shuts down every created manager,
        including those subsequently replaced by a caller-owned attachment.
        Non-strict loading errors become failed reports, but manager construction errors
        propagate. Partial loading effects are not rolled back.

        Example:
            For an open db, report = db.bootstrap_storage_manager(startup_on_add=False)
            refreshes Store registrations; inspect report.issues for failures or skips.


        :param startup_on_add: Request backend startup during loading.
        :param include_offline: Include offline configurations in loading.
        :param clear_existing: Replace existing registered facades and remove absent
            configurations when True.
        :param strict: Propagate loading exceptions and reject reports with counted
            failures.
        :return: StorageBootstrapReport also retained on the facade.
        :raises StorageManagementError: Strict loading reports failures, or manager
            construction rejects configuration.
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
        """Prepare and install a complete driver binding before retiring old resources.

        Validate replacement schema, refresh categories and identity caches, and
        bind owners, aliases and the dirty queue. Preserve same-catalogue maintenance
        and storage services. Preparation failure leaves the old facade usable and
        the candidate owned by its caller. A different old driver is rolled back
        and closed after publication; reinstalling the same driver retains its SQL
        connections. Pending primary work is never committed by replacement.

        If retiring old handles fails, the new binding is already installed. Keep
        the retired driver owned, report the error and enter ``closing``; call
        ``close()`` to finish shutdown of both drivers. Do not retry the replacement
        or assume the candidate is still caller-owned after publication.

        Call while direct SQL users are quiescent. Managed macro transactions and
        drivers owned by another facade are rejected. Changing physical catalogues
        with owned or attached runtime services or queued work requires a separate Database.

        :param new_driver: Compatible open driver to install.
        :return: None.
        """
        self._require_runtime_mutable()
        old_driver = getattr(self, "_driver", None)
        old_wrapper = getattr(self, "_driver_wrapper", None)
        queue = self.dirty_records_queue
        old_lock = getattr(self, "lock", None)
        if queue is None or (old_driver is not None and old_lock is None):
            raise DatabaseIntegrityError(
                "Cannot replace the driver of a closed database"
            )
        for owner in (new_driver.db, new_driver.macros.db):
            if owner is not None and owner is not self:
                raise InputIntegrityError(
                    "Replacement driver belongs to another database"
                )
        for driver in (old_driver, new_driver):
            if (
                driver is not None
                and driver.macros.current_transaction_connection() is not None
            ):
                raise InputIntegrityError(
                    "Cannot replace a driver during a macro transaction"
                )
        if old_driver is not None and not same_catalogue(old_driver, new_driver):
            if (
                self.maintenance is not None
                or self._failed_runtime_maintainers
                or self._owned_storage_managers
                or getattr(self, "storage", None) is not None
                or not queue.empty()
            ):
                raise InputIntegrityError(
                    "Open a separate Database to change catalogues with runtime services or queued work"
                )

        # Preparation reads only the candidate. Failed validation leaves the old
        # owner graph, SQL connections and services intact; the candidate stays owned
        # by its caller. Callers must quiesce direct SQL users before replacing drivers.
        wrapper = DriverWrapper(new_driver, db=self)
        try:
            bindings = (
                schema_bindings(wrapper, self.helper_tables)
                if old_driver is not None
                else {}
            )
            bindings.update(connection_metadata(new_driver))
            bindings.update(
                {
                    "_driver": new_driver,
                    "_driver_wrapper": wrapper,
                    "_macros": new_driver.macros,
                    "_metadata_sql": MetadataSQL(self),
                    "lock": wrapper.lock,
                    "conn": new_driver.conn,
                }
            )
            for name in (
                "execute",
                "executemany",
                "executescript",
                "get",
                "shell",
                "get_connection",
            ):
                bindings[name] = getattr(wrapper, name, None)
            wrapper.dirty_records_queue = queue
            # Preserve the existing service and its queues; its context resolves this
            # facade on each dispatch. Storage repositories likewise resolve live macros.
            callback = self._maintainer_callback_proxy or _NoopMaintainerCallback()
            previous = (
                new_driver.db,
                new_driver.macros.db,
                new_driver.dirty_records_queue,
                new_driver.maintainer_callback,
            )
            with old_lock if old_lock is not None else nullcontext():
                try:
                    new_driver.db = self
                    new_driver.macros.db = self
                    new_driver.dirty_records_queue = queue
                    new_driver.maintainer_callback = callback
                except BaseException:
                    (
                        new_driver.db,
                        new_driver.macros.db,
                        new_driver.dirty_records_queue,
                        new_driver.maintainer_callback,
                    ) = previous
                    raise
                vars(self).update(bindings)
                if old_driver is not None and old_driver is not new_driver:
                    self._retired_drivers.append(old_driver)
                try:
                    if old_wrapper is not None:
                        old_wrapper.close()
                    self._close_retired_drivers()
                except BaseException as error:
                    self._lifecycle_state = "closing"
                    error.add_note(
                        "Replacement binding is installed; retired cleanup failed. "
                        "Call Database.close() to finish both driver lifetimes."
                    )
                    raise
        except BaseException:
            if getattr(self, "_driver_wrapper", None) is not wrapper:
                wrapper.close()
            raise

    def _close_retired_drivers(self) -> None:
        """Release replaced owners only after every one of their handles closes.

        Failed cleanup leaves the driver and its ownership back-references intact.
        Successful cleanup detaches it and removes the retry obligation. The active
        binding is separate and remains available to outstanding owned services
        until Database.close reaches it.
        """
        while self._retired_drivers:
            retired = self._retired_drivers[-1]
            close_driver(retired)
            retired.db = None
            retired.macros.db = None
            retired.dirty_records_queue = None
            retired.maintainer_callback = _NoopMaintainerCallback()
            self._retired_drivers.pop()

    def __del__(self) -> None:
        """
        Attempt close during finalization and suppress ordinary cleanup exceptions.

        Finalization timing is not deterministic; explicit close or a context manager is
        required for predictable resource release.

        Example:
            Prefer db.close() before discarding the last reference to an open database.


        :return: None.
        """
        try:
            self.close()
        except Exception:
            # Never raise from __del__
            pass

    @property
    def lifecycle_state(self) -> Literal["initializing", "open", "closing", "closed"]:
        """Report whether construction, normal use or resource shutdown owns the facade.

        A failed close leaves ``closing`` until a retry succeeds. Dependencies remain
        available to outstanding owned work; callers must not begin new operations.
        """
        return self._lifecycle_state

    def _require_runtime_mutable(self) -> None:
        """Reject service startup and rebinding once shutdown has been requested."""
        if self._lifecycle_state in {"closing", "closed"}:
            raise DatabaseIntegrityError("Database is closing or closed")

    def close(self) -> None:
        """Stop owned services before releasing their database dependencies.

        Maintenance must finish before owned storage managers close. Injected managers
        remain caller-owned. A service failure propagates and leaves dependencies and
        ownership intact for another close attempt. Rebinding and service startup are
        refused once closing begins; callers must quiesce their own operations.

        After services stop, retry retired drivers, then release active driver handles.
        A handle failure also leaves ``closing`` and retains its owner for retry;
        some other handles may already be closed, so this is not a usable database.
        Discard pending primary work without committing during cleanup.
        Previously committed writes remain durable. Repeated successful close is safe;
        finalization is best effort, whereas explicit close reports failures.
        """
        if self._lifecycle_state == "closed":
            return
        self._lifecycle_state = "closing"

        # A failed startup may also have left a worker awaiting shutdown. Keep
        # every reference until all workers have stopped; retries are idempotent.
        pending = list(getattr(self, "_failed_runtime_maintainers", ()))
        maintenance = getattr(self, "maintenance", None)
        if maintenance is not None and maintenance not in pending:
            pending.append(maintenance)
        for worker in pending:
            worker.close()
        self._failed_runtime_maintainers = []

        owned_storage = getattr(self, "_owned_storage_managers", [])
        while owned_storage:
            owned_storage[-1].close()
            owned_storage.pop()

        self._close_retired_drivers()
        driver, wrapper = self._driver, self._driver_wrapper
        if driver is not None:
            close_driver(driver)
        if wrapper is not None:
            wrapper.close()
        self.break_cycles()
        self._lifecycle_state = "closed"

    def __enter__(self) -> "Database":
        """
        Return this facade for use inside a cleanup context.

        No new transaction is opened.

        Example:
            For an open db, with db as active: binds active to db and closes it when the
            block exits.


        :return: This Database instance.
        """

        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> Literal[False]:
        """
        Close the facade and allow a context-body exception to propagate.

        Both normal and exceptional exits discard pending work. Already committed
        operations remain durable; this resource context does not group writes. If the
        body already raised, cleanup failure is added as a note on that exception.

        Example:
            An exception inside with db: propagates after rollback and cleanup;
            use macros.transaction() to group supported semantic writes.


        :param exc_type: Exception class passed by the context protocol, or None.
        :param exc: Exception instance passed by the context protocol, or None.
        :param tb: Traceback passed by the context protocol, or None.
        :return: False, so exceptions are not suppressed.
        """

        try:
            self.close()
        except BaseException as cleanup_error:
            if exc is None:
                raise
            exc.add_note(f"Database context cleanup failed: {cleanup_error!r}")
        return False

    # Todo: Might actually want to delete these objects - and this might be an internal method
    def break_cycles(self) -> None:
        """
        Clear selected runtime and connection references without closing resources.

        Each assignment is best effort, including on partially initialized instances.
        Storage, preferences and the clean alias are not cleared. Call close first when
        handles still need releasing; clearing references alone does not perform
        shutdown.

        Example:
            Normal db.close() invokes this after attempting to close its captured
            connections.


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
            For an initialized db, "works" in db.main_tables checks its cached
            main-table classification.


        :return: New frozenset of main-table names.
        :raises TypeError: The underlying category is still None.
        """
        tables = self._main_tables
        if tables is None:
            raise TypeError("The main-table category is not initialized")
        return frozenset(tables)

    @property
    def interlink_tables(self) -> frozenset[str]:
        """
        Snapshot the cached inter-table relationship category as an immutable set.

        Example:
            For an initialized db, sorted(db.interlink_tables) lists the cached
            relationship tables.


        :return: New frozenset of interlink-table names.
        :raises TypeError: The underlying category is still None.
        """
        tables = self._interlink_tables
        if tables is None:
            raise TypeError("The interlink-table category is not initialized")
        return frozenset(tables)

    def check_exists(self) -> bool:
        """
        Ask the bound driver whether its database exists.

        This does not update the cached _exists field or independently validate schema
        completeness.

        Example:
            For an open db, db.check_exists() delegates the existence check to its
            driver.


        :return: The driver exists result, whose meaning depends on the backend.
        """
        return self.driver.exists()

    # Todo: These methods also private?
    def refresh_db_metadata(self) -> None:
        """
        Rebuild cached table categories and validate required helper tables.

        Optional compatibility tables may be absent. Missing required helpers raise
        after category state has been reset, with close-name suggestions. Classification
        is name-based. A missing UUID requests set_uuid without re-reading it.
        sqlite_sequence is removed from main_tables after forming dirtiable_tables, so
        it can remain in that union. Wrapper category aliases are refreshed together
        with the database categories.

        Example:
            During initialization, db.refresh_db_metadata() classifies the discovered
            schema before its categories are copied onto the wrapper.


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
            set(self.helper_tables) - set(OPTIONAL_HELPER_TABLES) - set(self.all_tables)
        )
        if missing_helpers:
            import difflib

            suggestions = {
                t: difflib.get_close_matches(
                    t, sorted(self.all_tables), n=3, cutoff=0.6
                )
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
            err_str = default_log.log_variables(
                err_str, "ERROR", ("table_cat", table_cat), ("table", table)
            )
            raise NotImplementedError(err_str)

        # The dirtiable tables are a union of the main tables and the two types of link table
        self.dirtiable_tables = deepcopy(
            self.main_tables.union(self.interlink_tables).union(self.intralink_tables)
        )

        self._uuid = self.driver_wrapper.get_uuid()
        if six_unicode(self._uuid).lower().strip() == "none":
            self.driver_wrapper.set_uuid()

        # Remove SQLite sequence from the main tables - if present - this is for internal use only
        self._main_tables.discard("sqlite_sequence")

        # This owner refreshes registries after committed schema changes, including
        # native custom-field DDL. Keep the wrapper on the current category sets.
        self.driver_wrapper.all_tables = self.all_tables
        self.driver_wrapper.custom_tables = self.custom_tables
        self.driver_wrapper.main_tables = self.main_tables
        self.driver_wrapper.interlink_tables = self.interlink_tables
        self.driver_wrapper.intralink_tables = self.intralink_tables
        self.driver_wrapper.helper_tables = self.helper_tables
        self.driver_wrapper.dirtiable_tables = self.dirtiable_tables

    # Todo: Backup is somewhat useless if there is no way to restore
    def backup(self) -> None:
        """
        Request a backend-specific backup of the current database.

        Example:
            For a configured open db, db.backup() invokes driver.direct_backup(); the
            backend chooses the backup destination and behavior.


        :return: None; any driver return value is discarded.
        """
        self.driver.direct_backup()

    def create_new_database(self, blank: bool = True, backup: bool = True) -> None:
        """Request create-only initialization; never delete an existing database.

        ``blank`` and ``backup`` remain accepted for source compatibility but cannot
        authorize replacement. Use the separately named offline replacement operation.
        """
        self.driver.direct_create_new_database()

    @classmethod
    def replace_database(
        cls,
        metadata: dict[str, Any],
        *,
        backup_path: str | os.PathLike[str],
        db_type: str = "SQLite",
    ) -> pathlib.Path:
        """Replace an offline file database after preparing a verified recovery backup.

        Stop all database users first. The required backup path must be absent and
        separate from the target. Preparation/backup failures preserve the original;
        a published backup remains available if the final atomic replacement fails.
        PostgreSQL replacement is unsupported by this file operation.
        No facade is opened or returned; open the resulting database normally afterward.
        """
        driver = load_database_driver(db_type)(metadata, db=None, set_conn=False)
        try:
            return driver.direct_replace_database(backup_path=backup_path)
        finally:
            driver.close()

    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - METHODS TO OUTPUT BASIC INFORMATION ABOUT THE DATABASE START HERE

    def __unicode__(self) -> str:
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

    def __str__(self) -> str:
        """
        Return the diagnostic Unicode representation as Python text.

        Example:
            >>> db = object.__new__(Database)
            >>> db.type, db.metadata = "SQLite", {}
            >>> str(db) == db.__unicode__()
            True

        :return: Text containing the backend type and metadata.
        """

        return self.__unicode__()

    def __repr__(self) -> str:
        """
        Format the facade type and metadata database_path in a short label.

        Existing-driver construction can leave metadata=None; service-only metadata may
        omit database_path.

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
        metadata = self.metadata
        if metadata is None:
            raise TypeError("Database metadata is unavailable")
        db_path = metadata["database_path"]
        return (
            "[ LX_database - type - "
            + six_unicode(db_type)
            + " at "
            + six_unicode(db_path)
            + " ]"
        )

    # Todo: Might want to consider renaming this to full_repr, for consistency
    def full_rep(self) -> str:
        """
        Render cached identity, metadata and table categories for inspection.

        Metadata is unredacted. The legacy Helper_tables section repeats
        intralink_tables rather than rendering helper_tables.

        Example:
            For a fully initialized db, report = db.full_rep() captures its current
            category caches as text.


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

    def categorize_table(self, table_name: str) -> str:
        """
        Classify a known table name using ordered naming conventions.

        Precedence is allowed_types__ prefix, configured helper names, custom-column
        patterns, interlink pattern, intralink pattern, then main. Regex checks accept
        matching prefixes; classification does not inspect columns or foreign keys.

        Example:
            >>> db = object.__new__(Database)
            >>> db.all_tables, db.helper_tables = (
            ...     {"works", "allowed_types__works"},
            ...     set(),
            ... )
            >>> db.categorize_table("allowed_types__works")
            'allowed_types'
            >>> db.categorize_table("works")
            'main'


        :param table_name: Name converted to text and checked against all_tables.
        :return: One of allowed_types, helper, custom, interlink, intralink or main.
        :raises InputIntegrityError: The text name is absent from all_tables.
        """
        table_name = six_unicode(table_name)
        if self.all_tables is None or table_name not in self.all_tables:
            err_str = "Error - categorize_table has been passed an invalid table name."
            err_str = default_log.log_variables(
                err_str, "ERROR", ("table_name", table_name)
            )
            raise InputIntegrityError(err_str)

        return table_category(table_name, self.helper_tables)

    #
    # ----------------------------------------------------------------------------------------------------------------------
    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - METHODS TO ADD TO THE DATABASE START HERE

    def dupe_row(self, row: "RowAPI") -> "RowAPI":
        """
        Insert a blank row, copy source fields and synchronize under the new ID.

        Restore the new ID after copying the source row_dict. If sync raises
        DatabaseIntegrityError, delete the new row and reraise; cleanup errors can mask
        that failure. Other errors do not trigger this cleanup. Unique source values may
        prevent duplication.

        Example:
            In a table whose copied values satisfy its constraints, duplicate =
            db.dupe_row(row) allocates a separate row ID.


        :param row: Row whose table and field values supply the duplicate.
        :return: New persisted Row with its allocated identity.
        :raises DatabaseIntegrityError: Synchronization violates a database constraint.
        """
        row_table = self._row_table(row)
        row_table_id_col = self.driver_wrapper.get_id_column(row_table)

        payload = row._require_row_dict()
        new_row = self.get_blank_row(row_table)
        new_row_id = new_row.row_id

        # Store the old row id - replace the row dict in the new blank row - replace the row_id - sync
        new_row.row_dict = payload
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

    def delete(self, row: "RowAPI") -> None:
        """
        Delete the supplied row identity through this facade wrapper.

        Ownership is not checked: deletion uses this database with the supplied table
        and ID. The passed Row object is not cleared.

        Example:
            For a Row belonging to db, db.delete(row) removes its persistent record
            through delete_by_id.


        :param row: Row-like object providing table and a non-None row_id.
        :return: None.
        :raises InputIntegrityError: The supplied row_id is None.
        """
        row_table = self._row_table(row)
        row_id = row.row_id
        if row_id is None:
            err_str = "Unable to delete given row - row_id is not found."
            err_str = default_log.log_variables(err_str, "ERROR", ("row", row))
            raise InputIntegrityError(err_str)
        self.driver_wrapper.delete_by_id(target_table=row_table, row_id=row_id)

    #
    # ----------------------------------------------------------------------------------------------------------------------

    def get_blank_row(self, table: Optional[str] = None) -> "RowAPI":
        """
        Insert a backend blank record and wrap its generated fields in a Row.

        This performs a write. A Row-construction failure can occur after the backend
        has inserted the record.

        Example:
            On a writable database, row = db.get_blank_row("works") allocates a record
            before further field assignments.


        :param table: Target table name understood by the wrapper.
        :return: Row bound to this facade, carrying the inserted identity.
        """
        if table is None:
            raise InputIntegrityError("A table is required to create a blank row")
        blank_row_dict = self.driver_wrapper.get_blank_row(table)
        return Row(database=self, row_dict=blank_row_dict)

    # ------------------------------------------------------------------------------------------------------------------
    # - TRIGGER HELPERS
    # ------------------------------------------------------------------------------------------------------------------

    def get_triggers(self) -> list[str]:
        """
        Retrieve trigger names through the wrapper and selected driver.

        Example:
            For an open db, names = db.get_triggers() discovers triggers before
            selective removal.


        :return: Backend trigger-name list.
        """

        return self.driver_wrapper.get_triggers()

    def drop_triggers(self, triggers: list[str]) -> bool:
        """
        Delegate removal of the supplied trigger names.

        The backend determines validation and transaction behavior; this facade adds no
        rollback.

        Example:
            On a disposable database, db.drop_triggers(names) removes the selected names
            obtained from db.get_triggers().


        :param triggers: Trigger-name collection accepted by the backend.
        :return: Backend removal result; the shared SQL driver returns True on success.
        """

        return self.driver_wrapper.drop_triggers(triggers)

    def drop_all_triggers(self) -> bool:
        """
        Discover current trigger names and delegate their removal.

        Example:
            On a disposable database, db.drop_all_triggers() removes every trigger
            discovered by the selected backend.


        :return: Backend removal result; the shared SQL driver returns True on success.
        """

        return self.driver_wrapper.drop_all_triggers()

    #
    # ----------------------------------------------------------------------------------------------------------------------
    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - SPECIALIZED UPDATE METHODS START HERE

    def update_columns(
        self,
        values_map: dict[int, Any],
        field: Optional[str] = None,
        table: Optional[str] = None,
    ) -> None:
        """
        Forward a bulk value mapping and optional column/table hints to the wrapper.

        Example:
            For a backend-supported values_map, db.update_columns(values_map,
            field=column, table=table) forwards those arguments unchanged.


        :param values_map: Value mapping in the format accepted by the selected driver.
        :param field: Optional target field hint.
        :param table: Optional target table hint.
        :return: None; the wrapper return value is discarded.
        """
        self.driver_wrapper.update_columns(
            values_map=values_map, field=field, table=table
        )

    #
    # ----------------------------------------------------------------------------------------------------------------------
    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - MAGIC METHODS START HERE

    def __eq__(self, other: object) -> bool:
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
        if self.metadata == getattr(other, "metadata"):
            return True
        else:
            return False

    #  Todo: We need a names methods mixin

    def get_table_from_column(self, column_name: str) -> str:
        """
        Find the first table whose discovered columns contain the requested name.

        Iterate the table-to-columns mapping in its returned order. This reads schema
        metadata, not row contents. When several tables share a column name, the first
        match wins; callers needing disambiguation should supply their own table.

        Example:
            db.get_table_from_column("work_title") returns "works" in the current schema.

        :param column_name: Column name to find in the discovered column collections.
        :return: First matching table name.
        :raises InputIntegrityError: No discovered table contains the column.
        """
        for tab, col_set in self.get_tables_and_columns().items():
            if column_name in col_set:
                return tab
        raise InputIntegrityError(
            "The column is not in a table - as far as we can tell."
        )


#
# ----------------------------------------------------------------------------------------------------------------------
########################################################################################################################
########################################################################################################################
