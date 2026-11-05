"""
Define valid and intentionally invalid examples for internal static-type contracts.

The module keeps generated data, ordering and failure modes explicit so consumers
can assert stable behavior.

Example:
    Exercise internal contracts through a consuming regression::

        python -m pytest -q tests/scripts/test_internal_type_contracts.py
"""

import argparse
import sqlite3
from collections.abc import Iterator, KeysView, Mapping, Sequence
from contextlib import AbstractContextManager
from typing import Any, Literal, Protocol, assert_type

import LiuXin_alpha.storage.api as storage
from LiuXin_alpha.core.commands import CoreCommand
from LiuXin_alpha.core.program_api import CoreProgramAPI
from LiuXin_alpha.core.program_endpoints import install_program_endpoints
from LiuXin_alpha.core.program_endpoints.common import ProgramEndpointRegistrar
from LiuXin_alpha.core.program_endpoints.handlers import ProgramEndpointHandlers
from LiuXin_alpha.core.program_endpoints.storage import install_queries
from LiuXin_alpha.core.program_services.evacuation_execution import (
    EvacuationExecution,
    execute_evacuation,
)
from LiuXin_alpha.core.program_services.evacuation_models import (
    EvacuationLimits,
    EvacuationPlan,
)
from LiuXin_alpha.core.program_services.evacuation_planning import build_evacuation_plan
from LiuXin_alpha.core.queries import CoreQuery
from LiuXin_alpha.core.readonly import CoreReadClientAPI, ReadOnlyCoreClient
from LiuXin_alpha.core.runtime import CoreRuntime
from LiuXin_alpha.databases.api.driver_wrapper_api import DatabaseDriverWrapperAPI
from LiuXin_alpha.databases.api.custom_columns_api import (
    CustomColumnCache,
    CustomColumnsAPI,
)
from LiuXin_alpha.databases.api import (
    DatabaseAPI,
    DatabaseGeneratorAPI,
    RowAPI,
    MacrosAPI,
)
from LiuXin_alpha.databases.db_types.macro_types import LegacyLinkData
from LiuXin_alpha.databases.db_types.repair_types import PhysicalReference
from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.databases.database.dirtied_mixin import ObservedDirtyRecordsQueue
from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.databases.db_types.row_types import RowPayload
from LiuXin_alpha.databases.api.driver_api import DatabaseDriverAPI
from LiuXin_alpha.databases.api.portable_macros_api import PortableMacrosAPI
from LiuXin_alpha.databases.api.maintenance_api import MaintenancePluginAPI
from LiuXin_alpha.databases.api.metadata_sql_api import MetadataSQLAPI
from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.connection import (
    PostgresConnectionAdapter,
)
from LiuXin_alpha.databases.db_types.backend_capabilities import (
    BackendCapabilities,
    BackendFeature,
)
from LiuXin_alpha.databases.db_types.driver_types import (
    QueryConnection,
    QueryCursor,
    RowPage,
    RowSQLConnection,
    SQLConnection,
    SQLCursor,
    SQLiteFunctionConnection,
    SQLRow,
)
from LiuXin_alpha.databases.db_types.query_observation import QueryObservation
from LiuXin_alpha.databases.db_types.query_types import TextMatch
from LiuXin_alpha.databases.driver_wrapper import DriverWrapper
from LiuXin_alpha.databases.maintenance.plugins import (
    MaintenancePluginContext,
    MaintenancePluginUpgrade,
)
from LiuXin_alpha.databases.maintenance.builtin_plugins import NullMaintenancePlugin
from LiuXin_alpha.databases.maintenance.bound_connection import BoundConnection
from LiuXin_alpha.databases.maintenance.runtime import MaintenanceRuntime
from LiuXin_alpha.databases.maintenance.rebuild import RebuildDefinition, rebuild_page
from LiuXin_alpha.databases.transactions import transaction_scope
from LiuXin_alpha.storage.storage_manager.manager import TransientStorageManager
from LiuXin_alpha.storage.storage_manager.mixins._state import _StorageManagerState
from LiuXin_alpha.surfaces.acquisition_types import AcquisitionReader, CoreStoredFile
from LiuXin_alpha.surfaces.cli.completion import build_completion_parser
from LiuXin_alpha.surfaces.cli.parser_types import (
    CompletionRegistrar,
    CompletionSubparsers,
)
from LiuXin_alpha.surfaces.cli.parsers import create_parser
from LiuXin_alpha.surfaces.core import CoreRow, CoreSurfaceModel
from LiuXin_alpha.surfaces.presentation import RowLookup, row_value
from LiuXin_alpha.surfaces.terminal.browser import TextDatabaseBrowser
from LiuXin_alpha.surfaces.terminal.browser_components.models import RowRecord
from LiuXin_alpha.surfaces.terminal.commands.base import TerminalCommandAPI
from LiuXin_alpha.surfaces.terminal.plugins.base import TerminalLifecyclePluginAPI
from LiuXin_alpha.surfaces.terminal.windowed_components.models import CursesWindow
from LiuXin_alpha.surfaces.terminal.windowed_ui import _CursesUiDriver


def driver_wrapper_contracts(
    wrapper: DatabaseDriverWrapperAPI,
    implementation: DriverWrapper,
    sqlite_connection: sqlite3.Connection,
    postgres_connection: PostgresConnectionAdapter,
) -> None:
    """Check concrete conformance, result shapes and mistakes at the SQL boundary."""
    contract: DatabaseDriverWrapperAPI = implementation
    connection: QueryConnection = sqlite_connection
    cursor: QueryCursor = connection.execute("SELECT ?", (1,))
    cursor.close()
    postgres: QueryConnection = postgres_connection
    pg_cursor: QueryCursor = postgres.execute("SELECT ?", (1,))
    pg_cursor.close()
    assert_type(contract.add_row({"work_title": "Example"}), int)
    assert_type(wrapper.get_all_rows("works"), list[dict[str, Any]])
    assert_type(wrapper.get_tables(), list[str])
    assert_type(wrapper.get_tables_and_columns(), dict[str, list[str]])
    assert_type(wrapper.get_row_from_id("works", 1), dict[str, Any] | Literal[False])
    assert_type(wrapper.get_view_row_from_id("some_view", 1), dict[str, Any] | None)
    assert_type(wrapper.get_link_table_name("works", "agents"), str | Literal[False])
    assert_type(wrapper.delete_by_id("works", [1, 2]), bool)
    assert_type(wrapper.update_row({"work_id": 1}), bool | None)
    assert_type(wrapper.execute("SELECT ?", (1,)), QueryCursor)
    wrapper.set_custom_column_metadata(1, display={"heading": "Shelf"})
    wrapper.delete_by_id("works", "one")  # expect-error: reportArgumentType arg-type
    wrapper.set_custom_column_metadata(
        1,
        is_editable="yes",  # expect-error: reportArgumentType arg-type
    )
    wrapper.set_custom_column_metadata(
        1,
        display="{}",  # expect-error: reportArgumentType arg-type
    )
    wrapper.get_all_rows(
        "works",
        reverse="yes",  # expect-error: reportArgumentType arg-type
    )
    wrapper.get_row_from_id("works", 1)[  # expect-error: reportIndexIssue index
        "work_title"
    ]
    missing_view = wrapper.get_view_row_from_id("some_view", 1)
    missing_view["id"]  # expect-error: reportOptionalSubscript index

    assert_type(wrapper.fetch_one("SELECT 1", connection=connection), SQLRow | None)
    assert_type(wrapper.fetch_all("SELECT 1"), list[SQLRow])
    assert_type(wrapper.scan_page("works", columns=["work_title"], after_id=3), RowPage)
    assert_type(wrapper.iter_row_batches("works", batch_size=20), Iterator[RowPage])
    assert_type(wrapper.get_backend_capabilities(), BackendCapabilities)
    wrapper.get_backend_capabilities().require(BackendFeature.READ_SNAPSHOTS)
    events: list[QueryObservation] = []
    wrapper.set_query_observers([events.append])
    wrapper.scan_page(
        "works",
        batch_size="20",  # expect-error: reportArgumentType arg-type
    )
    wrapper.fetch_one(
        "SELECT 1",
        connection=object(),  # expect-error: reportArgumentType arg-type
    )
    wrapper.set_query_observers(
        [object()],  # expect-error: reportArgumentType list-item
    )


def readonly_client_contracts(
    reader: CoreReadClientAPI,
    implementation: ReadOnlyCoreClient,
) -> None:
    """Accept named reads while hiding mutation from the narrow surface contract.

    Example:
        >>> readonly_client_contracts(reader, implementation)  # doctest: +SKIP
    """
    restricted: CoreReadClientAPI = implementation
    restricted.query("schema.tables")
    reader.query("health")
    reader.command("shutdown")  # expect-error: reportAttributeAccessIssue attr-defined


def terminal_component_contracts(
    browser: TextDatabaseBrowser,
    driver: _CursesUiDriver,
    row: CoreRow,
    window: CursesWindow,
) -> None:
    """
    Perform the terminal component contracts step with deterministic fixture inputs.

    Example:
        Exercise terminal component contracts through a consuming regression::

            python -m pytest -q tests/scripts/test_internal_type_contracts.py


    :param browser: Value supplied for browser under the deterministic fixture contract.
    :param driver: Value supplied for driver under the deterministic fixture contract.
    :param row: Row values inserted into or read from deterministic fixture state.
    :param window: Value supplied for window under the deterministic fixture contract.
    :return: None; fixture state or the supplied destination is updated in place.
    """
    row_record: RowRecord = row
    assert_type(browser._table_slice("works", limit=2, offset=0), list[CoreRow])
    assert_type(browser.format_row("works", row_record), str)
    assert_type(driver._visible_console_lines(width=80, visible_rows=10), list[str])
    driver.bind_browser(browser)
    window.addstr(0, 0, "content")
    browser._table_slice(
        "works",
        limit="two",  # expect-error: reportArgumentType arg-type
        offset=0,
    )
    browser.format_row("works", object())  # expect-error: reportArgumentType arg-type
    driver.bind_browser(object())  # expect-error: reportArgumentType arg-type
    driver.read_line("prompt", initial="x")  # expect-error: reportCallIssue call-arg
    window.addstr(0, 0, 7)  # expect-error: reportArgumentType arg-type


class TerminalEmitter(Protocol):
    """
    A small host capability an external terminal extension can request.

    Example:
        Exercise TerminalEmitter through a consuming regression::

            python -m pytest -q tests/scripts/test_internal_type_contracts.py
    """

    def emit(self, text: str, *, end: str = "\n") -> None:
        """
        Perform the emit step with deterministic fixture inputs.

        Example:
            Exercise TerminalEmitter.emit through a consuming regression::

                python -m pytest -q tests/scripts/test_internal_type_contracts.py


        :param text: Text encoded, parsed or embedded in the fixture.
        :param end: Value supplied for end under the deterministic fixture contract.
        :return: None; fixture state or the supplied destination is updated in place.
        """
        ...


class EmittingCommand(TerminalCommandAPI[TerminalEmitter]):
    """
    Check concrete command conformance against the real generic API.

    Example:
        Exercise EmittingCommand through a consuming regression::

            python -m pytest -q tests/scripts/test_internal_type_contracts.py
    """

    def execute(self, browser: TerminalEmitter, args: list[str]) -> bool:
        """
        Execute the internal contracts fixture workflow.

        Example:
            Exercise EmittingCommand.execute through a consuming regression::

                python -m pytest -q tests/scripts/test_internal_type_contracts.py


        :param browser: Value supplied for browser under the deterministic fixture contract.
        :param args: Positional arguments forwarded to the bounded test double.
        :return: The deterministic fixture value, path, bytes, record or collection
            described above.
        """
        browser.emit(" ".join(args))
        return True


class WrongTerminalCommand(TerminalCommandAPI[TerminalEmitter]):
    """
    Provide an intentionally invalid WrongTerminalCommand implementation for static-type rejection tests.

    Example:
        Exercise WrongTerminalCommand through a consuming regression::

            python -m pytest -q tests/scripts/test_internal_type_contracts.py
    """

    def execute(  # expect-error: reportIncompatibleMethodOverride -
        self,
        browser: int,  # expect-error: - override
        args: list[str],
    ) -> bool:
        """
        Execute the internal contracts fixture workflow.

        Example:
            Exercise WrongTerminalCommand.execute through a consuming regression::

                python -m pytest -q tests/scripts/test_internal_type_contracts.py


        :param browser: Value supplied for browser under the deterministic fixture contract.
        :param args: Positional arguments forwarded to the bounded test double.
        :return: The deterministic fixture value, path, bytes, record or collection
            described above.
        """
        return bool(browser or args)


def terminal_extension_contracts(
    browser: TextDatabaseBrowser,
    host: TerminalEmitter,
    command: TerminalCommandAPI[TerminalEmitter],
    plugin: TerminalLifecyclePluginAPI[TerminalEmitter],
) -> None:
    """
    Perform the terminal extension contracts step with deterministic fixture inputs.

    Example:
        Exercise terminal extension contracts through a consuming regression::

            python -m pytest -q tests/scripts/test_internal_type_contracts.py


    :param browser: Value supplied for browser under the deterministic fixture contract.
    :param host: Value supplied for host under the deterministic fixture contract.
    :param command: Value supplied for command under the deterministic fixture contract.
    :param plugin: Value supplied for plugin under the deterministic fixture contract.
    :return: None; fixture state or the supplied destination is updated in place.
    """
    assert_type(command.execute(host, ["help"]), bool)
    assert_type(EmittingCommand().execute(host, []), bool)
    command.execute(browser, [])
    browser.register_command(EmittingCommand())
    browser.register_lifecycle_plugin(plugin)
    plugin.on_startup(host)
    plugin.on_shutdown(browser, reason="quit")
    command.execute(object(), [])  # expect-error: reportArgumentType arg-type
    command.execute(host, "help")  # expect-error: reportArgumentType arg-type
    plugin.on_startup(object())  # expect-error: reportArgumentType arg-type
    plugin.on_shutdown(host, reason=1)  # expect-error: reportArgumentType arg-type


def valid_composition(runtime: CoreRuntime) -> None:
    """
    Perform the valid composition step with deterministic fixture inputs.

    Example:
        Exercise valid composition through a consuming regression::

            python -m pytest -q tests/scripts/test_internal_type_contracts.py


    :param runtime: Value supplied for runtime under the deterministic fixture contract.
    :return: None; fixture state or the supplied destination is updated in place.
    """
    api = CoreProgramAPI()
    install_program_endpoints(api, runtime)
    install_queries(api, runtime)
    manager = TransientStorageManager()
    valid_calls(manager, api, runtime, runtime)


def valid_calls(
    manager: _StorageManagerState,
    handlers: ProgramEndpointHandlers,
    registrar: ProgramEndpointRegistrar,
    runtime: CoreRuntime,
) -> None:
    """
    Perform the valid calls step with deterministic fixture inputs.

    Example:
        Exercise valid calls through a consuming regression::

            python -m pytest -q tests/scripts/test_internal_type_contracts.py


    :param manager: Value supplied for manager under the deterministic fixture contract.
    :param handlers: Value supplied for handlers under the deterministic fixture
        contract.
    :param registrar: Value supplied for registrar under the deterministic fixture
        contract.
    :param runtime: Value supplied for runtime under the deterministic fixture contract.
    :return: None; fixture state or the supplied destination is updated in place.
    """
    assert_type(manager._new_revision_locked(), str)
    assert_type(manager._metadata_transaction(), AbstractContextManager[None])
    assert_type(manager._find_asset_locked((), 0), storage.DigitalAssetRecord | None)
    assert_type(
        handlers.storage_store_get(runtime, CoreQuery("storage.store.get")),
        Mapping[str, object],
    )
    registrar.register_query_handler("store.get", handlers.storage_store_get)
    registrar.register_command_handler("store.probe", handlers.storage_store_probe)
    install_program_endpoints(handlers, registrar)


def bad_storage_calls(manager: _StorageManagerState) -> int:
    """
    Perform the bad storage calls step with deterministic fixture inputs.

    Example:
        Exercise bad storage calls through a consuming regression::

            python -m pytest -q tests/scripts/test_internal_type_contracts.py


    :param manager: Value supplied for manager under the deterministic fixture contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    manager._metadata_transactoin()  # expect-error: reportAttributeAccessIssue attr-defined
    manager._find_asset_locked((), "zero")  # expect-error: reportArgumentType arg-type
    manager._new_revision_locked("unexpected")  # expect-error: reportCallIssue call-arg
    manager._allocate_metadata_id_locked(
        "typo"  # expect-error: reportArgumentType arg-type
    )
    return manager._new_revision_locked()  # expect-error: reportReturnType return-value


def bad_endpoint_calls(
    handlers: ProgramEndpointHandlers,
    registrar: ProgramEndpointRegistrar,
    runtime: CoreRuntime,
) -> None:
    """
    Perform the bad endpoint calls step with deterministic fixture inputs.

    Example:
        Exercise bad endpoint calls through a consuming regression::

            python -m pytest -q tests/scripts/test_internal_type_contracts.py


    :param handlers: Value supplied for handlers under the deterministic fixture
        contract.
    :param registrar: Value supplied for registrar under the deterministic fixture
        contract.
    :param runtime: Value supplied for runtime under the deterministic fixture contract.
    :return: None; fixture state or the supplied destination is updated in place.
    """
    handlers.storage_store_proeb(  # expect-error: reportAttributeAccessIssue attr-defined
        runtime, CoreCommand("probe")
    )
    handlers.storage_store_get(
        runtime,
        CoreCommand("get"),  # expect-error: reportArgumentType arg-type
    )
    registrar.register_query_handler(
        "probe",
        handlers.storage_store_probe,  # expect-error: reportArgumentType arg-type
    )
    registrar.register_command_handler(
        "get",
        handlers.storage_store_get,  # expect-error: reportArgumentType arg-type
    )
    registrar.register_query_handler(
        42,  # expect-error: reportArgumentType arg-type
        handlers.storage_store_get,
    )
    registrar.register_query_handler(  # expect-error: - call-arg
        "get",
        handlers.storage_store_get,
        unexpected=True,  # expect-error: reportCallIssue -
    )
    registrar.register_query_handler(  # expect-error: reportCallIssue call-arg
        "missing"
    )
    install_program_endpoints(
        object(),  # expect-error: reportArgumentType arg-type
        registrar,
    )
    install_program_endpoints(
        handlers,
        object(),  # expect-error: reportArgumentType arg-type
    )
    install_queries(object(), registrar)  # expect-error: reportArgumentType arg-type
    install_queries(handlers, object())  # expect-error: reportArgumentType arg-type


class IncorrectStorageHelper(TransientStorageManager):
    """
    Provide an intentionally invalid IncorrectStorageHelper implementation for static-type rejection tests.

    Example:
        Exercise IncorrectStorageHelper through a consuming regression::

            python -m pytest -q tests/scripts/test_internal_type_contracts.py
    """

    def _new_revision_locked(  # expect-error: reportIncompatibleMethodOverride override
        self,
    ) -> int:
        """
        Perform the new revision locked step with deterministic fixture inputs.

        Example:
            Exercise IncorrectStorageHelper. new revision locked through a consuming regression::

                python -m pytest -q tests/scripts/test_internal_type_contracts.py


        :return: The deterministic fixture value, path, bytes, record or collection
            described above.
        """
        return 1


class IncorrectProgramHandler(CoreProgramAPI):
    """
    Provide an intentionally invalid IncorrectProgramHandler implementation for static-type rejection tests.

    Example:
        Exercise IncorrectProgramHandler through a consuming regression::

            python -m pytest -q tests/scripts/test_internal_type_contracts.py
    """

    @classmethod
    def storage_store_get(  # expect-error: reportIncompatibleMethodOverride override
        cls, runtime: CoreRuntime, query: CoreQuery
    ) -> str:
        """
        Perform the storage store get step with deterministic fixture inputs.

        Example:
            Exercise IncorrectProgramHandler.storage store get through a consuming regression::

                python -m pytest -q tests/scripts/test_internal_type_contracts.py


        :param runtime: Value supplied for runtime under the deterministic fixture contract.
        :param query: Value supplied for query under the deterministic fixture contract.
        :return: The deterministic fixture value, path, bytes, record or collection
            described above.
        """
        return "not a record"


def reject_incorrect_implementations(
    api: IncorrectProgramHandler,
    registrar: ProgramEndpointRegistrar,
) -> None:
    """
    Perform the reject incorrect implementations step with deterministic fixture inputs.

    Example:
        Exercise reject incorrect implementations through a consuming regression::

            python -m pytest -q tests/scripts/test_internal_type_contracts.py


    :param api: Value supplied for api under the deterministic fixture contract.
    :param registrar: Value supplied for registrar under the deterministic fixture
        contract.
    :return: None; fixture state or the supplied destination is updated in place.
    """
    install_program_endpoints(
        api,  # expect-error: reportArgumentType arg-type
        registrar,
    )


def evacuation_contracts(
    manager: storage.StorageManagerAPI, plan: EvacuationPlan
) -> None:
    """
    Perform the evacuation contracts step with deterministic fixture inputs.

    Example:
        Exercise evacuation contracts through a consuming regression::

            python -m pytest -q tests/scripts/test_internal_type_contracts.py


    :param manager: Value supplied for manager under the deterministic fixture contract.
    :param plan: Value supplied for plan under the deterministic fixture contract.
    :return: None; fixture state or the supplied destination is updated in place.
    """
    limits = EvacuationLimits(10, 1024)
    assert_type(
        execute_evacuation(manager, plan, limits, keep_source_bytes=True),
        EvacuationExecution,
    )
    execute_evacuation(
        manager,
        {},  # expect-error: reportArgumentType arg-type
        limits,
        keep_source_bytes=True,
    )
    execute_evacuation(
        manager,
        plan,
        {"max_actions": 10},  # expect-error: reportArgumentType arg-type
        keep_source_bytes=True,
    )
    build_evacuation_plan(
        manager,
        source_ref="not a UUID",  # expect-error: reportArgumentType arg-type
        destination_ref=None,
        max_assets=10,
    )


def surface_contracts(
    model: CoreSurfaceModel, row: CoreRow, reader: AcquisitionReader
) -> None:
    """
    Perform the surface contracts step with deterministic fixture inputs.

    Example:
        Exercise surface contracts through a consuming regression::

            python -m pytest -q tests/scripts/test_internal_type_contracts.py


    :param model: Value supplied for model under the deterministic fixture contract.
    :param row: Row values inserted into or read from deterministic fixture state.
    :param reader: Value supplied for reader under the deterministic fixture contract.
    :return: None; fixture state or the supplied destination is updated in place.
    """
    actual_reader: AcquisitionReader = model
    actual_row: RowLookup = row
    assert_type(CoreStoredFile(actual_reader, "file", 7).read_bytes(), bytes)
    assert_type(row_value(actual_row, "title"), object)
    assert_type(row_value({"title": "雪"}, "title"), object)
    CoreStoredFile(object(), "file", 7)  # expect-error: reportArgumentType arg-type
    CoreStoredFile(reader, "file", "seven")  # expect-error: reportArgumentType arg-type
    row_value(object(), "title")  # expect-error: reportArgumentType arg-type


def database_foundation_contracts(
    driver: DatabaseDriverAPI, macros: PortableMacrosAPI
) -> None:
    """Check lazy row reads, missing identities, predicates and migration callbacks."""
    assert_type(
        driver.direct_iterator_return("SELECT work_id FROM works", ("work_id",)),
        Iterator[dict[str, Any]],
    )
    assert_type(
        driver.direct_get_next_book_group(), tuple[list[dict[str, Any]], int | None]
    )
    assert_type(driver.direct_identify_table_from_row({}), str | Literal[False])
    predicate = TextMatch(("work_title",), "example")
    assert_type(
        macros.find_rows_matching("works", predicate), tuple[Mapping[str, Any], ...]
    )
    MaintenancePluginUpgrade("1", migrate=lambda context, previous: None)
    driver.direct_iterator_return(
        123,  # expect-error: reportArgumentType arg-type
        ("work_id",),
    )
    macros.find_rows_matching(
        "works",
        "raw SQL",  # expect-error: reportArgumentType arg-type
    )
    macros.find_rows_matching(
        "works",
        predicate,
        limit="100",  # expect-error: reportArgumentType arg-type
    )

    def missing_migration_context() -> None:
        pass

    MaintenancePluginUpgrade(
        "1",
        migrate=missing_migration_context,  # expect-error: reportArgumentType arg-type
    )


def maintenance_contracts(
    runtime: MaintenanceRuntime,
    borrowed: BoundConnection,
    row_connection: RowSQLConnection,
    sqlite_connection: sqlite3.Connection,
    postgres_connection: PostgresConnectionAdapter,
    definition: RebuildDefinition,
) -> None:
    """Check structural plugins and distinguish borrowed SQL from owned handles."""
    plugin: MaintenancePluginAPI = NullMaintenancePlugin()
    plugin.startup(MaintenancePluginContext(db=runtime.db))
    sqlite: SQLConnection = sqlite_connection
    sqlite_functions: SQLiteFunctionConnection = sqlite_connection
    postgres: SQLConnection = postgres_connection
    sqlite_query: QueryConnection = sqlite
    postgres_query: QueryConnection = postgres
    sqlite_query.execute("SELECT 1").close()
    postgres_query.execute("SELECT 1").close()
    borrowed_connection: RowSQLConnection = borrowed
    borrowed_connection.execute("SELECT 1").close()
    sqlite_functions.create_function("identity", 1, lambda value: value)
    with transaction_scope(sqlite_connection, owns_transaction=True) as transaction:
        assert_type(transaction, sqlite3.Connection)
    assert_type(row_connection.execute("SELECT ?", (1,)), SQLCursor)
    assert_type(runtime.deliver("example", [1, 2]), dict[str, int])
    rebuild_page(runtime, definition, limit=10)
    row_connection.commit()  # expect-error: reportAttributeAccessIssue attr-defined
    runtime.deliver(
        "example",
        ["one"],  # expect-error: reportArgumentType list-item
    )

    def invalid_claim_callback(claim: object) -> None:
        pass

    runtime.deliver(
        "example",
        [1],
        check_claim=invalid_claim_callback,  # expect-error: reportArgumentType arg-type
    )
    rebuild_page(
        runtime,
        definition,
        limit="10",  # expect-error: reportArgumentType arg-type
    )


def database_facade_contracts(
    database: DatabaseAPI, concrete: Database, row: RowAPI
) -> None:
    """Distinguish optional rows, overload modes and typed notification payloads."""
    assert_type(database.get_interlink_row(row, row), RowAPI | None)
    assert_type(
        database.get_interlink_row(row, row, onelink=False), list[RowAPI] | None
    )
    assert_type(concrete.get_interlink_row(row, row), RowAPI | None)
    assert_type(concrete.get_interlinked_rows(row, "works"), list[RowAPI | None])
    assert_type(database.get_values_set("work_title"), set[Any])
    assert_type(
        database.get_values_set("work_title", iterator_return=True), Iterator[Any]
    )
    database.multi_column_search([("work_id", "=", 1)])
    database.interlink_rows(row, row, priority=1.5, type="author")
    database.update_interlink(row, row, priority=None)
    notifications = ObservedDirtyRecordsQueue[tuple[str, int, str]]()
    notifications.put(("works", 1, "edit"))
    database.interlink_rows(
        row,
        row,
        priority=[],  # expect-error: reportArgumentType arg-type
    )
    database.multi_column_search(
        "work_id = 1",  # expect-error: reportArgumentType arg-type
    )
    notifications.put(
        ("works", "one", "edit"),  # expect-error: reportArgumentType arg-type
    )
    database.update_interlink_priority(
        row,
        "works",
        ["one"],  # expect-error: reportArgumentType list-item
    )


def row_state_contracts(row: RowAPI, concrete: Row, database: DatabaseAPI) -> None:
    """Keep missing state visible and mapping views distinct from mutable lists."""
    assert_type(row.table, str | None)
    assert_type(row.row_dict, RowPayload)
    assert_type(concrete.row_dict, RowPayload)
    assert_type(row.keys(), KeysView[str])
    assert_type(row._require_row_dict(), dict[str, Any])
    row.row_dict = None
    row.row_dict = False
    row.row_dict = {"work_id": 1}
    row.row_id = None
    row.load_row_from_id(table="works")
    row.load_row_from_id(
        "one",  # expect-error: reportArgumentType arg-type
        "works",
    )
    row.row_dict = True  # expect-error: reportAttributeAccessIssue assignment
    database.driver_wrapper.get_id_column(
        row.table,  # expect-error: reportArgumentType arg-type
    )
    keys = row.keys()
    keys.append("work_title")  # expect-error: reportAttributeAccessIssue attr-defined


def cli_parser_contracts(
    registrar: CompletionRegistrar,
    subparsers: CompletionSubparsers,
) -> None:
    """
    Perform the cli parser contracts step with deterministic fixture inputs.

    Example:
        Exercise cli parser contracts through a consuming regression::

            python -m pytest -q tests/scripts/test_internal_type_contracts.py


    :param registrar: Value supplied for registrar under the deterministic fixture
        contract.
    :param subparsers: Value supplied for subparsers under the deterministic fixture
        contract.
    :return: None; fixture state or the supplied destination is updated in place.
    """
    assert_type(
        create_parser(register_completion=build_completion_parser),
        argparse.ArgumentParser,
    )
    assert_type(create_parser(register_completion=registrar), argparse.ArgumentParser)
    registrar(subparsers)
    create_parser(
        register_completion=object(),  # expect-error: reportArgumentType arg-type
    )
    registrar(object())  # expect-error: reportArgumentType arg-type


def physical_driver_contracts(
    driver: DatabaseDriverAPI, sqlite: sqlite3.Connection, cursor: SQLCursor
) -> None:
    """Keep concrete connection identity and distinguish SQL results from services."""
    assert_type(driver._register_open_connection(sqlite), sqlite3.Connection)
    assert_type(
        driver.direct_add_multiple_simple_row_dicts(iter(())), Literal[True] | None
    )
    assert_type(driver.direct_delete_many("works", "work_id", [1]), bool | None)
    assert_type(driver.user_version, int | str | None)
    assert_type(driver.direct_get_schema_version(), int | str | None)
    assert_type(
        driver.direct_create_custom_column("works", "note", multi="many_one"), str
    )
    assert_type(cursor.execute("SELECT 1"), SQLCursor)
    driver.direct_add_multiple_simple_row_dicts(
        ["invalid row"],  # expect-error: reportArgumentType list-item
    )
    driver._read_row_for_update(
        "invalid connection",  # expect-error: reportArgumentType arg-type
        "works",
        "work_id",
        1,
    )


def sql_callback_contracts(
    driver: DatabaseDriverAPI, sqlite: sqlite3.Connection
) -> None:
    """Preserve aggregate factory shape and physical-table declarations at the boundary."""
    from LiuXin_alpha.databases.database_driver_plugins.SQL.databasedriver.utils import (
        SqliteConcatenate,
        register_sqlite_aggregate,
    )

    aggregate = SqliteConcatenate()
    aggregate.step("text")
    aggregate.step(None)
    assert_type(aggregate.finalize(), str | None)
    register_sqlite_aggregate(sqlite, "concat", 1, SqliteConcatenate)
    driver.direct_create_main_table("examples", {"name": {"datatype": "TEXT"}})
    assert_type(driver.direct_get_null_row("examples"), dict[str, Any] | Literal[False])
    aggregate.step(123)  # expect-error: reportArgumentType arg-type

    def invalid_aggregate_factory() -> tuple[str]:
        return ("state",)

    register_sqlite_aggregate(
        sqlite,
        "broken",
        1,
        invalid_aggregate_factory,  # expect-error: reportArgumentType arg-type
    )
    driver.direct_create_main_table(
        "examples",
        ["name"],  # expect-error: reportArgumentType arg-type
    )


def macro_service_contracts(macros: MacrosAPI, sqlite: sqlite3.Connection) -> None:
    """Keep transaction handles, query containers and legacy ID modes precise."""
    with macros.transaction() as connection:
        assert_type(connection, SQLConnection)
        connection.execute("SELECT ?", (1,)).close()
    with macros.read_transaction() as snapshot:
        assert_type(snapshot, SQLConnection)
    assert_type(macros.inspect_repair_references("works"), list[PhysicalReference])
    assert_type(macros.get_catalog_repair_receipt("receipt"), dict[str, Any] | None)
    assert_type(
        macros.find_rows_matching("works", TextMatch(("work_title",), "A"), limit=None),
        tuple[Mapping[str, Any], ...],
    )
    assert_type(macros.get_link_data("works", "agents", 1), LegacyLinkData)
    assert_type(
        macros.get_cc_id_from_value("custom_column_1", "A", conn=sqlite), int | None
    )
    assert_type(
        macros.get_cc_id_from_value("custom_column_1", "A", all=True, conn=sqlite),
        list[Sequence[Any]],
    )
    macros.break_cc_links_by_book_id("links", (owner for owner in (1, 2)), conn=sqlite)
    macros.insert_multiple_values_into_cc_table(
        "custom_column_1",
        ["A"],
        conn=object(),  # expect-error: reportArgumentType arg-type
    )
    macros.make_generic_link(
        "links",
        "left_id",
        "right_id",
        "priority",
        "one",  # expect-error: reportArgumentType arg-type
        2,
    )
    receipt = macros.get_catalog_repair_receipt("receipt")
    receipt["version"]  # expect-error: reportOptionalSubscript index
    with macros.transaction() as connection:
        connection.missing()  # expect-error: reportAttributeAccessIssue attr-defined
    macros.find_rows_matching(
        "works",
        TextMatch(("work_title",), "A"),
        limit="all",  # expect-error: reportArgumentType arg-type
    )


def schema_builder_and_helper_contracts(
    generator: "DatabaseGeneratorAPI", sqlite: sqlite3.Connection
) -> None:
    """Check emitted SQL, real connections, row-shaped indices and typed notification hosts."""
    from LiuXin_alpha.databases.database_driver_plugins.SQL.database_generator_frbr.database_generator import (
        SQLiteDatabaseGenerator,
    )
    from LiuXin_alpha.databases.dbprefs import DBPrefs
    from LiuXin_alpha.databases.notify import dummy_notify
    from LiuXin_alpha.databases.utils import (
        _get_next_series_num_for_list,
        get_data_as_dict,
    )

    SQLiteDatabaseGenerator(sqlite)
    assert_type(
        generator._build_allowed_types_table_interlink("links", ("author",)), list[str]
    )
    assert_type(
        generator.direct_get_direct_link_main_tables_sql(
            "works", "agents", requested_cols=iter(("type",))
        ),
        tuple[list[str], str],
    )
    generator.direct_build_intralink_table_sql(
        "works", ("related",), {"type"}, symmetric=True
    )
    assert_type(_get_next_series_num_for_list([[1.0], [2.0]]), int | float)
    assert_type(_get_next_series_num_for_list((1.0, 2.0), unwrap=False), int | float)
    SQLiteDatabaseGenerator("connection")  # expect-error: reportArgumentType arg-type
    generator.direct_build_intralink_table_sql(
        "works",
        symmetric="yes",  # expect-error: reportArgumentType arg-type
    )
    _get_next_series_num_for_list([1.0])  # expect-error: reportArgumentType list-item
    _get_next_series_num_for_list(  # expect-error: reportCallIssue call-overload
        [[1.0]],
        unwrap=False,  # expect-error: reportArgumentType -
    )
    dummy_notify("updated", [1], object())  # expect-error: reportArgumentType arg-type
    DBPrefs.read_serialized(b"/library")  # expect-error: reportArgumentType arg-type
    get_data_as_dict(object())  # expect-error: reportArgumentType arg-type


def driver_factory_contracts(metadata_sql: "MetadataSQLAPI") -> None:
    """Registry imports expose the driver constructor; row locks require a borrowed SQL handle."""
    from LiuXin_alpha.databases.database_driver_plugins.registry import (
        load_database_driver,
    )

    factory = load_database_driver("SQLite")
    assert_type(factory, type[DatabaseDriverAPI])
    assert_type(
        factory({"database_path": "/tmp/example.db"}, set_conn=False), DatabaseDriverAPI
    )
    factory("metadata")  # expect-error: reportArgumentType arg-type
    metadata_sql.lock_agent_rows(
        object(),  # expect-error: reportArgumentType arg-type
        [1],
    )


def custom_column_service_contracts(
    db: DatabaseAPI, cache: CustomColumnCache, connection: SQLConnection
) -> None:
    """Require real cache operations and preserve ID/row/definition result shapes."""
    from LiuXin_alpha.catalog.api.field_metadata_api import FieldMetadataAPI
    from LiuXin_alpha.catalog.field_metadata import FieldMetadata
    from LiuXin_alpha.databases.custom_columns import CustomColumns

    metadata: FieldMetadataAPI = FieldMetadata()
    facade = CustomColumns(
        db,
        conn=connection,
        data=cache,
        field_metadata=metadata,
        field_map={"id": 0, 3: 1},
    )
    service: CustomColumnsAPI = facade
    facade.notify = lambda event, ids: None
    facade.dirtied = lambda ids, commit=True: None
    assert_type(service.data, CustomColumnCache)
    assert_type(service.get_custom_items_with_ids(num=3), list[tuple[int, Any]])
    assert_type(
        service.custom_dirty_books_referencing("#shelf", 7), list[Sequence[Any]]
    )
    assert_type(service.set_custom_column_metadata(3, display={"is_names": True}), bool)
    service.set_custom_bulk_multiple(cc_row_ids=iter((7, 8)), add=[b"new"], num=3)
    service.set_custom_bulk(
        cc_row_ids=[7, 8], val="Cycle", num=3, extras={0: 1.0, 1: 2.0}
    )
    service.rename_custom_item_in_data(
        book_ids=[(7,), (8,)], column_num=3, new_value=None
    )
    facade.data = None
    CustomColumns(db, data={})  # expect-error: reportArgumentType arg-type
    CustomColumns(db, conn=object())  # expect-error: reportArgumentType arg-type
    service.rename_custom_item_in_data(
        book_ids=[7],  # expect-error: reportArgumentType list-item
        column_num=3,
        new_value=None,
    )
    service.set_custom_column_metadata(
        3,
        display="{}",  # expect-error: reportArgumentType arg-type
    )
    service.data.set(
        row="7",  # expect-error: reportArgumentType arg-type
        col=1,
        val=None,
    )


def backend_macro_and_cursor_contracts(
    db: DatabaseAPI, adapter: PostgresConnectionAdapter
) -> None:
    """Generic backends do not promise legacy SQL or positional custom rows."""
    from psycopg2.extras import RealDictCursor

    from LiuXin_alpha.databases.api.portable_macros_api import DatabaseMacrosAPI
    from LiuXin_alpha.databases.database_driver_plugins._macro_state import (
        require_legacy_macros,
    )
    from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.connection import (
        with_postgres_connection,
    )
    from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.databasedriver import (
        PostgresDatabaseMacros,
    )

    provider: DatabaseMacrosAPI = PostgresDatabaseMacros(None)
    assert_type(db.macros, DatabaseMacrosAPI)
    assert_type(provider.get_link_data("works", "agents", 7), LegacyLinkData)
    legacy = require_legacy_macros(db.macros)
    assert_type(legacy, MacrosAPI)
    legacy.mark_custom_column_for_delete(num=3)
    db.macros.mark_custom_column_for_delete(  # expect-error: reportAttributeAccessIssue attr-defined
        num=3
    )
    positional = adapter.cursor()
    assert_type(positional.fetchone(), Sequence[Any] | None)
    assert_type(positional.fetchall(), list[Sequence[Any]])
    selected = adapter.cursor(cursor_factory=RealDictCursor)
    assert_type(selected.fetchone(), SQLRow | None)
    assert_type(selected.fetchall(), list[SQLRow])
    assert_type(adapter.get_row("SELECT 1", all=False), Sequence[Any] | None)
    assert_type(with_postgres_connection("probe", lambda cursor: 7), int)
    adapter.cursor(  # expect-error: reportCallIssue call-overload
        cursor_factory=object()  # expect-error: reportArgumentType -
    )
    positional.fetchmany("10")  # expect-error: reportArgumentType arg-type
    adapter.execute("SELECT 1", object())  # expect-error: reportArgumentType arg-type
    db.set_macros(object())  # expect-error: reportArgumentType arg-type


def search_registration_contracts(connection: sqlite3.Connection) -> None:
    """Search-index setup needs function registration, not arbitrary SQL execution."""
    from LiuXin_alpha.databases.search_functions import register_search_functions

    register_search_functions(connection)
    register_search_functions(object())  # expect-error: reportArgumentType arg-type


def sqlite_result_container_contracts() -> None:
    """Native row factories are dynamic; an all-row fetch still returns a list."""
    from LiuXin_alpha.databases.database_driver_plugins.SQLite.databasedriver import (
        SQLite_Connection as SQLiteConnection,
    )
    from LiuXin_alpha.databases.database_driver_plugins.SQLite_apsw.databasedriver import (
        SQLite_Connection as CompatibilityConnection,
    )

    with SQLiteConnection(":memory:") as sqlite:
        assert_type(sqlite.get("SELECT 1"), list[Any])
        assert_type(sqlite.get_row("SELECT 1"), list[Any])
    with CompatibilityConnection(":memory:") as compatibility:
        assert_type(compatibility.get("SELECT 1"), list[Any])
        assert_type(compatibility.get_row("SELECT 1"), list[Any])
