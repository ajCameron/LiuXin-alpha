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
from collections.abc import Iterator, Mapping
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
    SQLRow,
)
from LiuXin_alpha.databases.db_types.query_observation import QueryObservation
from LiuXin_alpha.databases.driver_wrapper import DriverWrapper
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
