"""Static-only positive and negative examples for internal call contracts.

Never execute these functions. Each ``expect-error`` line must produce the
named basedpyright and mypy diagnostics, respectively. All other lines must
type-check, including calls that use the actual composed implementations.
"""

from collections.abc import Mapping
from contextlib import AbstractContextManager
from typing import assert_type

import LiuXin_alpha.storage.api as storage
from LiuXin_alpha.core.commands import CoreCommand
from LiuXin_alpha.core.program_api import CoreProgramAPI
from LiuXin_alpha.core.program_services.evacuation_execution import (
    EvacuationExecution,
    execute_evacuation,
)
from LiuXin_alpha.core.program_services.evacuation_models import EvacuationLimits, EvacuationPlan
from LiuXin_alpha.core.program_services.evacuation_planning import build_evacuation_plan
from LiuXin_alpha.core.program_endpoints import install_program_endpoints
from LiuXin_alpha.core.program_endpoints.common import ProgramEndpointRegistrar
from LiuXin_alpha.core.program_endpoints.handlers import ProgramEndpointHandlers
from LiuXin_alpha.core.program_endpoints.storage import install_queries
from LiuXin_alpha.core.queries import CoreQuery
from LiuXin_alpha.core.runtime import CoreRuntime
from LiuXin_alpha.storage.storage_manager.manager import TransientStorageManager
from LiuXin_alpha.storage.storage_manager.mixins._state import _StorageManagerState
from LiuXin_alpha.surfaces.acquisition_types import AcquisitionReader, CoreStoredFile
from LiuXin_alpha.surfaces.core import CoreRow, CoreSurfaceModel
from LiuXin_alpha.surfaces.presentation import RowLookup, row_value


def valid_composition(runtime: CoreRuntime) -> None:
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
    assert_type(manager._new_revision_locked(), str)
    assert_type(manager._metadata_transaction(), AbstractContextManager[None])
    assert_type(
        manager._find_asset_locked((), 0), storage.DigitalAssetRecord | None
    )
    assert_type(
        handlers.storage_store_get(runtime, CoreQuery("storage.store.get")),
        Mapping[str, object],
    )
    registrar.register_query_handler("store.get", handlers.storage_store_get)
    registrar.register_command_handler("store.probe", handlers.storage_store_probe)
    install_program_endpoints(handlers, registrar)


def bad_storage_calls(manager: _StorageManagerState) -> int:
    manager._metadata_transactoin()  # expect-error: reportAttributeAccessIssue attr-defined
    manager._find_asset_locked((), "zero")  # expect-error: reportArgumentType arg-type
    manager._new_revision_locked("unexpected")  # expect-error: reportCallIssue call-arg
    manager._allocate_metadata_id_locked("typo")  # expect-error: reportArgumentType arg-type
    return manager._new_revision_locked()  # expect-error: reportReturnType return-value


def bad_endpoint_calls(
    handlers: ProgramEndpointHandlers,
    registrar: ProgramEndpointRegistrar,
    runtime: CoreRuntime,
) -> None:
    handlers.storage_store_proeb(runtime, CoreCommand("probe"))  # expect-error: reportAttributeAccessIssue attr-defined
    handlers.storage_store_get(runtime, CoreCommand("get"))  # expect-error: reportArgumentType arg-type
    registrar.register_query_handler("probe", handlers.storage_store_probe)  # expect-error: reportArgumentType arg-type
    registrar.register_command_handler("get", handlers.storage_store_get)  # expect-error: reportArgumentType arg-type
    registrar.register_query_handler(42, handlers.storage_store_get)  # expect-error: reportArgumentType arg-type
    registrar.register_query_handler("get", handlers.storage_store_get, unexpected=True)  # expect-error: reportCallIssue call-arg
    registrar.register_query_handler("missing")  # expect-error: reportCallIssue call-arg
    install_program_endpoints(object(), registrar)  # expect-error: reportArgumentType arg-type
    install_program_endpoints(handlers, object())  # expect-error: reportArgumentType arg-type
    install_queries(object(), registrar)  # expect-error: reportArgumentType arg-type
    install_queries(handlers, object())  # expect-error: reportArgumentType arg-type


class IncorrectStorageHelper(TransientStorageManager):
    def _new_revision_locked(self) -> int:  # expect-error: reportIncompatibleMethodOverride override
        return 1


class IncorrectProgramHandler(CoreProgramAPI):
    @classmethod
    def storage_store_get(cls, runtime: CoreRuntime, query: CoreQuery) -> str:  # expect-error: reportIncompatibleMethodOverride override
        return "not a record"


def reject_incorrect_implementations(
    api: IncorrectProgramHandler,
    registrar: ProgramEndpointRegistrar,
) -> None:
    install_program_endpoints(api, registrar)  # expect-error: reportArgumentType arg-type


def evacuation_contracts(manager: storage.StorageManagerAPI, plan: EvacuationPlan) -> None:
    limits = EvacuationLimits(10, 1024)
    assert_type(execute_evacuation(manager, plan, limits, keep_source_bytes=True), EvacuationExecution)
    execute_evacuation(manager, {}, limits, keep_source_bytes=True)  # expect-error: reportArgumentType arg-type
    execute_evacuation(manager, plan, {"max_actions": 10}, keep_source_bytes=True)  # expect-error: reportArgumentType arg-type
    build_evacuation_plan(manager, source_ref="not a UUID", destination_ref=None, max_assets=10)  # expect-error: reportArgumentType arg-type


def surface_contracts(model: CoreSurfaceModel, row: CoreRow, reader: AcquisitionReader) -> None:
    actual_reader: AcquisitionReader = model
    actual_row: RowLookup = row
    assert_type(CoreStoredFile(actual_reader, "file", 7).read_bytes(), bytes)
    assert_type(row_value(actual_row, "title"), object)
    assert_type(row_value({"title": "雪"}, "title"), object)
    CoreStoredFile(object(), "file", 7)  # expect-error: reportArgumentType arg-type
    CoreStoredFile(reader, "file", "seven")  # expect-error: reportArgumentType arg-type
    row_value(object(), "title")  # expect-error: reportArgumentType arg-type
