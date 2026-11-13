"""Structural native custom-field contract shared by service components."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from contextlib import AbstractContextManager
from typing import Any, Protocol

from LiuXin_alpha.databases.api.database_api.database_api import DatabaseAPI
from LiuXin_alpha.databases.custom_columns.native_repository import (
    CustomFieldRepository,
)
from LiuXin_alpha.databases.custom_columns.native_types import (
    ChangePlan,
    CollectionMode,
    FieldDefinition,
    FieldPatch,
    FieldRef,
    Issue,
    OwnerRef,
    Page,
    Predicate,
    ValueEntry,
    WriteReceipt,
)
from LiuXin_alpha.databases.db_types.driver_types import SQLConnection


class CustomFieldHost(Protocol):
    """Structural storage and transaction contract used by native service mixins."""

    db: DatabaseAPI
    repository: CustomFieldRepository

    def read(self) -> AbstractContextManager[SQLConnection]: ...
    def write(self) -> AbstractContextManager[SQLConnection]: ...
    def subscribe(
        self, listener: Callable[[WriteReceipt], None]
    ) -> Callable[[], None]: ...
    def _publish(self, receipt: WriteReceipt) -> None: ...
    def list_fields(
        self, *, attachment: str | None = None, include_retired: bool = False
    ) -> tuple[FieldDefinition, ...]: ...
    def get_field(
        self, field: FieldRef, *, include_retired: bool = False
    ) -> FieldDefinition: ...
    def create_field(
        self,
        attachment: str,
        label: str,
        *,
        name: str | None = None,
        datatype: str = "text",
        mode: CollectionMode = "scalar",
        editable: bool = True,
        display: Mapping[str, Any] | None = None,
    ) -> FieldDefinition: ...
    def _validate_keys(
        self, conn: SQLConnection, definition: FieldDefinition
    ) -> None: ...
    def get_values(
        self, owners: Iterable[OwnerRef], fields: Iterable[FieldRef]
    ) -> dict[OwnerRef, dict[int, Any]]: ...
    @staticmethod
    def _value(definition: FieldDefinition, values: list[ValueEntry]) -> Any: ...
    def get_value(self, owner: OwnerRef, field: FieldRef) -> Any: ...
    def get_owner_values(self, owner: OwnerRef) -> dict[int, Any]: ...
    def apply_patch(self, patches: Iterable[FieldPatch]) -> WriteReceipt: ...
    def set_value(
        self,
        owner: OwnerRef,
        field: FieldRef,
        value: Any,
        *,
        expected_revision: int | None = None,
    ) -> WriteReceipt: ...
    def edit_entries(
        self,
        owner: OwnerRef,
        field: FieldRef,
        *,
        add: Iterable[Any] = (),
        update: Mapping[int, Any] | None = None,
        remove: Iterable[int] = (),
        order: Iterable[int] | None = None,
    ) -> WriteReceipt: ...
    def retire_field(self, field: FieldRef) -> WriteReceipt: ...
    def _require_no_dependents(
        self, conn: SQLConnection, definition: FieldDefinition
    ) -> None: ...
    def audit(self) -> tuple[Issue, ...]: ...
    def _check_plan(self, conn: SQLConnection, plan: ChangePlan, kind: str) -> None: ...
    def _rebuild(
        self,
        conn: SQLConnection,
        old: FieldDefinition,
        new: FieldDefinition,
        stored: dict[int, list[ValueEntry]],
    ) -> None: ...
    def _update_definition(self, conn: SQLConnection, d: FieldDefinition) -> None: ...
    def plan_field_migration(
        self,
        field: FieldRef,
        changes: Mapping[str, Any],
        *,
        owner_mapping: Mapping[int, int] | None = None,
        value_mapping: Mapping[Any, Any] | None = None,
        reduce: str | None = None,
        collapse: bool = False,
    ) -> ChangePlan: ...
    def _convert(
        self,
        conn: SQLConnection,
        old: FieldDefinition,
        new: FieldDefinition,
        payload: dict[str, Any],
    ) -> dict[int, list[ValueEntry]]: ...
    def apply_field_migration(self, plan: ChangePlan) -> WriteReceipt: ...
    def plan_purge(self, field: FieldRef) -> ChangePlan: ...
    def apply_purge(self, plan: ChangePlan) -> None: ...
    def plan_repair(
        self, *, actions: tuple[str, ...] = ("cleanup_trigger", "ordering")
    ) -> ChangePlan: ...
    def apply_repair(self, plan: ChangePlan) -> None: ...
    def find_owners(
        self,
        attachment: str,
        predicates: Iterable[Predicate] = (),
        *,
        limit: int = 100,
        after_id: int = 0,
    ) -> Page: ...
    def inventory(
        self,
        field: FieldRef,
        *,
        limit: int = 100,
        after_id: int = 0,
        unused: bool = False,
    ) -> Page: ...
    def usage_count(self, field: FieldRef, value: Any) -> int: ...
    def is_shared(self, field: FieldRef, value: Any) -> bool: ...
    def next_series_position(self, field: FieldRef, series: str) -> float: ...

    def rename_value(
        self, field: FieldRef, value_id: int, new_value: Any
    ) -> WriteReceipt: ...
    def delete_value(self, field: FieldRef, value_id: int) -> WriteReceipt: ...
