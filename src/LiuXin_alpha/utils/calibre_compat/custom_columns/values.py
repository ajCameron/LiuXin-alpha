"""Adapt Calibre value shapes while delegating every mutation to native storage."""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from typing import TYPE_CHECKING, Any

from LiuXin_alpha.databases.custom_columns.native_service import CustomFieldService
from LiuXin_alpha.databases.custom_columns.native_types import (
    FieldDefinition,
    FieldPatch,
    FieldRef,
    OwnerRef,
    SeriesValue,
)
from LiuXin_alpha.databases.custom_columns.native_values import identity
from LiuXin_alpha.databases.transaction_participants import after_commit
from LiuXin_alpha.utils.calibre_compat.custom_columns.errors import (
    CompatibilityError,
)

if TYPE_CHECKING:
    from LiuXin_alpha.utils.calibre_compat.custom_columns.facade import CustomColumns


def _require_delimiter_free(values: Iterable[str]) -> None:
    """Reject unrepresentable legacy text-set values before any native mutation."""
    if any("|" in value for value in values):
        raise CompatibilityError(
            "Delimiter-bearing collection values require the native API"
        )


def _project(definition: FieldDefinition, value: Any) -> tuple[Any, float | None]:
    if definition.mode == "scalar":
        return (
            (value.value, value.position)
            if isinstance(value, SeriesValue)
            else (value, None)
        )
    if definition.mode != "set" or definition.datatype != "text":
        raise CompatibilityError(
            "Use the native API for this collection or its Series positions"
        )
    values = [entry.value for entry in value]
    _require_delimiter_free(values)
    if definition.display.get("sort_alpha"):
        values.sort(key=str.casefold)
    return values, None


def _inventory(
    service: CustomFieldService, field: FieldRef
) -> Iterator[dict[str, Any]]:
    cursor = 0
    while True:
        page = service.inventory(field, limit=1000, after_id=cursor)
        yield from page.items
        if page.next_cursor is None:
            return
        cursor = page.next_cursor


def _dispatch(host: CustomColumns, name: str, values: dict[str, Any]) -> Any:
    metadata = host.custom_field_metadata(
        label=values.get("label"), num=values.get("num")
    )
    service = host.native
    definition = service.get_field(metadata["num"])
    if name.startswith("get_custom"):
        if name == "get_custom_items_with_ids":
            return [
                (item["id"], item["value"])
                for item in _inventory(service, definition.id)
            ]
        owner = values["idx"] if values.get("index_is_id") else host.id(values["idx"])
        if definition.datatype == "composite":
            raise CompatibilityError(
                "Computed native fields require the Catalog evaluator"
            )
        with service.read() as connection:
            stored = service.repository.read_entries(connection, definition, [owner])
            result = service._value(definition, stored.get(owner, []))
        projected, extra = _project(definition, result)
        if name == "get_custom_extra":
            return extra
        return (projected, extra) if name == "get_custom_and_extra" else projected
    if name == "get_next_cc_series_num_for":
        with service.read() as conn:
            positions = [
                (entry.position,)
                for group in service.repository.read_entries(conn, definition).values()
                for entry in group
                if entry.position is not None
                and identity(entry.value, definition)
                == identity(values["series"], definition)
            ]
        return host._get_next_series_num_for_list(sorted(positions))
    if not values.get("commit", True):
        raise ValueError(
            "set_custom(commit=False) is unsupported; use native managed transactions"
        )
    if definition.mode != "scalar":
        if definition.mode != "set" or definition.datatype != "text":
            raise CompatibilityError("Use native operations for this collection")
        _require_delimiter_free(
            item["value"] for item in _inventory(service, definition.id)
        )
    owners = []
    affected: set[int] = set()

    with service.write() as connection:
        if name in {"set_custom", "set_custom_bulk"}:
            owners = (
                [values["cc_row_id"]]
                if name == "set_custom"
                else list(values["cc_row_ids"])
            )
            supplied = host.custom_data_adapters[definition.datatype](
                values["val"], metadata
            )
            if (
                definition.datatype == "rating"
                and isinstance(supplied, float)
                and supplied.is_integer()
            ):
                supplied = int(supplied)
            patches = []
            for index, owner in enumerate(owners):
                value: Any = supplied
                extra = values.get("extra")
                if values.get("extras") is not None:
                    extra = values["extras"][index]
                if definition.datatype == "series" and value is not None:
                    value, parsed = host._get_series_values(value)
                    value = SeriesValue(
                        value,
                        extra
                        if extra is not None
                        else parsed
                        if parsed is not None
                        else 1.0,
                    )
                if definition.mode != "scalar":
                    if definition.mode != "set" or definition.datatype != "text":
                        raise CompatibilityError("Use native collection operations")
                    value = [] if value is None else value
                    if values.get("append"):
                        old = service.get_value(
                            OwnerRef(host.table, owner), definition.id
                        )
                        value = [*(e.value for e in old), *value]
                    _require_delimiter_free(value)
                patches.append(
                    FieldPatch(OwnerRef(host.table, owner), definition.id, value)
                )
            service.apply_patch(patches)
            if values.get("allow_case_change") and definition.datatype in {
                "text",
                "series",
            }:
                for patch in patches:
                    supplied_values = (
                        patch.value if definition.mode != "scalar" else [patch.value]
                    )
                    for item in supplied_values:
                        spelling = item.value if isinstance(item, SeriesValue) else item
                        if spelling is None:
                            continue
                        match = next(
                            v
                            for v in _inventory(service, definition.id)
                            if identity(v["value"], definition)
                            == identity(spelling, definition)
                        )
                        if match["value"] != spelling:
                            renamed = service.rename_value(
                                definition.id, match["id"], spelling
                            )
                            affected.update(o.id for o in renamed.owners)
        elif name == "set_custom_bulk_multiple":
            if definition.datatype != "text" or definition.mode != "set":
                raise CompatibilityError("Legacy bulk addition requires text sets")
            owners = list(values["cc_row_ids"])
            add = host.cleanup_tags(values.get("add") or [])
            _require_delimiter_free(add)
            remove = {
                identity(tag, definition)
                for tag in host.cleanup_tags(values.get("remove") or [])
            } - {identity(tag, definition) for tag in add}
            patches = []
            for owner in owners:
                old = service.get_value(OwnerRef(host.table, owner), definition.id)
                retained = [
                    e for e in old if identity(e.value, definition) not in remove
                ]
                patches.append(
                    FieldPatch(
                        OwnerRef(host.table, owner),
                        definition.id,
                        [*retained, *add],
                    )
                )
            service.apply_patch(patches)
        elif name == "rename_custom_item":
            if definition.mode != "scalar":
                _require_delimiter_free([values["new_name"]])
            receipt = service.rename_value(
                definition.id, values["old_id"], values["new_name"]
            )
            owners = [o.id for o in receipt.owners]
        elif name == "delete_custom_item_using_id":
            receipt = service.delete_value(definition.id, values["idx"])
            owners = [o.id for o in receipt.owners]
        elif name == "delete_item_from_multiple":
            if definition.datatype != "text" or definition.mode != "set":
                raise CompatibilityError("Legacy removal requires a text set")
            matching = [
                item
                for item in _inventory(service, definition.id)
                if identity(item["value"], definition)
                == identity(values["item"], definition)
            ]
            if matching:
                receipt = service.delete_value(definition.id, matching[0]["id"])
                owners = [o.id for o in receipt.owners]
        else:
            raise CompatibilityError(
                f"Use native operations for {name} on native storage"
            )
        touched = sorted(set(owners) | affected)
        host._refresh_custom_cache(metadata, touched)
        host.dirtied(touched, commit=False)
        after_commit(host.db.macros, host._prepare_custom_columns_refresh(connection))
        if values.get("notify", True):
            host.notify("metadata", touched)
    if name == "delete_item_from_multiple":
        return owners
    return affected if name == "set_custom" else None
