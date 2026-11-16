"""Native custom-field transport with explicit typed values and fresh plan validation."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import fields, is_dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any

from LiuXin_alpha.core.errors import CoreDispatchError
from LiuXin_alpha.core.program_services.payloads import _payload

if TYPE_CHECKING:
    from LiuXin_alpha.core.commands import CoreCommand
    from LiuXin_alpha.core.queries import CoreQuery
    from LiuXin_alpha.core.runtime import CoreRuntime
    from LiuXin_alpha.databases.custom_columns.native_service import CustomFieldService
    from LiuXin_alpha.databases.custom_columns.native_types import (
        ChangePlan,
        FieldRef,
        OwnerRef,
        WriteReceipt,
    )


def wire(value: Any) -> Any:
    """Keep structured entry IDs, explicit dates, and owner keys transport safe."""
    if isinstance(value, datetime):
        return {"$datetime": value.isoformat()}
    if is_dataclass(value) and not isinstance(value, type):
        return {f.name: wire(getattr(value, f.name)) for f in fields(value)}
    if isinstance(value, dict):
        return {str(k): wire(v) for k, v in value.items()}
    if isinstance(value, (tuple, list)):
        return [wire(v) for v in value]
    return value


def unwire(value: Any) -> Any:
    """Decode only documented native value tags; ordinary strings remain strings."""
    from LiuXin_alpha.databases.custom_columns.native_types import (
        SeriesValue,
        ValueEntry,
    )

    if isinstance(value, list):
        return [unwire(v) for v in value]
    if not isinstance(value, dict):
        return value
    if set(value) == {"$datetime"}:
        return datetime.fromisoformat(value["$datetime"])
    if set(value) <= {"value", "position"} and "value" in value:
        return SeriesValue(value["value"], value.get("position", 1.0))
    if (
        {"id", "value", "ordinal"}
        <= set(value)
        <= {"id", "value", "ordinal", "position", "value_id"}
    ):
        return ValueEntry(**{k: unwire(v) for k, v in value.items()})
    raise CoreDispatchError("Unrecognized structured custom-field value")


def owner(value: Any) -> OwnerRef:
    from LiuXin_alpha.databases.custom_columns.native_types import OwnerRef

    if not isinstance(value, dict) or set(value) != {"table", "id"}:
        raise CoreDispatchError("Owner requires table and integer id")
    if not isinstance(value["table"], str) or type(value["id"]) is not int:
        raise CoreDispatchError("Invalid owner identity")
    return OwnerRef(**value)


def reference(value: Any) -> FieldRef:
    if type(value) is int:
        return value
    if (
        isinstance(value, (tuple, list))
        and len(value) == 2
        and all(isinstance(v, str) for v in value)
    ):
        return (value[0], value[1])
    raise CoreDispatchError("Field requires an integer ID or [attachment, label]")


def _plan(service: CustomFieldService, payload: dict[str, Any]) -> ChangePlan:
    kind = payload["kind"]
    planners = {
        "purge": lambda: service.plan_purge(reference(payload["field"])),
        "repair": lambda: service.plan_repair(
            actions=tuple(payload.get("actions", ["cleanup_trigger", "ordering"]))
        ),
        "migration": lambda: service.plan_field_migration(
            reference(payload["field"]),
            payload["changes"],
            owner_mapping=payload.get("owner_mapping"),
            value_mapping={
                unwire(k): unwire(v) for k, v in payload.get("value_mapping", [])
            },
            reduce=payload.get("reduce"),
            collapse=payload.get("collapse", False),
        ),
    }
    if kind not in planners:
        raise CoreDispatchError("Unknown custom-field plan kind")
    return planners[kind]()


def _many(service: CustomFieldService, payload: dict[str, Any]) -> list[dict[str, Any]]:
    result = service.get_values(
        [owner(v) for v in payload["owners"]], [reference(v) for v in payload["fields"]]
    )
    return [{"owner": wire(o), "values": wire(values)} for o, values in result.items()]


def native_fields_query(runtime: CoreRuntime, query: CoreQuery) -> Any:
    """Execute an allowlisted native read or preview; construction never upgrades.

    :param runtime: Runtime providing the borrowed database for a consistent read.
    :param query: Named native query with structured owner, field, value or plan intent.
    :return: Transport-safe definitions, values, pages, audit issues or a change preview.
    """
    payload = _payload(query)
    from LiuXin_alpha.databases.custom_columns.native_service import CustomFieldService
    from LiuXin_alpha.databases.custom_columns.native_types import Predicate

    service = CustomFieldService(runtime.database)
    from LiuXin_alpha.catalog.custom_fields import CustomFieldEvaluator

    evaluator = CustomFieldEvaluator(service)
    handlers: dict[str, Callable[[], Any]] = {
        "definitions": lambda: service.list_fields(**payload),
        "value": lambda: evaluator.get_value(
            owner(payload["owner"]), reference(payload["field"])
        ),
        "values": lambda: _many(service, payload),
        "owner": lambda: evaluator.get_owner_values(owner(payload["owner"])),
        "inventory": lambda: service.inventory(
            reference(payload["field"]),
            **{k: v for k, v in payload.items() if k != "field"},
        ),
        "usage": lambda: service.usage_count(
            reference(payload["field"]), unwire(payload["value"])
        ),
        "find": lambda: service.find_owners(
            payload["attachment"],
            [
                Predicate(
                    reference(p["field"]),
                    p["operator"],
                    unwire(p.get("value")),
                    p.get("quantifier", "any"),
                )
                for p in payload.get("predicates", [])
            ],
            limit=payload.get("limit", 100),
            after_id=payload.get("after_id", 0),
        ),
        "next-series": lambda: service.next_series_position(
            reference(payload["field"]), payload["series"]
        ),
        "audit": service.audit,
        "plan": lambda: _plan(service, payload),
    }
    operation = query.name.removeprefix("custom-fields.native.")
    if operation not in handlers:
        raise CoreDispatchError("Unknown native custom-field query")
    return wire(handlers[operation]())


def _apply(service: CustomFieldService, payload: dict[str, Any]) -> Any:
    # Reconstruct the preview from intent, never deserialize caller-supplied plans.
    from LiuXin_alpha.databases.custom_columns.native_types import StalePlan

    plan = _plan(service, payload)
    if plan.fingerprint != payload["fingerprint"]:
        raise StalePlan("The preview is stale; request a fresh plan")
    handlers = {
        "migration": service.apply_field_migration,
        "purge": service.apply_purge,
        "repair": service.apply_repair,
    }
    return handlers[plan.kind](plan)


def _entries(service: CustomFieldService, payload: dict[str, Any]) -> WriteReceipt:
    return service.edit_entries(
        owner(payload["owner"]),
        reference(payload["field"]),
        add=unwire(payload.get("add", [])),
        update={int(k): unwire(v) for k, v in payload.get("update", {}).items()},
        remove=payload.get("remove", []),
        order=payload.get("order"),
    )


def native_fields_command(runtime: CoreRuntime, command: CoreCommand) -> Any:
    """Execute one atomic native mutation; schema changes require admin routes.

    :param runtime: Runtime providing the database and shared field metadata projection.
    :param command: Named native command with structured values or reviewed migration intent.
    :return: Transport-safe write receipt, definition or plan-application result.
    """
    payload = _payload(command)
    from LiuXin_alpha.caches.custom_fields import CustomFieldCache
    from LiuXin_alpha.databases.custom_columns.native_service import CustomFieldService
    from LiuXin_alpha.databases.custom_columns.native_types import FieldPatch

    service = CustomFieldService(runtime.database)
    projection = CustomFieldCache(service, runtime.services.field_metadata)
    handlers: dict[str, Callable[[], Any]] = {
        "create": lambda: service.create_field(**payload),
        "patch": lambda: service.apply_patch(
            [
                FieldPatch(
                    owner(p["owner"]),
                    reference(p["field"]),
                    unwire(p["value"]),
                    p.get("expected_revision"),
                )
                for p in payload["patches"]
            ]
        ),
        "entries": lambda: _entries(service, payload),
        "rename-value": lambda: service.rename_value(
            reference(payload["field"]), payload["value_id"], unwire(payload["value"])
        ),
        "delete-value": lambda: service.delete_value(
            reference(payload["field"]), payload["value_id"]
        ),
        "retire": lambda: service.retire_field(reference(payload["field"])),
        "apply": lambda: _apply(service, payload),
    }
    operation = command.name.removeprefix("custom-fields.native.")
    if operation not in handlers:
        raise CoreDispatchError("Unknown native custom-field command")
    with projection.write():
        result = handlers[operation]()
    return wire(result)
