"""Transport-safe managed-storage graph operations for Core.

The database schema is intentionally hidden behind a closed resource registry.
This gives program clients complete asset/replica/policy/workflow persistence
without exposing arbitrary table access as part of the stable Core contract.
"""

# pyright: reportImportCycles=false

from __future__ import annotations

import dataclasses
from collections.abc import Iterable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, cast

from LiuXin_alpha.core.description import CorePayloadFieldDescription
from LiuXin_alpha.core.errors import CoreDispatchError

if TYPE_CHECKING:
    from LiuXin_alpha.core.commands import CoreCommand
    from LiuXin_alpha.core.queries import CoreQuery
    from LiuXin_alpha.core.runtime import CoreRuntime


@dataclasses.dataclass(frozen=True, slots=True)
class _ResourceSpec:
    name: str
    table: str
    id_column: str
    prefix: str
    kind: str
    writable: bool = True


_RESOURCE_SPECS = tuple(
    _ResourceSpec(*values)
    for values in (
        (
            "asset",
            "digital_assets",
            "digital_asset_id",
            "digital_asset_",
            "asset",
            True,
        ),
        (
            "composite",
            "composite_digital_assets",
            "composite_digital_asset_id",
            "composite_digital_asset_",
            "asset",
            True,
        ),
        (
            "replica",
            "asset_replicas",
            "asset_replica_id",
            "asset_replica_",
            "replica",
            True,
        ),
        (
            "asset-item-link",
            "digital_asset_item_links",
            "digital_asset_item_link_id",
            "digital_asset_item_link_",
            "relationship",
            True,
        ),
        (
            "composite-item-link",
            "composite_digital_asset_item_links",
            "composite_digital_asset_item_link_id",
            "composite_digital_asset_item_link_",
            "relationship",
            True,
        ),
        (
            "composite-member-link",
            "composite_digital_asset_digital_asset_links",
            "composite_digital_asset_digital_asset_link_id",
            "composite_digital_asset_digital_asset_link_",
            "relationship",
            True,
        ),
        (
            "replication-policy",
            "replication_policies",
            "replication_policy_id",
            "replication_policy_",
            "policy",
            True,
        ),
        (
            "backup-policy",
            "backup_policies",
            "backup_policy_id",
            "backup_policy_",
            "policy",
            True,
        ),
        (
            "backup-workflow",
            "backup_workflows",
            "backup_workflow_id",
            "backup_workflow_",
            "workflow",
            True,
        ),
        (
            "backup-workflow-source",
            "backup_workflow_sources",
            "backup_workflow_source_id",
            "backup_workflow_source_",
            "workflow",
            True,
        ),
        (
            "backup-workflow-state",
            "backup_workflow_state",
            "backup_workflow_state_id",
            "backup_workflow_state_",
            "workflow-state",
            False,
        ),
        (
            "backup-workflow-output",
            "backup_workflow_outputs",
            "backup_workflow_output_id",
            "backup_workflow_output_",
            "workflow-state",
            False,
        ),
        (
            "backup-presence",
            "backup_presence_links",
            "backup_presence_link_id",
            "backup_presence_link_",
            "workflow-state",
            False,
        ),
    )
)
_RESOURCES = {spec.name: spec for spec in _RESOURCE_SPECS}


def _field(
    name: str,
    *,
    required: bool = False,
    field_type: str | None = None,
) -> CorePayloadFieldDescription:
    return CorePayloadFieldDescription(
        name=name,
        required=required,
        field_type=field_type,
    )


def _payload(envelope: Any) -> dict[str, Any]:
    raw = getattr(envelope, "payload", None)
    if raw is None:
        return {}
    if not isinstance(raw, Mapping):
        raise CoreDispatchError("Core payload must be an object.")
    return dict(raw)


def _required_text(payload: Mapping[str, Any], name: str) -> str:
    value = str(payload.get(name, "")).strip()
    if not value:
        raise CoreDispatchError("`{}` is required.".format(name))
    return value


def _required_int(payload: Mapping[str, Any], name: str) -> int:
    value = payload.get(name)
    if value is None or isinstance(value, bool):
        raise CoreDispatchError("`{}` must be an integer.".format(name))
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise CoreDispatchError(
            "`{}` must be an integer.".format(name)
        ) from exc


def _mapping(payload: Mapping[str, Any], name: str) -> dict[str, Any]:
    value = payload.get(name)
    if not isinstance(value, Mapping):
        raise CoreDispatchError("`{}` must be an object.".format(name))
    return dict(value)


def _spec(payload: Mapping[str, Any]) -> _ResourceSpec:
    resource = _required_text(payload, "resource").lower()
    try:
        return _RESOURCES[resource]
    except KeyError as exc:
        raise CoreDispatchError(
            "Unknown storage resource `{}`.".format(resource),
            code="unknown_storage_resource",
            details={"resource": resource, "available": sorted(_RESOURCES)},
        ) from exc


def _macros(runtime: "CoreRuntime") -> Any:
    macros = getattr(runtime.database, "macros", None)
    required = ("get_row", "get_rows", "insert_row", "update_row", "delete_row")
    if macros is None or any(
        not callable(getattr(macros, name, None))
        for name in required
    ):
        raise CoreDispatchError(
            "The local database does not provide portable storage persistence.",
            code="capability_unavailable",
            details={"area": "storage", "operation": "resource persistence"},
        )
    return macros


def _headings(runtime: "CoreRuntime", spec: _ResourceSpec) -> set[str]:
    for target in (runtime.database, getattr(runtime.database, "driver_wrapper", None)):
        method = getattr(target, "get_column_headings", None)
        if callable(method):
            try:
                headings = cast(Iterable[object], method(spec.table))
                return {str(value) for value in headings}
            except Exception:
                continue
    return set()


def _column_name(
    spec: _ResourceSpec,
    key: str,
    *,
    headings: set[str],
) -> str:
    token = str(key).strip()
    if not token:
        raise CoreDispatchError("Storage resource field names may not be empty.")
    if token == "id":
        return spec.id_column
    if token == spec.id_column or token.startswith(spec.prefix):
        candidate = token
    else:
        candidate = "{}{}".format(spec.prefix, token)
    if headings and candidate not in headings:
        raise CoreDispatchError(
            "Unknown `{}` field `{}`.".format(spec.name, token),
            code="unknown_storage_field",
            details={"resource": spec.name, "field": token},
        )
    return candidate


def _normalise_values(
    runtime: "CoreRuntime",
    spec: _ResourceSpec,
    values: Mapping[str, Any],
    *,
    allow_id: bool = False,
) -> dict[str, Any]:
    headings = _headings(runtime, spec)
    normalised: dict[str, Any] = {}
    for key, value in values.items():
        column = _column_name(spec, str(key), headings=headings)
        if column == spec.id_column and not allow_id:
            raise CoreDispatchError(
                "`id` is managed by Core and may not be written."
            )
        if column.endswith(("_created_timestamp_ep_k", "_modified_timestamp_ep_k")):
            raise CoreDispatchError(
                "Core manages storage resource timestamps."
            )
        normalised[column] = value
    return normalised


def _record(spec: _ResourceSpec, row: Mapping[str, Any]) -> dict[str, Any]:
    raw = dict(row)
    values: dict[str, Any] = {}
    for key, value in raw.items():
        if key == spec.id_column:
            continue
        if key.startswith(spec.prefix):
            values[key[len(spec.prefix) :]] = value
        else:
            values[key] = value
    return {
        "resource": spec.name,
        "id": raw.get(spec.id_column),
        "values": values,
    }


def _healthy_replica(row: Mapping[str, Any]) -> bool:
    presence = str(row.get("asset_replica_presence_status") or "").casefold()
    integrity = str(row.get("asset_replica_integrity_status") or "").casefold()
    return presence not in {"missing", "offline", "deleted"} and integrity not in {
        "bad",
        "corrupt",
        "failed",
    }


class CoreStorageGraphAPI:
    """Install the storage resource and policy API facets."""

    def install(self, runtime: "CoreRuntime") -> None:
        query = runtime.register_query_handler
        command = runtime.register_command_handler

        query(
            "storage.resources.describe",
            self.resources_describe,
            summary="Describe the closed managed-storage resource registry.",
            tags=("storage", "assets", "schema"),
        )
        query(
            "storage.resource.list",
            self.resource_list,
            summary="List one managed-storage resource type.",
            payload_fields=(
                _field("resource", required=True, field_type="string"),
                _field("where", field_type="object"),
                _field("limit", field_type="integer"),
                _field("offset", field_type="integer"),
            ),
            tags=("storage", "assets", "read"),
        )
        query(
            "storage.resource.get",
            self.resource_get,
            summary="Read one managed-storage resource.",
            payload_fields=(
                _field("resource", required=True, field_type="string"),
                _field("id", required=True, field_type="integer"),
            ),
            tags=("storage", "assets", "read"),
        )
        query(
            "storage.asset.get",
            self.asset_get,
            summary="Read one asset with replicas, item links, and policy status.",
            payload_fields=(_field("asset_id", required=True, field_type="integer"),),
            tags=("storage", "assets", "replicas", "read"),
        )
        query(
            "storage.policy.assess",
            self.policy_assess,
            summary="Assess one asset against its replication and backup policies.",
            payload_fields=(_field("asset_id", required=True, field_type="integer"),),
            tags=("storage", "policies", "read"),
        )
        query(
            "storage.policy.plan",
            self.policy_plan,
            summary="Plan additional store placements needed by one asset.",
            payload_fields=(_field("asset_id", required=True, field_type="integer"),),
            tags=("storage", "policies", "planning", "read"),
        )
        query(
            "storage.policy.violations",
            self.policy_violations,
            summary="List assets below replication or backup policy targets.",
            payload_fields=(
                _field("limit", field_type="integer"),
                _field("offset", field_type="integer"),
            ),
            tags=("storage", "policies", "read"),
        )

        command(
            "storage.resource.create",
            self.resource_create,
            summary="Create one managed-storage resource.",
            payload_fields=(
                _field("resource", required=True, field_type="string"),
                _field("values", required=True, field_type="object"),
            ),
            tags=("storage", "assets", "write"),
        )
        command(
            "storage.resource.update",
            self.resource_update,
            summary="Update one managed-storage resource.",
            payload_fields=(
                _field("resource", required=True, field_type="string"),
                _field("id", required=True, field_type="integer"),
                _field("values", required=True, field_type="object"),
            ),
            tags=("storage", "assets", "write"),
        )
        command(
            "storage.resource.delete",
            self.resource_delete,
            summary="Delete one mutable managed-storage resource.",
            payload_fields=(
                _field("resource", required=True, field_type="string"),
                _field("id", required=True, field_type="integer"),
            ),
            tags=("storage", "assets", "write"),
        )
        command(
            "storage.asset.policies.set",
            self.asset_policies_set,
            summary="Assign replication and backup policies to one asset.",
            payload_fields=(
                _field("asset_id", required=True, field_type="integer"),
                _field("replication_policy_id", field_type="integer|null"),
                _field("backup_policy_id", field_type="integer|null"),
            ),
            tags=("storage", "assets", "policies", "write"),
        )

    @staticmethod
    def resources_describe(
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        del query
        available = getattr(runtime.database, "macros", None) is not None
        return {
            "available": available,
            "resources": [
                {
                    "name": spec.name,
                    "kind": spec.kind,
                    "writable": spec.writable,
                }
                for spec in _RESOURCE_SPECS
            ],
        }

    @staticmethod
    def resource_list(
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        payload = _payload(query)
        spec = _spec(payload)
        where_raw = payload.get("where", {})
        if not isinstance(where_raw, Mapping):
            raise CoreDispatchError("`where` must be an object.")
        where = _normalise_values(runtime, spec, where_raw, allow_id=True)
        limit = max(0, min(int(payload.get("limit", 100)), 10_000))
        offset = max(0, int(payload.get("offset", 0)))
        rows = _macros(runtime).get_rows(
            spec.table,
            where=where or None,
            order_by=(spec.id_column,),
        )
        selected = rows[offset : offset + limit]
        return {
            "resource": spec.name,
            "records": [_record(spec, row) for row in selected],
            "offset": offset,
            "limit": limit,
            "complete": offset + len(selected) >= len(rows),
        }

    @staticmethod
    def resource_get(
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        payload = _payload(query)
        spec = _spec(payload)
        resource_id = _required_int(payload, "id")
        row = _macros(runtime).get_row(
            spec.table,
            resource_id,
            id_column=spec.id_column,
        )
        return {
            "resource": spec.name,
            "record": None if row is None else _record(spec, row),
        }

    def asset_get(
        self,
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        payload = _payload(query)
        asset_id = _required_int(payload, "asset_id")
        macros = _macros(runtime)
        asset_spec = _RESOURCES["asset"]
        row = macros.get_row(
            asset_spec.table,
            asset_id,
            id_column=asset_spec.id_column,
        )
        if row is None:
            return {"asset": None, "replicas": [], "item_links": [], "policy": None}
        replica_spec = _RESOURCES["replica"]
        link_spec = _RESOURCES["asset-item-link"]
        replicas = macros.get_rows(
            replica_spec.table,
            where={"asset_replica_digital_asset_id": asset_id},
            order_by=(replica_spec.id_column,),
        )
        links = macros.get_rows(
            link_spec.table,
            where={"digital_asset_item_link_digital_asset_id": asset_id},
            order_by=(link_spec.id_column,),
        )
        return {
            "asset": _record(asset_spec, row),
            "replicas": [_record(replica_spec, value) for value in replicas],
            "item_links": [_record(link_spec, value) for value in links],
            "policy": self._assess(runtime, asset_id),
        }

    @staticmethod
    def _assess(runtime: "CoreRuntime", asset_id: int) -> dict[str, Any]:
        macros = _macros(runtime)
        asset = macros.get_row(
            "digital_assets",
            asset_id,
            id_column="digital_asset_id",
        )
        if asset is None:
            raise CoreDispatchError(
                "Unknown digital asset {}.".format(asset_id),
                code="storage_asset_not_found",
                details={"asset_id": asset_id},
            )
        replicas = macros.get_rows(
            "asset_replicas",
            where={"asset_replica_digital_asset_id": asset_id},
            order_by=("asset_replica_id",),
        )
        healthy = [row for row in replicas if _healthy_replica(row)]
        active = [
            row
            for row in healthy
            if str(row.get("asset_replica_mode") or "active") == "active"
        ]
        backups = [
            row
            for row in healthy
            if str(row.get("asset_replica_mode") or "") in {"backup", "archive"}
        ]
        replication_policy_id = asset.get("digital_asset_replication_policy_id")
        backup_policy_id = asset.get("digital_asset_backup_policy_id")
        replication_policy = (
            None
            if replication_policy_id is None
            else macros.get_row(
                "replication_policies",
                replication_policy_id,
                id_column="replication_policy_id",
            )
        )
        backup_policy = (
            None
            if backup_policy_id is None
            else macros.get_row(
                "backup_policies",
                backup_policy_id,
                id_column="backup_policy_id",
            )
        )
        replication_min = int(
            (replication_policy or {}).get("replication_policy_min_copies", 1)
        )
        replication_target = int(
            (replication_policy or {}).get("replication_policy_target_copies")
            or replication_min
        )
        backup_min = int(
            (backup_policy or {}).get("backup_policy_min_backup_copies", 0)
        )
        backup_target = int(
            (backup_policy or {}).get("backup_policy_target_backup_copies")
            or backup_min
        )
        return {
            "asset_id": asset_id,
            "replication": {
                "policy_id": replication_policy_id,
                "minimum": replication_min,
                "target": replication_target,
                "healthy_copies": len(active),
                "meets_minimum": len(active) >= replication_min,
                "meets_target": len(active) >= replication_target,
            },
            "backup": {
                "policy_id": backup_policy_id,
                "minimum": backup_min,
                "target": backup_target,
                "healthy_copies": len(backups),
                "meets_minimum": len(backups) >= backup_min,
                "meets_target": len(backups) >= backup_target,
            },
            "replica_count": len(replicas),
            "healthy_replica_count": len(healthy),
        }

    def policy_assess(
        self,
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        return self._assess(runtime, _required_int(_payload(query), "asset_id"))

    def policy_plan(
        self,
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        payload = _payload(query)
        asset_id = _required_int(payload, "asset_id")
        assessment = self._assess(runtime, asset_id)
        macros = _macros(runtime)
        replicas = macros.get_rows(
            "asset_replicas",
            where={"asset_replica_digital_asset_id": asset_id},
            order_by=("asset_replica_id",),
        )
        occupied = {
            row.get("asset_replica_store_id")
            for row in replicas
            if row.get("asset_replica_store_id") is not None
        }
        stores = macros.get_rows("stores", order_by=("store_id",))
        candidates = [
            row
            for row in stores
            if row.get("store_id") not in occupied
            and not bool(row.get("store_is_read_only", False))
        ]

        def placements(family: str, mode: str) -> list[dict[str, Any]]:
            state = assessment[family]
            count = max(0, int(state["target"]) - int(state["healthy_copies"]))
            selected: list[dict[str, Any]] = []
            for store in candidates:
                capability = "store_supports_{}_replica_mode".format(mode)
                if store.get(capability, True) in {False, 0}:
                    continue
                selected.append(
                    {
                        "store_id": store.get("store_id"),
                        "store_name": store.get("store_name"),
                        "mode": mode,
                    }
                )
                if len(selected) >= count:
                    break
            return selected

        replication = placements("replication", "active")
        backup = placements("backup", "backup")
        return {
            "asset_id": asset_id,
            "assessment": assessment,
            "placements": replication + backup,
            "replication_shortfall": max(
                0,
                int(assessment["replication"]["target"])
                - int(assessment["replication"]["healthy_copies"])
                - len(replication),
            ),
            "backup_shortfall": max(
                0,
                int(assessment["backup"]["target"])
                - int(assessment["backup"]["healthy_copies"])
                - len(backup),
            ),
        }

    def policy_violations(
        self,
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        payload = _payload(query)
        limit = max(0, min(int(payload.get("limit", 100)), 10_000))
        offset = max(0, int(payload.get("offset", 0)))
        assets = _macros(runtime).get_rows(
            "digital_assets",
            order_by=("digital_asset_id",),
        )
        violations: list[dict[str, Any]] = []
        for asset in assets:
            asset_id = int(asset["digital_asset_id"])
            state = self._assess(runtime, asset_id)
            if (
                not state["replication"]["meets_minimum"]
                or not state["backup"]["meets_minimum"]
            ):
                violations.append(state)
        selected = violations[offset : offset + limit]
        return {
            "records": selected,
            "offset": offset,
            "limit": limit,
            "complete": offset + len(selected) >= len(violations),
        }

    @staticmethod
    def resource_create(
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        payload = _payload(command)
        spec = _spec(payload)
        if not spec.writable:
            raise CoreDispatchError(
                "`{}` is workflow-owned and read-only.".format(spec.name),
                code="storage_resource_read_only",
            )
        values = _normalise_values(runtime, spec, _mapping(payload, "values"))
        resource_id = _macros(runtime).insert_row(
            spec.table,
            values,
            id_column=spec.id_column,
        )
        row = _macros(runtime).get_row(
            spec.table,
            resource_id,
            id_column=spec.id_column,
        )
        return runtime.services.reconcile(
            {
                "resource": spec.name,
                "id": resource_id,
                "record": None if row is None else _record(spec, row),
                "created": True,
            }
        )

    @staticmethod
    def resource_update(
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        payload = _payload(command)
        spec = _spec(payload)
        if not spec.writable:
            raise CoreDispatchError(
                "`{}` is workflow-owned and read-only.".format(spec.name),
                code="storage_resource_read_only",
            )
        resource_id = _required_int(payload, "id")
        macros = _macros(runtime)
        if macros.get_row(
            spec.table,
            resource_id,
            id_column=spec.id_column,
        ) is None:
            raise CoreDispatchError(
                "Unknown {} {}.".format(spec.name, resource_id),
                code="storage_resource_not_found",
            )
        values = _normalise_values(runtime, spec, _mapping(payload, "values"))
        macros.update_row(
            spec.table,
            resource_id,
            values,
            id_column=spec.id_column,
        )
        row = macros.get_row(
            spec.table,
            resource_id,
            id_column=spec.id_column,
        )
        return runtime.services.reconcile(
            {
                "resource": spec.name,
                "id": resource_id,
                "record": None if row is None else _record(spec, row),
                "updated": True,
            }
        )

    @staticmethod
    def resource_delete(
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        payload = _payload(command)
        spec = _spec(payload)
        if not spec.writable:
            raise CoreDispatchError(
                "`{}` is workflow-owned and read-only.".format(spec.name),
                code="storage_resource_read_only",
            )
        resource_id = _required_int(payload, "id")
        macros = _macros(runtime)
        row = macros.get_row(
            spec.table,
            resource_id,
            id_column=spec.id_column,
        )
        if row is None:
            return {
                "resource": spec.name,
                "id": resource_id,
                "deleted": False,
            }
        macros.delete_row(
            spec.table,
            resource_id,
            id_column=spec.id_column,
        )
        return runtime.services.reconcile(
            {
                "resource": spec.name,
                "id": resource_id,
                "deleted": True,
                "record": _record(spec, row),
            }
        )

    @staticmethod
    def asset_policies_set(
        runtime: "CoreRuntime",
        command: "CoreCommand",
    ) -> dict[str, Any]:
        payload = _payload(command)
        asset_id = _required_int(payload, "asset_id")
        macros = _macros(runtime)
        asset = macros.get_row(
            "digital_assets",
            asset_id,
            id_column="digital_asset_id",
        )
        if asset is None:
            raise CoreDispatchError(
                "Unknown digital asset {}.".format(asset_id),
                code="storage_asset_not_found",
            )
        values: dict[str, Any] = {}
        for name, table, id_column in (
            (
                "replication_policy_id",
                "replication_policies",
                "replication_policy_id",
            ),
            ("backup_policy_id", "backup_policies", "backup_policy_id"),
        ):
            if name not in payload:
                continue
            policy_id = payload.get(name)
            if policy_id is not None:
                if isinstance(policy_id, bool):
                    raise CoreDispatchError("`{}` must be an integer or null.".format(name))
                policy_id = int(policy_id)
                if macros.get_row(
                    table,
                    policy_id,
                    id_column=id_column,
                ) is None:
                    raise CoreDispatchError(
                        "Unknown {} {}.".format(name, policy_id),
                        code="storage_policy_not_found",
                    )
            values["digital_asset_{}".format(name)] = policy_id
        if not values:
            raise CoreDispatchError(
                "Provide `replication_policy_id` or `backup_policy_id`."
            )
        macros.update_row(
            "digital_assets",
            asset_id,
            values,
            id_column="digital_asset_id",
        )
        return runtime.services.reconcile(
            {
                "asset_id": asset_id,
                "policies": {
                    key.removeprefix("digital_asset_"): value
                    for key, value in values.items()
                },
                "updated": True,
            }
        )


def install_storage_graph_api(runtime: "CoreRuntime") -> CoreStorageGraphAPI:
    """Register managed-storage graph operations on ``runtime``."""

    api = CoreStorageGraphAPI()
    api.install(runtime)
    return api


__all__ = ["CoreStorageGraphAPI", "install_storage_graph_api"]
