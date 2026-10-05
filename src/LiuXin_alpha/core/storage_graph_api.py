"""
Expose managed-storage database resources and row-based policy assessments through named Core routes.

A closed resource registry selects tables and public field prefixes instead of
accepting arbitrary table names. These are portable macro-backed row operations,
not the storage manager's byte-transfer, verified-observation, or placement-policy
workflows. Mutation receipts are reconciled after database calls; no transaction,
rollback, or authorization layer is added here. Outer Core dispatch performs wire
encoding for result values that remain backend-native at this boundary.
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
    """
    Map one public storage-resource name to its table, identity column, field prefix, and mutability category.

    The frozen record validates none of its fields. writable is enforced by the
    generic create/update/delete handlers, not a database permission or global
    prohibition on writes to workflow-owned tables.

    Example:
        >>> spec = _ResourceSpec("asset", "digital_assets", "digital_asset_id", "digital_asset_", "asset")
        >>> spec.writable, spec.id_column
        (True, 'digital_asset_id')
    """

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
    """
    Declare a public request field for endpoint introspection without checking request values.

    Example:
        >>> _field("resource", required=True, field_type="string").name
        'resource'


    :param name: Public payload key described by this declaration.
    :param required: Whether the advertised contract marks the field mandatory.
    :param field_type: Optional type label for clients, not an executable validator.
    :return: New field-description record retaining the supplied values.
    """
    return CorePayloadFieldDescription(
        name=name,
        required=required,
        field_type=field_type,
    )


def _payload(envelope: Any) -> dict[str, Any]:
    """
    Copy a Mapping payload shallowly, treating missing or None payload as an empty request.

    Example:
        >>> from types import SimpleNamespace
        >>> _payload(SimpleNamespace(payload={"resource": "asset"}))
        {'resource': 'asset'}


    :param envelope: Object whose optional payload attribute carries request data.
    :return: New dictionary retaining nested value references and original keys.
    :raises CoreDispatchError: If a non-None payload is not a Mapping.
    """
    raw = getattr(envelope, "payload", None)
    if raw is None:
        return {}
    if not isinstance(raw, Mapping):
        raise CoreDispatchError("Core payload must be an object.")
    return dict(raw)


def _required_text(payload: Mapping[str, Any], name: str) -> str:
    """
    Stringify and strip a required field, rejecting empty text but not non-string values.

    Explicit None becomes "None"; it is not treated as a missing field here.

    Example:
        >>> _required_text({"resource": " asset "}, "resource")
        'asset'


    :param payload: Request mapping containing the required key.
    :param name: Exact field name to retrieve and include in a missing-value error.
    :return: Nonempty stripped text, without validating resource membership.
    :raises CoreDispatchError: If the field is absent or converts to empty/whitespace-only text.
    """
    value = str(payload.get(name, "")).strip()
    if not value:
        raise CoreDispatchError("`{}` is required.".format(name))
    return value


def _required_int(payload: Mapping[str, Any], name: str) -> int:
    """
    Convert a required non-None, non-boolean field using int, without enforcing a positive range.

    Numeric fractions may truncate. Only TypeError/ValueError from conversion are
    wrapped; overflow and other custom conversion failures remain visible.

    Example:
        >>> _required_int({"id": "7"}, "id")
        7


    :param payload: Request mapping containing the integer-convertible field.
    :param name: Required key also used in the validation message.
    :return: Converted integer, including zero or negative values if supplied.
    :raises CoreDispatchError: For missing/None/bool values or conversion TypeError/ValueError.
    """
    value = payload.get(name)
    if value is None or isinstance(value, bool):
        raise CoreDispatchError("`{}` must be an integer.".format(name))
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise CoreDispatchError("`{}` must be an integer.".format(name)) from exc


def _mapping(payload: Mapping[str, Any], name: str) -> dict[str, Any]:
    """
    Require a Mapping field and return a shallow dictionary copy without key/value normalization.

    Example:
        >>> _mapping({"values": {"size_bytes": 4}}, "values")
        {'size_bytes': 4}


    :param payload: Request mapping carrying the nested object.
    :param name: Required object-valued field name.
    :return: New dictionary retaining references to nested values.
    :raises CoreDispatchError: If the field is absent, None, or not a Mapping.
    """
    value = payload.get(name)
    if not isinstance(value, Mapping):
        raise CoreDispatchError("`{}` must be an object.".format(name))
    return dict(value)


def _spec(payload: Mapping[str, Any]) -> _ResourceSpec:
    """
    Resolve a stripped, lowercased public resource name in the closed registry.

    Hyphens remain significant; underscore aliases and arbitrary table names are
    not inferred. The returned specification is shared with the registry.

    Example:
        >>> _spec({"resource": " BACKUP-POLICY "}).table
        'backup_policies'


    :param payload: Request mapping with required resource text.
    :return: Registered immutable resource specification.
    :raises CoreDispatchError: For missing resource text or an unknown token, with available names in the latter's details.
    """
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
    """
    Require the full five-method portable persistence surface, including writers even for read handlers.

    Checks get_row, get_rows, insert_row, update_row, and delete_row for callability;
    no signatures or table capabilities are validated and no method runs here.

    Example:
        >>> from types import SimpleNamespace
        >>> from unittest.mock import Mock
        >>> macros = Mock()
        >>> _macros(SimpleNamespace(database=SimpleNamespace(macros=macros))) is macros
        True


    :param runtime: Runtime whose database may expose the portable macro owner.
    :return: Existing macro owner after all required callable checks pass.
    :raises CoreDispatchError: With capability_unavailable if the owner or any required method is absent/noncallable.
    """
    macros = getattr(runtime.database, "macros", None)
    required = ("get_row", "get_rows", "insert_row", "update_row", "delete_row")
    if macros is None or any(
        not callable(getattr(macros, name, None)) for name in required
    ):
        raise CoreDispatchError(
            "The local database does not provide portable storage persistence.",
            code="capability_unavailable",
            details={"area": "storage", "operation": "resource persistence"},
        )
    return macros


def _headings(runtime: "CoreRuntime", spec: _ResourceSpec) -> set[str]:
    """
    Try database then driver-wrapper headings, returning an empty set if neither call yields a result.

    Call/iteration/text-conversion Exceptions trigger fallback; attribute access is
    outside that catch and may propagate. An empty successful result stops fallback.
    Empty headings disable field-membership checks in the normalizer.

    Example:
        >>> from types import SimpleNamespace
        >>> from unittest.mock import Mock
        >>> database = SimpleNamespace(get_column_headings=Mock(side_effect=OSError("unavailable")))
        >>> _headings(SimpleNamespace(database=database), _RESOURCES["asset"])
        set()


    :param runtime: Runtime exposing database and optional driver-wrapper schema introspection.
    :param spec: Resource specification whose physical table name is queried.
    :return: Set of stringified headings from the first successful method, or an empty set after fallback.
    """
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
    """
    Translate public id or field text to a resource column, optionally enforcing known schema headings.

    The literal id alias returns immediately without a heading-membership check.
    Other tokens preserve an existing resource prefix or receive one; empty
    headings permit any such candidate, including fields absent from the schema.

    Example:
        >>> _column_name(_RESOURCES["asset"], "size_bytes", headings={"digital_asset_size_bytes"})
        'digital_asset_size_bytes'


    :param spec: Resource prefix and physical identity-column mapping.
    :param key: Public or physical field name, converted to stripped text.
    :param headings: Known physical headings; an empty set disables candidate-membership checks.
    :return: Physical column name; this helper does not validate writability or value type.
    :raises CoreDispatchError: For empty names or candidates absent from a nonempty headings set.
    """
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
    """
    Normalize field names and reject managed IDs/timestamps while leaving supplied values unchanged.

    Converted-key collisions overwrite earlier entries. Timestamp suffixes are
    rejected even for list filters with allow_id=True. If heading introspection
    fails or returns empty, other candidate names are not checked against schema.

    Example:
        >>> from types import SimpleNamespace
        >>> runtime = SimpleNamespace(database=SimpleNamespace())
        >>> _normalise_values(runtime, _RESOURCES["asset"], {"size_bytes": 4})
        {'digital_asset_size_bytes': 4}


    :param runtime: Runtime used to obtain resource-table headings when available.
    :param spec: Resource-specific field prefix and managed identity column.
    :param values: Mapping whose keys are translated and whose values are preserved by reference.
    :param allow_id: Permit identity-column entries, as used by read filters; timestamps remain forbidden.
    :return: New dictionary keyed by physical column names, without value coercion or deep copying.
    :raises CoreDispatchError: For unknown/empty fields, prohibited IDs, or managed timestamp suffixes.
    """
    headings = _headings(runtime, spec)
    normalised: dict[str, Any] = {}
    for key, value in values.items():
        column = _column_name(spec, str(key), headings=headings)
        if column == spec.id_column and not allow_id:
            raise CoreDispatchError("`id` is managed by Core and may not be written.")
        if column.endswith(("_created_timestamp_ep_k", "_modified_timestamp_ep_k")):
            raise CoreDispatchError("Core manages storage resource timestamps.")
        normalised[column] = value
    return normalised


def _record(spec: _ResourceSpec, row: Mapping[str, Any]) -> dict[str, Any]:
    """
    Expose a row as resource/id/values, removing the physical identity and resource prefix from values keys.

    Unprefixed fields remain unchanged. Colliding public keys overwrite earlier
    entries in row iteration order; nested values retain backend-native objects.

    Example:
        >>> _record(_RESOURCES["asset"], {"digital_asset_id": 7, "digital_asset_size_bytes": 4})
        {'resource': 'asset', 'id': 7, 'values': {'size_bytes': 4}}


    :param spec: Resource label, physical ID column, and removable field prefix.
    :param row: Row mapping with string keys; it is copied shallowly before projection.
    :return: New resource record with id=None if the physical identity key was absent.
    """
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
    """
    Exclude a small set of bad presence/integrity strings without independently verifying Replica bytes.

    Status text is casefolded but not stripped. Missing, blank, or unrecognized
    statuses pass; this is a permissive row-label filter, not a VERIFIED-state check.

    Example:
        >>> _healthy_replica({}), _healthy_replica({"asset_replica_presence_status": "OFFLINE"})
        (True, False)


    :param row: Replica row carrying optional asset_replica_presence_status and asset_replica_integrity_status fields.
    :return: False for missing/offline/deleted presence or bad/corrupt/failed integrity; True otherwise.
    """
    presence = str(row.get("asset_replica_presence_status") or "").casefold()
    integrity = str(row.get("asset_replica_integrity_status") or "").casefold()
    return presence not in {"missing", "offline", "deleted"} and integrity not in {
        "bad",
        "corrupt",
        "failed",
    }


class CoreStorageGraphAPI:
    """
    Register closed resource CRUD and row-based Asset/policy handlers without owning storage resources.

    Workflow-owned resources are read-only through generic mutation handlers.
    Policy assessment/planning here uses persisted row labels and copy counts,
    not the storage manager's live availability or separation-aware planner.
    Direct handler calls bypass runtime locking, events, and final wire conversion.

    Example:
        >>> adapter = CoreStorageGraphAPI()
        >>> adapter.install(runtime)  # doctest: +SKIP
    """

    def install(self, runtime: "CoreRuntime") -> None:
        """
        Register seven graph/policy queries followed by four mutation commands with explicit metadata.

        Registration invokes no storage operation and probes no capabilities.
        Earlier bindings remain if a later registration raises; duplicate-name
        handling belongs to the supplied runtime.

        Example:
            >>> CoreStorageGraphAPI().install(runtime)  # doctest: +SKIP


        :param runtime: Registrar receiving handlers, summaries, payload declarations, and tags.
        :return: None after all route registrations complete.
        """
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
        """
        Describe every registered resource and report whether the database has a non-None macros attribute.

        available does not check macro callability, table existence, or backend
        reachability. Resource order follows the static registry even when unavailable.

        Example:
            >>> from types import SimpleNamespace
            >>> runtime = SimpleNamespace(database=SimpleNamespace(macros=object()))
            >>> CoreStorageGraphAPI.resources_describe(runtime, None)["available"]
            True


        :param runtime: Runtime whose database is inspected for the macros attribute only.
        :param query: Query envelope, ignored; no payload options are read.
        :return: available flag and ordered name/kind/writable declarations for all thirteen resource types.
        """
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
        """
        Read all matching resource rows in ID order, then slice an offset/limit window in memory.

        limit defaults to 100 and is int-converted/clamped to 0..10,000; offset
        defaults to zero and is clamped nonnegative. Booleans/fractions follow int
        conversion, while explicit None raises. Filters allow IDs but still reject
        managed timestamp fields. Pagination does not bound the database read.

        Example:
            >>> result = CoreStorageGraphAPI.resource_list(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime providing macro reads and optional schema headings for field validation.
        :param query: Query with resource, optional Mapping where, and optional limit/offset; where=None is invalid.
        :return: Resource name, projected records, normalized pagination, and complete based on the full matching row count.
        """
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
        """
        Read one resource row by integer-converted ID, representing absence with record=None.

        Example:
            >>> result = CoreStorageGraphAPI.resource_get(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime providing the complete portable persistence macro surface.
        :param query: Query with a registered resource name and required non-None/non-boolean id.
        :return: Resource label and projected record, or None when the macro reports no row.
        """
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
        """
        Read an Asset, all its Replica/item-link rows, and a separate row-based policy assessment.

        A missing Asset returns an empty-shaped result instead of raising. For a
        present Asset, assessment rereads the Asset and Replicas, so concurrent
        changes can produce inconsistent components or a later not-found error.
        Replica output is not filtered to healthy or available copies.

        Example:
            >>> result = CoreStorageGraphAPI().asset_get(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime supplying portable managed-storage table access.
        :param query: Query with required integer-convertible asset_id, excluding None/bool.
        :return: asset, replicas, item_links, and policy components; absent Asset gives None/empty lists/None.
        """
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
        """
        Compare row-label-filtered Replica counts with the Asset's explicit replication/backup policy rows.

        Missing policy rows default replication minimum to one and backup minimum
        to zero. Falsey targets, including zero, fall back to the selected minimum.
        Missing/falsey Replica mode counts as active; mode comparisons are otherwise
        case-sensitive. Counts include multiple claims on one Store and do not
        check byte verification, availability, separation, tags, or inherited policies.

        Example:
            >>> state = CoreStorageGraphAPI._assess(runtime, 7)  # doctest: +SKIP


        :param runtime: Runtime providing macro reads of the Asset, Replicas, and explicit policy IDs.
        :param asset_id: Asset ID passed directly to row lookups; no conversion is performed here.
        :return: Replication/backup counts and minimum/target comparisons plus total and permissively healthy Replica counts.
        :raises CoreDispatchError: With storage_asset_not_found if the Asset row is absent.
        """
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
        """
        Validate the requested Asset ID and return the row-based count assessment.

        This does not invoke the storage manager's effective-policy or verified
        placement assessment; see _assess for its persisted-label/default rules.

        Example:
            >>> state = CoreStorageGraphAPI().policy_assess(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime providing portable Asset/Replica/policy table reads.
        :param query: Query with required integer-convertible asset_id, excluding None/bool.
        :return: Assessment dictionary with explicit policy IDs, counts, and minimum/target comparisons.
        """
        return self._assess(runtime, _required_int(_payload(query), "asset_id"))

    def policy_plan(
        self,
        runtime: "CoreRuntime",
        query: "CoreQuery",
    ) -> dict[str, Any]:
        """
        Suggest unoccupied, non-read-only Store rows for active and backup copies using row-based target counts.

        Any existing Replica row occupies its Store, regardless of health/state.
        Candidates are read in Store-ID order and filtered only by read_only and
        the relevant mode flag, not availability, tags, separation, capacity, or
        backend feasibility. Each family independently reuses the same candidates.
        Selection checks its count after appending, so an already met target can
        still suggest one placement. No bytes are copied or capacity reserved.

        Example:
            >>> plan = CoreStorageGraphAPI().policy_plan(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime providing portable Asset, Replica, policy, and Store row reads.
        :param query: Query with required integer-convertible asset_id.
        :return: Assessment, active-then-backup placement suggestions, and nonnegative per-family shortfalls.
        """
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
            """
            Select candidate Store rows for one assessed family, checking the requested count after each append.

            Missing capability flags default to supported; only values equal to
            False or zero are excluded. Candidates are not consumed or marked
            occupied, so another family can select the same Store. A zero shortfall
            still permits one eligible selection under the post-append count check.

            Example:
                >>> suggestions = placements("replication", "active")  # doctest: +SKIP


            :param family: Assessment key, replication or backup, supplying target and healthy_copies.
            :param mode: Mode token used in the Store capability-column name and emitted suggestion.
            :return: Ordered store_id/store_name/mode dictionaries, possibly fewer than required or one when none are needed.
            """
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
        """
        Assess every Asset and page those below replication or backup minimums, not merely below targets.

        The historical route summary mentions targets; selection actually uses
        meets_minimum. All Asset/Replica/policy reads precede pagination. limit is
        int-converted/clamped to 0..10,000 and offset to nonnegative; defaults are
        100/0. Concurrent disappearance or malformed row data can abort the scan.

        Example:
            >>> result = CoreStorageGraphAPI().policy_violations(runtime, query)  # doctest: +SKIP


        :param runtime: Runtime providing full Asset enumeration and row-based policy assessment.
        :param query: Query with optional integer-convertible limit/offset; explicit None is not accepted.
        :return: records window in Asset-ID order, normalized pagination, and completeness relative to the violating subset.
        """
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
        """
        Insert a writable resource row, read it back, and reconcile the service cache.

        Core-managed ID/timestamp fields are rejected, but values and relationships
        otherwise rely on database validation. created=True means insertion returned;
        a missing readback becomes record=None. Readback/projection/reconciliation
        can fail after insertion without compensating deletion.

        Example:
            >>> receipt = CoreStorageGraphAPI.resource_create(runtime, command)  # doctest: +SKIP


        :param runtime: Runtime providing macro insertion/readback, optional headings, and cache reconciliation.
        :param command: Command with writable resource name and required Mapping values; empty mappings are passed through.
        :return: Reconciled resource/id/record receipt with created=True after insertion and readback finish.
        :raises CoreDispatchError: If the resource is workflow-owned/read-only or payload/field validation fails.
        """
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
        """
        Require an existing writable resource row, normalize edits, update it, read it back, and reconcile.

        Update return values are ignored; updated does not establish a changed row
        count. Empty edits are forwarded. No atomicity is added around the existence
        check/write/readback, and later failures do not roll back earlier effects.

        Example:
            >>> receipt = CoreStorageGraphAPI.resource_update(runtime, command)  # doctest: +SKIP


        :param runtime: Runtime providing portable macro persistence and post-write service reconciliation.
        :param command: Command with writable resource, required integer-convertible id, and Mapping values.
        :return: Reconciled updated=True receipt with the readback record, possibly None after concurrent removal.
        :raises CoreDispatchError: For read-only/unknown resources, missing rows, or invalid payload/field names.
        """
        payload = _payload(command)
        spec = _spec(payload)
        if not spec.writable:
            raise CoreDispatchError(
                "`{}` is workflow-owned and read-only.".format(spec.name),
                code="storage_resource_read_only",
            )
        resource_id = _required_int(payload, "id")
        macros = _macros(runtime)
        if (
            macros.get_row(
                spec.table,
                resource_id,
                id_column=spec.id_column,
            )
            is None
        ):
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
        """
        Delete a writable resource row if present, reconciling only when deletion was attempted.

        Missing rows return deleted=False immediately without reconciliation.
        delete_row's return value is ignored, and the returned record describes
        pre-delete state. No extra confirmation, byte deletion, or rollback is
        implemented here; database constraints/cascades determine row effects.

        Example:
            >>> receipt = CoreStorageGraphAPI.resource_delete(runtime, command)  # doctest: +SKIP


        :param runtime: Runtime providing macro get/delete and service reconciliation.
        :param command: Command with writable resource and required integer-convertible id.
        :return: Plain deleted=False receipt for absence, or reconciled deleted=True receipt with the former record.
        :raises CoreDispatchError: For read-only/unknown resources or invalid request fields.
        """
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
        """
        Assign or clear one or both explicit Asset policy references after checking supplied non-null policy IDs.

        Omitted fields remain unchanged; None clears a reference. Non-null values
        reject booleans but otherwise use int coercion. Every supplied policy is
        checked before the single update, with no transaction across those reads
        and the write. No copies are created, removed, or reconciled by a storage
        policy executor; services.reconcile refreshes the post-write cache receipt.

        Example:
            >>> receipt = CoreStorageGraphAPI.asset_policies_set(runtime, command)  # doctest: +SKIP


        :param runtime: Runtime providing Asset/policy row lookup, macro update, and cache reconciliation.
        :param command: Command with asset_id and at least one of replication_policy_id or backup_policy_id, each integer-convertible/null.
        :return: Reconciled updated=True receipt with only the supplied normalized policy assignments; no readback is performed.
        :raises CoreDispatchError: If the Asset/policy is missing, no assignments are supplied, or required-value validation fails.
        """
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
                    raise CoreDispatchError(
                        "`{}` must be an integer or null.".format(name)
                    )
                policy_id = int(policy_id)
                if (
                    macros.get_row(
                        table,
                        policy_id,
                        id_column=id_column,
                    )
                    is None
                ):
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
    """
    Construct a stateless storage-graph adapter, register all its routes, and return it.

    Partial registration is not rolled back if a later binding raises.

    Example:
        >>> from unittest.mock import Mock
        >>> runtime = Mock()
        >>> isinstance(install_storage_graph_api(runtime), CoreStorageGraphAPI)
        True
        >>> runtime.register_query_handler.call_count, runtime.register_command_handler.call_count
        (7, 4)


    :param runtime: Registrar receiving managed-resource and row-based policy handlers.
    :return: New adapter after every query/command registration succeeds.
    """

    api = CoreStorageGraphAPI()
    api.install(runtime)
    return api


__all__ = ["CoreStorageGraphAPI", "install_storage_graph_api"]
