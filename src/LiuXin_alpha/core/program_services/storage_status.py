"""Combine durable Store configuration, live observations, and replica accounting.

Inventory loading, per-Store presentation, and overall health aggregation are
separate stages; malformed durable rows remain visible in the result.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any
from uuid import UUID

from LiuXin_alpha.core.program_services.payloads import _payload, plain
from LiuXin_alpha.storage import api as storage_api
from LiuXin_alpha.storage.store_spec_utils import store_configuration_from_row

if TYPE_CHECKING:
    from LiuXin_alpha.core.queries import CoreQuery
    from LiuXin_alpha.core.runtime import CoreRuntime


@dataclass(frozen=True)
class _StoreInventory:
    configurations: dict[UUID, storage_api.StoreConfiguration]
    persisted: dict[UUID, dict[str, Any]]
    invalid_rows: list[dict[str, Any]]
    row_count: int


def _load_store_inventory(
    runtime: CoreRuntime,
    manager_configurations: Mapping[UUID, storage_api.StoreConfiguration],
) -> _StoreInventory:
    """Merge durable Store rows with the manager's loaded configuration view."""
    configurations = dict(manager_configurations)
    persisted: dict[UUID, dict[str, Any]] = {}
    invalid_store_rows: list[dict[str, Any]] = []
    database_store_rows = 0

    try:
        has_store_table = "stores" in {
            str(table) for table in runtime.database.get_tables()
        }
    except Exception:
        has_store_table = False
    if has_store_table:
        rows = tuple(
            runtime.database.get_all_rows(
                "stores",
                iterator_return=False,
            )
            or ()
        )
        database_store_rows = len(rows)
        for row in rows:
            plain_row = plain(row)
            row_values = plain_row if isinstance(plain_row, Mapping) else {}
            store_id_raw = row_values.get("store_id")
            try:
                store_id = (
                    None if store_id_raw in (None, "") else int(str(store_id_raw))
                )
            except (TypeError, ValueError):
                store_id = None
            online_status = (
                str(row_values.get("store_online_status") or "").strip().casefold()
                or None
            )
            try:
                configuration = store_configuration_from_row(
                    row,
                    fallback_store_id=store_id,
                )
            except Exception as exc:
                invalid_store_rows.append(
                    {
                        "store_id": store_id,
                        "store_name": row_values.get("store_name"),
                        "store_kind": row_values.get("store_kind"),
                        "store_root_uri": row_values.get("store_root_uri"),
                        "online_status": online_status,
                        "error": str(exc) or type(exc).__name__,
                    }
                )
                continue
            configurations[configuration.store_uuid] = configuration
            persisted[configuration.store_uuid] = {
                "store_id": store_id,
                "online_status": online_status,
            }

    return _StoreInventory(
        configurations, persisted, invalid_store_rows, database_store_rows
    )


@dataclass(frozen=True)
class _StatusContext:
    inventory: _StoreInventory
    manager_configurations: Mapping[UUID, storage_api.StoreConfiguration]
    observations: Mapping[UUID, storage_api.StoreStatus]
    live_store_refs: set[UUID]
    default_store_ref: UUID | None
    asset_sizes: Mapping[storage_api.DigitalAssetID, int]
    replicas_by_store: Mapping[UUID, list[storage_api.ReplicaRecord]]
    status: storage_api.StorageOperationalStatus


def _render_store_status(
    configuration: storage_api.StoreConfiguration,
    context: _StatusContext,
) -> dict[str, Any]:
    """Project one Store's topology, replica accounting, and observed health."""
    observations = context.observations
    persisted = context.inventory.persisted
    manager_configurations = context.manager_configurations
    replicas_by_store = context.replicas_by_store
    asset_sizes = context.asset_sizes
    status = context.status
    live_store_refs = context.live_store_refs
    default_store_ref = context.default_store_ref
    store_ref = configuration.store_uuid
    current_status = observations.get(store_ref)
    persisted_values = persisted.get(store_ref, {})
    online_status = persisted_values.get("online_status")
    registered = store_ref in manager_configurations
    configuration_in_sync = (
        not registered or manager_configurations[store_ref] == configuration
    )
    deliberately_offline = online_status in {"offline", "retired"}

    replicas = replicas_by_store.get(store_ref, [])
    asset_ids = {record.digital_asset_id for record in replicas}
    replica_state_counts = Counter(record.state.value for record in replicas)
    replica_mode_counts = Counter(record.mode.value for record in replicas)
    replica_bytes = sum(
        asset_sizes.get(record.digital_asset_id, 0) for record in replicas
    )
    logical_bytes = sum(asset_sizes.get(asset_id, 0) for asset_id in asset_ids)
    unaccounted_replicas = sum(
        record.digital_asset_id not in asset_sizes for record in replicas
    )

    replica_ids = {record.replica_id for record in replicas}
    attributed_issues = [
        issue
        for issue in status.issues
        if issue.store_ref == store_ref
        or (issue.replica_id is not None and issue.replica_id in replica_ids)
    ]
    severities = {issue.severity.value for issue in attributed_issues}
    if current_status is None:
        health = "offline" if deliberately_offline else "unknown"
    elif not current_status.available:
        health = "unavailable"
    elif "error" in severities:
        health = "error"
    elif "warning" in severities:
        health = "warning"
    else:
        health = "healthy"

    total_bytes = None if current_status is None else current_status.total_bytes
    free_bytes = None if current_status is None else current_status.free_bytes
    used_bytes = (
        None if total_bytes is None or free_bytes is None else total_bytes - free_bytes
    )
    free_percent = (
        None
        if total_bytes in (None, 0) or free_bytes is None
        else round((free_bytes / total_bytes) * 100, 2)
    )
    available = (
        False
        if current_status is None and deliberately_offline
        else (None if current_status is None else bool(current_status.available))
    )
    writable = (
        False
        if current_status is None and configuration.read_only
        else (None if current_status is None else bool(current_status.writable))
    )
    return {
        "store_uuid": str(store_ref),
        "store_id": persisted_values.get("store_id"),
        "name": configuration.store_name,
        "kind": configuration.store_kind,
        "root": configuration.store_root_uri,
        "url": configuration.store_url,
        "protocol": configuration.store_access_protocol,
        "role": configuration.operational_role,
        "online_status": online_status,
        "supports_folders": bool(configuration.supports_folders),
        "read_only": bool(configuration.read_only),
        "available": available,
        "writable": writable,
        "health": health,
        "registered": registered,
        "loaded": store_ref in live_store_refs,
        "configuration_in_sync": configuration_in_sync,
        "is_default": store_ref == default_store_ref,
        "tags": list(configuration.store_tags),
        "failure_domain": configuration.store_failure_domain,
        "region": configuration.store_region,
        "supported_replica_modes": sorted(
            mode.value for mode in configuration.supported_replica_modes
        ),
        "assets": len(asset_ids),
        "replicas": len(replicas),
        "logical_bytes": logical_bytes,
        "replica_bytes": replica_bytes,
        "unaccounted_replicas": unaccounted_replicas,
        "replica_states": dict(sorted(replica_state_counts.items())),
        "replica_modes": dict(sorted(replica_mode_counts.items())),
        "total_bytes": total_bytes,
        "free_bytes": free_bytes,
        "used_bytes": used_bytes,
        "free_percent": free_percent,
        "object_count": (
            None if current_status is None else current_status.object_count
        ),
        "checked_at": (None if current_status is None else current_status.checked_at),
        "message": (None if current_status is None else current_status.message),
        "warnings": ([] if current_status is None else list(current_status.warnings)),
        "issues": [plain(issue) for issue in attributed_issues],
    }


def storage_status(
    runtime: CoreRuntime,
    query: CoreQuery,
) -> dict[str, Any]:
    payload = _payload(query)
    manager = runtime.library.storage
    status = manager.get_operational_status(
        refresh_stores=bool(payload.get("refresh_stores", False))
    )
    manager_configurations = {
        configuration.store_uuid: configuration
        for configuration in manager.iter_store_configurations()
    }
    inventory = _load_store_inventory(runtime, manager_configurations)
    configurations = inventory.configurations
    invalid_store_rows = inventory.invalid_rows
    database_store_rows = inventory.row_count

    observations = {
        observation.store_ref: observation.status
        for observation in status.store_statuses
    }
    live_store_refs = {
        store.configuration.store_uuid for store in manager.iter_stores()
    }
    try:
        default_store_ref = manager.get_default_store_ref()
    except Exception:
        default_store_ref = None

    asset_sizes = {
        record.digital_asset_id: int(record.size_bytes)
        for record in manager.iter_digital_asset_records()
    }
    all_replicas = tuple(manager.iter_replica_records())
    live_replicas = tuple(
        record
        for record in all_replicas
        if record.state is not storage_api.ReplicaState.DELETED
    )
    replicas_by_store: dict[UUID, list[storage_api.ReplicaRecord]] = {}
    for record in live_replicas:
        replicas_by_store.setdefault(record.location.store_ref, []).append(record)

    overview_issues: list[dict[str, Any]] = []
    if invalid_store_rows:
        overview_issues.append(
            {
                "code": "invalid_store_configuration",
                "severity": "error",
                "message": (
                    f"{len(invalid_store_rows)} persisted Store row(s) could not be interpreted."
                ),
            }
        )

    context = _StatusContext(
        inventory,
        manager_configurations,
        observations,
        live_store_refs,
        default_store_ref,
        asset_sizes,
        replicas_by_store,
        status,
    )
    store_records = [
        _render_store_status(configuration, context)
        for configuration in sorted(
            configurations.values(),
            key=lambda value: (value.store_name.casefold(), value.store_uuid.int),
        )
    ]
    drifted_configurations = sum(
        record["registered"] and not record["configuration_in_sync"]
        for record in store_records
    )
    unregistered_online_stores = sum(
        not record["registered"]
        and record["online_status"] not in {"offline", "retired"}
        for record in store_records
    )

    if drifted_configurations:
        overview_issues.append(
            {
                "code": "store_configuration_drift",
                "severity": "warning",
                "message": (
                    f"{drifted_configurations} Store configuration(s) differ from the live "
                    "StorageManager; run `liuxin storage refresh`."
                ),
            }
        )
    if unregistered_online_stores:
        overview_issues.append(
            {
                "code": "store_not_registered",
                "severity": "warning",
                "message": (
                    f"{unregistered_online_stores} online Store configuration(s) are not registered "
                    "with the StorageManager; run `liuxin storage refresh`."
                ),
            }
        )

    issue_severities = Counter(issue.severity.value for issue in status.issues)
    replica_state_counts = Counter(record.state.value for record in live_replicas)
    known_statuses = [
        record for record in store_records if record["available"] is not None
    ]
    summary = {
        "database_store_rows": database_store_rows,
        "configured_stores": len(configurations),
        "invalid_store_rows": len(invalid_store_rows),
        "registered_stores": len(manager_configurations),
        "loaded_stores": len(live_store_refs),
        "folder_stores": sum(
            bool(record["supports_folders"]) for record in store_records
        ),
        "available_stores": sum(
            record["available"] is True for record in store_records
        ),
        "unavailable_stores": sum(
            record["available"] is False for record in store_records
        ),
        "unknown_availability_stores": (len(store_records) - len(known_statuses)),
        "writable_stores": sum(record["writable"] is True for record in store_records),
        "read_only_stores": sum(bool(record["read_only"]) for record in store_records),
        "configuration_drift": drifted_configurations,
        "unregistered_online_stores": unregistered_online_stores,
        "digital_assets": len(asset_sizes),
        "logical_bytes": sum(asset_sizes.values()),
        "live_replicas": len(live_replicas),
        "replica_bytes": sum(
            asset_sizes.get(record.digital_asset_id, 0) for record in live_replicas
        ),
        "replica_states": dict(sorted(replica_state_counts.items())),
        "issues": len(status.issues) + len(overview_issues),
        "issue_severities": {
            severity: issue_severities.get(severity, 0)
            + sum(issue["severity"] == severity for issue in overview_issues)
            for severity in ("info", "warning", "error")
        },
    }
    healthy = bool(status.healthy) and not overview_issues
    return {
        "healthy": healthy,
        "checked_at": status.checked_at,
        "summary": summary,
        "stores": store_records,
        "invalid_store_rows": invalid_store_rows,
        "overview_issues": overview_issues,
        "status": plain(status),
    }
