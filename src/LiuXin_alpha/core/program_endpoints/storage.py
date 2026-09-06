"""Core endpoint declarations for storage operations."""

from __future__ import annotations

from LiuXin_alpha.core.program_endpoints.common import (
    ProgramEndpointRegistrar,
    field,
)
from LiuXin_alpha.core.program_endpoints.handlers import StorageHandlers


def install_queries(api: StorageHandlers, runtime: ProgramEndpointRegistrar) -> None:
    """Register this family's query endpoints."""

    query = runtime.register_query_handler

    query(
                "storage.store.get",
                api.storage_store_get,
                summary="Return persisted and live details for one store.",
                payload_fields=(
                    field("store", required=True, field_type="string|integer"),
                ),
                tags=("storage", "stores", "read"),
            )

    query(
                "storage.default.get",
                api.storage_default_get,
                summary="Return the selected default store.",
                tags=("storage", "stores", "read"),
            )

    query(
                "storage.location.stat",
                api.storage_location_stat,
                summary="Return status and metadata for one storage location.",
                payload_fields=(
                    field("store_uuid", required=True, field_type="string"),
                    field("key", required=True, field_type="string"),
                ),
                tags=("storage", "files", "read"),
            )

    query(
                "storage.sources.supported",
                api.storage_sources_supported,
                summary="List source/store registration kinds supported by Core.",
                tags=("storage", "ingest", "capabilities"),
            )

    query(
                "storage.backends.list",
                api.storage_backends_list,
                summary=(
                    "List configured-Store backend providers, capabilities, and "
                    "operator limitations."
                ),
                payload_fields=(
                    field("include_internal", field_type="boolean"),
                ),
                tags=("storage", "stores", "capabilities", "read"),
            )

    query(
                "storage.status",
                api.storage_status,
                summary=(
                    "Return a persisted Store/capacity/Replica overview plus "
                    "actionable storage health."
                ),
                payload_fields=(field("refresh_stores", field_type="boolean"),),
                tags=("storage", "health", "read"),
            )

    query(
                "storage.reconcile.plan",
                api.storage_reconcile_plan,
                summary="Plan non-destructive storage recovery from current health.",
                payload_fields=(field("refresh_stores", field_type="boolean"),),
                tags=("storage", "reconcile", "read"),
            )

    query(
                "storage.repair.plan",
                api.storage_repair_plan,
                summary="Plan bounded Replica verification and policy placement.",
                payload_fields=(
                    field("asset_id", field_type="integer|null"),
                    field("max_assets", field_type="integer"),
                ),
                tags=("storage", "repair", "planning", "read"),
            )

    query(
                "storage.store.evacuate.plan",
                api.storage_store_evacuate_plan,
                summary="Plan safe Replica evacuation from one Store.",
                payload_fields=(
                    field("store", required=True, field_type="string|integer"),
                    field("destination_store", field_type="string|integer|null"),
                    field("max_assets", field_type="integer"),
                ),
                tags=("storage", "stores", "evacuation", "planning", "read"),
            )

    query(
                "storage.recovery.list",
                api.storage_recovery_list,
                summary="List durable ingest operations and recovery state.",
                payload_fields=(
                    field("state", field_type="string|null"),
                    field("limit", field_type="integer"),
                    field("offset", field_type="integer"),
                ),
                tags=("storage", "ingest", "recovery", "read"),
            )

def install_commands(api: StorageHandlers, runtime: ProgramEndpointRegistrar) -> None:
    """Register this family's command endpoints."""

    command = runtime.register_command_handler

    command(
                "storage.store.probe",
                api.storage_store_probe,
                summary="Probe one configured store and return its live status.",
                payload_fields=(field("store", required=True, field_type="string|integer"),),
                tags=("storage", "stores", "write"),
            )

    command(
                "storage.store.update",
                api.storage_store_update,
                summary="Update ordinary Store configuration fields through typed values.",
                payload_fields=(
                    field("store", required=True, field_type="string|integer"),
                    field("changes", required=True, field_type="object"),
                ),
                tags=("storage", "stores", "write"),
            )

    command(
                "storage.store.delete",
                api.storage_store_delete,
                summary="Unregister a store, optionally deleting its database row.",
                payload_fields=(
                    field("store", required=True, field_type="string|integer"),
                    field("delete_from_database", field_type="boolean"),
                ),
                tags=("storage", "stores", "write"),
            )

    command(
                "storage.default.set",
                api.storage_default_set,
                summary="Select the default store.",
                payload_fields=(field("store", required=True, field_type="string|integer"),),
                tags=("storage", "stores", "write"),
            )

    command(
                "storage.file.copy",
                api.storage_file_copy,
                summary="Copy one Digital Asset through Core into a selected Store.",
                payload_fields=(
                    field("asset_id", required=True, field_type="integer"),
                    field("store", field_type="string|integer|null"),
                    field("metadata", field_type="object|null"),
                ),
                tags=("storage", "files", "write"),
            )

    command(
                "storage.source.register",
                api.storage_source_register,
                summary="Register or ingest one supported local/remote storage source.",
                payload_fields=(
                    field("kind", required=True, field_type="string"),
                    field("options", required=True, field_type="object"),
                ),
                tags=("storage", "ingest", "write"),
            )

    command(
                "storage.replica.verify",
                api.storage_replica_verify,
                summary="Verify one Replica and persist its latest observation.",
                payload_fields=(
                    field("replica_id", required=True, field_type="integer"),
                    field("calculate_digests", field_type="boolean"),
                ),
                tags=("storage", "integrity", "write"),
            )

    command(
                "storage.asset.verify",
                api.storage_asset_verify,
                summary="Verify selected, sufficient, or every Replica for one Asset.",
                payload_fields=(
                    field("asset_id", required=True, field_type="integer"),
                    field("replica_ids", field_type="array|null"),
                    field("all_replicas", field_type="boolean"),
                ),
                tags=("storage", "integrity", "write"),
            )

    command(
                "storage.audit",
                api.storage_audit,
                summary="Verify a bounded page of non-deleted Replicas.",
                payload_fields=(
                    field("limit", field_type="integer"),
                    field("offset", field_type="integer"),
                    field("calculate_digests", field_type="boolean"),
                ),
                tags=("storage", "integrity", "maintenance", "write"),
            )

    command(
                "storage.reconcile.apply",
                api.storage_reconcile_apply,
                summary="Apply bounded, non-destructive Store reload and verification repairs.",
                payload_fields=(
                    field("max_actions", field_type="integer"),
                    field("include_offline", field_type="boolean"),
                ),
                tags=("storage", "reconcile", "maintenance", "write"),
            )

    command(
                "storage.repair.apply",
                api.storage_repair_apply,
                summary="Apply bounded, non-deleting Replica verification and placement repair.",
                payload_fields=(
                    field("asset_id", field_type="integer|null"),
                    field("max_assets", field_type="integer"),
                    field("max_actions", field_type="integer"),
                    field("max_transfer_bytes", field_type="integer"),
                ),
                tags=("storage", "repair", "maintenance", "write"),
            )

    command(
                "storage.store.evacuate.apply",
                api.storage_store_evacuate_apply,
                summary="Copy and verify replacements before retiring source Replica claims.",
                payload_fields=(
                    field("store", required=True, field_type="string|integer"),
                    field("destination_store", field_type="string|integer|null"),
                    field("max_assets", field_type="integer"),
                    field("max_actions", field_type="integer"),
                    field("max_transfer_bytes", field_type="integer"),
                    field("keep_source_bytes", field_type="boolean"),
                ),
                tags=("storage", "stores", "evacuation", "maintenance", "write"),
            )

    command(
                "storage.recovery.recover-pending",
                api.storage_recovery_recover_pending,
                summary="Recover all or one interrupted Store publication.",
                payload_fields=(
                    field("operation_id", field_type="string|null"),
                ),
                tags=("storage", "ingest", "recovery", "write"),
            )

    command(
                "storage.recovery.retry-ingest",
                api.storage_recovery_retry_ingest,
                summary="Retry one durable ingest when its source remains replayable.",
                payload_fields=(
                    field("operation_id", required=True, field_type="string"),
                ),
                tags=("storage", "ingest", "recovery", "write"),
            )
