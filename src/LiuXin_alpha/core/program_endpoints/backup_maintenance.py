"""Core endpoint declarations for backup maintenance operations."""

from __future__ import annotations

from LiuXin_alpha.core.program_endpoints.common import (
    ProgramEndpointRegistrar,
    field,
)
from LiuXin_alpha.core.program_endpoints.handlers import BackupMaintenanceHandlers


def install_queries(api: BackupMaintenanceHandlers, runtime: ProgramEndpointRegistrar) -> None:
    """Register this family's query endpoints."""

    query = runtime.register_query_handler

    query(
                "backup.plan",
                api.backup_plan,
                summary="Plan SquashFS backup packs between configured Stores.",
                payload_fields=(
                    field("source_store", required=True, field_type="string|integer"),
                    field(
                        "destination_store", required=True, field_type="string|integer"
                    ),
                    field("target_pack_size_bytes", required=True, field_type="integer"),
                    field("workflow_name_prefix", field_type="string|null"),
                    field("max_files_per_pack", field_type="integer|null"),
                    field("allowed_extensions", field_type="array|null"),
                    field("output_key_prefix", field_type="string"),
                ),
                tags=("backup", "storage", "read"),
            )

    query(
                "backup.workflows.list",
                api.backup_workflows_list,
                summary="List durable backup workflow definitions and status.",
                payload_fields=(
                    field("limit", field_type="integer"),
                    field("offset", field_type="integer"),
                ),
                tags=("backup", "storage", "read"),
            )

    query(
                "backup.workflow.get",
                api.backup_workflow_get,
                summary="Return one durable backup workflow and resume state.",
                payload_fields=(
                    field("workflow_id", required=True, field_type="integer"),
                ),
                tags=("backup", "storage", "read"),
            )

    query(
                "maintenance.status",
                api.maintenance_status,
                summary="Return maintenance plugins, queues, and dirty-record status.",
                tags=("maintenance", "read"),
            )

    query(
                "maintenance.duplicates.find",
                api.maintenance_duplicates_find,
                summary="Find duplicate values using database comparison semantics.",
                payload_fields=(
                    field("table", required=True, field_type="string"),
                    field("column", required=True, field_type="string"),
                    field("comparison", field_type="string"),
                ),
                tags=("maintenance", "duplicates", "read"),
            )

def install_commands(api: BackupMaintenanceHandlers, runtime: ProgramEndpointRegistrar) -> None:
    """Register this family's command endpoints."""

    command = runtime.register_command_handler

    command(
                "backup.workflow.save",
                api.backup_workflow_save,
                summary="Create or replace one durable backup workflow definition.",
                payload_fields=(
                    field("workflow_spec", required=True, field_type="object"),
                    field("workflow_id", field_type="integer|null"),
                ),
                tags=("backup", "storage", "write"),
            )

    command(
                "backup.workflow.start",
                api.backup_workflow_start,
                summary="Submit a durable, resumable backup workflow.",
                payload_fields=(
                    field("workflow_id", required=True, field_type="integer"),
                ),
                tags=("backup", "storage", "jobs", "write"),
            )

    command(
                "backup.squashfs.start",
                api.backup_squashfs_start,
                summary="Submit execution of one SquashFS backup workflow spec.",
                payload_fields=(
                    field("workflow_spec", required=True, field_type="object"),
                    field("verify_after_build", field_type="boolean"),
                    field("cleanup_staging_after_success", field_type="boolean"),
                    field("staging_root", field_type="string|null"),
                ),
                tags=("backup", "storage", "jobs", "write"),
            )

    command(
                "backup.squashfs.publish-store.start",
                api.backup_squashfs_publish_store_start,
                summary="Submit publication of one designated open SquashFS store.",
                payload_fields=(
                    field("store_id", required=True, field_type="integer"),
                    field("output_archive", field_type="string|null"),
                    field("compression", field_type="string"),
                    field("deterministic", field_type="boolean"),
                    field("force", field_type="boolean"),
                    field("duplicate_verified_files", field_type="boolean"),
                    field("strict", field_type="boolean"),
                    field("refresh_storage_manager", field_type="boolean"),
                ),
                tags=("backup", "storage", "jobs", "write"),
            )

    command(
                "backup.squashfs.publish-files.start",
                api.backup_squashfs_publish_files_start,
                summary="Submit designation and publication of file ids to a SquashFS archive.",
                payload_fields=(
                    field("file_ids", required=True, field_type="array"),
                    field("archive", required=True, field_type="string"),
                    field("store_name", field_type="string|null"),
                    field("compression", field_type="string"),
                    field("deterministic", field_type="boolean"),
                    field("force", field_type="boolean"),
                    field("strict", field_type="boolean"),
                    field("refresh_storage_manager", field_type="boolean"),
                ),
                tags=("backup", "storage", "jobs", "write"),
            )

    command(
                "maintenance.run",
                api.maintenance_run,
                summary="Run one bounded maintenance-engine pass.",
                payload_fields=(field("max_events", field_type="integer"),),
                tags=("maintenance", "write"),
            )

    command(
                "maintenance.clean",
                api.maintenance_clean,
                summary="Clean selected entity rows through the maintenance service.",
                payload_fields=(
                    field("table", required=True, field_type="string"),
                    field("row_ids", required=True, field_type="array"),
                ),
                tags=("maintenance", "write"),
            )

    command(
                "maintenance.merge",
                api.maintenance_merge,
                summary="Merge one duplicate entity into the retained entity.",
                payload_fields=(
                    field("table", required=True, field_type="string"),
                    field("retained_id", required=True, field_type="integer"),
                    field("merged_id", required=True, field_type="integer"),
                ),
                tags=("maintenance", "catalog", "write"),
            )
