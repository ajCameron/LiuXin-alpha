"""Core-owned backup operations and wire translation."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, cast

from LiuXin_alpha.core.errors import CoreDispatchError
from LiuXin_alpha.core.program_services.payloads import (
    _database_path,
    _database_type,
    _job_submit,
    _mapping,
    _optional_int,
    _payload,
    _required_int,
    _required_text,
    _text_list,
    plain,
)
from LiuXin_alpha.core.program_services.store_resolution import _store

if TYPE_CHECKING:
    from LiuXin_alpha.core.commands import CoreCommand
    from LiuXin_alpha.core.queries import CoreQuery
    from LiuXin_alpha.core.runtime import CoreRuntime


def backup_plan(
    runtime: CoreRuntime,
    query: CoreQuery,
) -> dict[str, Any]:
    payload = _payload(query)
    from LiuXin_alpha.storage.backup import StoreBackupPlanner

    source_reference = payload.get("source_store")
    destination_reference = payload.get("destination_store")
    if source_reference in (None, ""):
        raise CoreDispatchError("`source_store` is required.")
    if destination_reference in (None, ""):
        raise CoreDispatchError("`destination_store` is required.")
    source_store = _store(runtime, source_reference)
    destination_store = _store(runtime, destination_reference)
    planner = StoreBackupPlanner(runtime.library.storage)
    packs = planner.plan_store_backup(
        source_store_ref=source_store.store_ref,
        destination_store_ref=destination_store.store_ref,
        target_artifact_size_bytes=_required_int(
            payload,
            "target_pack_size_bytes",
        ),
        workflow_name_prefix=(
            str(payload["workflow_name_prefix"])
            if payload.get("workflow_name_prefix") is not None
            else None
        ),
        output_key_prefix=str(payload.get("output_key_prefix") or "backup-packs"),
        max_sources_per_artifact=_optional_int(
            payload,
            "max_files_per_pack",
            minimum=1,
        ),
        allowed_extensions=(
            _text_list(payload, "allowed_extensions")
            if payload.get("allowed_extensions") is not None
            else None
        ),
    )
    return {
        "packs": [plain(item) for item in packs],
        "count": len(packs),
    }


def backup_workflows_list(
    runtime: CoreRuntime,
    query: CoreQuery,
) -> dict[str, Any]:
    payload = _payload(query)
    limit = _optional_int(payload, "limit", default=100, minimum=1)
    offset = _optional_int(payload, "offset", default=0, minimum=0)
    assert limit is not None and offset is not None
    from LiuXin_alpha.storage.backup import BackupWorkflowRepository

    repository = BackupWorkflowRepository(runtime.database)
    rows = list(
        runtime.database.get_all_rows(
            "backup_workflows",
            iterator_return=False,
            sort_column="backup_workflow_id",
        )
    )
    page = rows[offset : offset + limit]
    records: list[dict[str, Any]] = []
    for row in page:
        workflow_id = int(row["backup_workflow_id"])
        state = repository.load_checkpoint(workflow_id)
        records.append(
            {
                "workflow_id": workflow_id,
                "workflow_name": str(row["backup_workflow_name"]),
                "workflow_kind": str(row["backup_workflow_kind"]),
                "output_url": str(row["backup_workflow_output_url"]),
                "status": state.status.value,
                "next_source_index": state.next_source_index,
                "staged_source_count": state.staged_source_count,
                "source_count": len(state.declaration.sources),
                "last_error": state.last_error,
            }
        )
    return {
        "records": records,
        "total": len(rows),
        "limit": limit,
        "offset": offset,
    }


def backup_workflow_get(
    runtime: CoreRuntime,
    query: CoreQuery,
) -> dict[str, Any]:
    payload = _payload(query)
    workflow_id = _required_int(payload, "workflow_id")
    from LiuXin_alpha.storage.backup import BackupWorkflowRepository

    state = BackupWorkflowRepository(runtime.database).load_checkpoint(workflow_id)
    return {
        "workflow_id": workflow_id,
        "spec": plain(state.declaration),
        "state": plain(state),
    }


def backup_workflow_save(
    runtime: CoreRuntime,
    command: CoreCommand,
) -> dict[str, Any]:
    payload = _payload(command)
    from LiuXin_alpha.core.workflow_jobs import (
        backup_workflow_spec_from_mapping,
    )
    from LiuXin_alpha.storage.api import WorkflowStatus
    from LiuXin_alpha.storage.backup import BackupWorkflowRepository

    spec = backup_workflow_spec_from_mapping(_mapping(payload, "workflow_spec"))
    workflow_id = _optional_int(payload, "workflow_id", minimum=1)
    saved_id = BackupWorkflowRepository(runtime.database).save_workflow_declaration(
        spec,
        workflow_id=workflow_id,
        status=WorkflowStatus.DRAFT,
    )
    return {
        "workflow_id": int(saved_id),
        "created": workflow_id is None,
        "spec": plain(spec),
    }


def backup_workflow_start(
    runtime: CoreRuntime,
    command: CoreCommand,
) -> dict[str, Any]:
    payload = _payload(command)
    workflow_id = _required_int(payload, "workflow_id")
    from LiuXin_alpha.storage.backup import BackupWorkflowRepository

    # Validate the durable definition before accepting the job.
    BackupWorkflowRepository(runtime.database).load_workflow_declaration(workflow_id)
    return _job_submit(
        runtime,
        payload,
        function_name="run_persisted_backup_job",
        kwargs={
            "database_path": _database_path(runtime),
            "db_type": _database_type(runtime),
            "workflow_id": workflow_id,
        },
        default_label=f"backup workflow {workflow_id}",
    )


def backup_squashfs_start(
    runtime: CoreRuntime,
    command: CoreCommand,
) -> dict[str, Any]:
    payload = _payload(command)
    return _job_submit(
        runtime,
        payload,
        function_name="run_squashfs_backup_job",
        kwargs={
            "database_path": _database_path(runtime),
            "db_type": _database_type(runtime),
            "workflow_spec": _mapping(payload, "workflow_spec"),
            "verify_after_build": bool(payload.get("verify_after_build", True)),
            "cleanup_staging_after_success": bool(
                payload.get("cleanup_staging_after_success", False)
            ),
            "staging_root": payload.get("staging_root"),
        },
        default_label="squashfs backup",
    )


def backup_squashfs_publish_store_start(
    runtime: CoreRuntime,
    command: CoreCommand,
) -> dict[str, Any]:
    payload = _payload(command)
    output_archive = payload.get("output_archive")
    return _job_submit(
        runtime,
        payload,
        function_name="run_publish_open_squashfs_store_job",
        kwargs={
            "database_path": _database_path(runtime),
            "db_type": _database_type(runtime),
            "store_id": _required_int(payload, "store_id"),
            "output_archive": (
                None if output_archive in (None, "") else str(output_archive)
            ),
            "compression": str(payload.get("compression") or "zstd"),
            "deterministic": bool(payload.get("deterministic", False)),
            "force": bool(payload.get("force", False)),
            "duplicate_verified_files": bool(
                payload.get("duplicate_verified_files", True)
            ),
            "strict": bool(payload.get("strict", False)),
            "refresh_storage_manager": bool(
                payload.get("refresh_storage_manager", True)
            ),
        },
        default_label="publish SquashFS store",
    )


def backup_squashfs_publish_files_start(
    runtime: CoreRuntime,
    command: CoreCommand,
) -> dict[str, Any]:
    payload = _payload(command)
    raw_file_ids = payload.get("file_ids")
    if not isinstance(raw_file_ids, Sequence) or isinstance(
        raw_file_ids,
        (str, bytes),
    ):
        raise CoreDispatchError("`file_ids` must be an array.")
    file_ids = [int(cast(Any, value)) for value in raw_file_ids]
    if not file_ids:
        raise CoreDispatchError("`file_ids` must not be empty.")
    store_name = payload.get("store_name")
    return _job_submit(
        runtime,
        payload,
        function_name="run_publish_squashfs_files_job",
        kwargs={
            "database_path": _database_path(runtime),
            "db_type": _database_type(runtime),
            "file_ids": file_ids,
            "archive": _required_text(payload, "archive"),
            "store_name": (None if store_name in (None, "") else str(store_name)),
            "compression": str(payload.get("compression") or "zstd"),
            "deterministic": bool(payload.get("deterministic", False)),
            "force": bool(payload.get("force", False)),
            "strict": bool(payload.get("strict", False)),
            "refresh_storage_manager": bool(
                payload.get("refresh_storage_manager", True)
            ),
        },
        default_label="publish SquashFS files",
    )
