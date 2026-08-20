"""Size-bounded backup planning over the new manager and Store contracts."""

from __future__ import annotations

import pathlib

from collections.abc import Iterable

from LiuXin_alpha.storage.api import (
    BackupPackPlan,
    BackupPlannerAPI,
    BackupSourceDeclaration,
    BackupSourceKind,
    BackupWorkflowDeclaration,
    BackupWorkflowKind,
    StoreUUID,
)
from LiuXin_alpha.storage.utils.workflow import normalize_archive_path


class StoreBackupPlanner(BackupPlannerAPI):
    """Partition a configured Store's complete inventory into SquashFS packs."""

    def __init__(self, storage_manager) -> None:
        self.storage_manager = storage_manager

    def plan_store_backup(
        self,
        *,
        source_store_ref: StoreUUID,
        destination_store_ref: StoreUUID,
        target_artifact_size_bytes: int,
        workflow_name_prefix: str | None = None,
        output_key_prefix: str = "backup-packs",
        max_sources_per_artifact: int | None = None,
        allowed_extensions: Iterable[str] | None = None,
    ) -> tuple[BackupPackPlan, ...]:
        if target_artifact_size_bytes <= 0:
            raise ValueError("target_artifact_size_bytes must be positive.")
        if max_sources_per_artifact is not None and max_sources_per_artifact <= 0:
            raise ValueError("max_sources_per_artifact must be positive.")

        source_store = self.storage_manager.get_store(source_store_ref)
        destination_store = self.storage_manager.get_store(destination_store_ref)
        extension_filter = (
            None
            if allowed_extensions is None
            else {
                str(extension).strip().lower().lstrip(".")
                for extension in allowed_extensions
                if str(extension).strip()
            }
        )
        entries = []
        for info in source_store.iter_file_infos():
            extension = pathlib.PurePosixPath(info.location.key).suffix.lower().lstrip(".")
            if extension_filter is not None and extension not in extension_filter:
                continue
            archive_path = normalize_archive_path(info.location.key)
            digest = info.digest or source_store.compute_digest(info.location)
            entries.append(
                BackupSourceDeclaration(
                    BackupSourceKind.STORE_LOCATION,
                    info.location,
                    archive_path=archive_path,
                    expected_size=info.size,
                    expected_digest=digest,
                    source_store_ref=source_store_ref,
                )
            )
        entries.sort(key=lambda source: source.archive_path or "")
        if not entries:
            return ()

        prefix = (
            workflow_name_prefix
            or source_store.configuration.store_name
            or f"store-{source_store_ref}"
        )
        plans: list[BackupPackPlan] = []
        current: list[BackupSourceDeclaration] = []
        current_size = 0

        def flush() -> None:
            nonlocal current, current_size
            if not current:
                return
            pack_index = len(plans) + 1
            workflow_name = f"{prefix}-pack-{pack_index:04d}"
            filename = f"{workflow_name}.sqsh"
            output = (
                destination_store.location(output_key_prefix, filename)
                if output_key_prefix
                else destination_store.locate(filename)
            )
            declaration = BackupWorkflowDeclaration(
                workflow_name=workflow_name,
                workflow_kind=BackupWorkflowKind.SQUASHFS_PACK,
                output_target=output,
                sources=tuple(current),
            )
            plans.append(
                BackupPackPlan(
                    pack_index=pack_index,
                    workflow_declaration=declaration,
                    source_count=len(current),
                    estimated_size_bytes=current_size,
                )
            )
            current = []
            current_size = 0

        for source in entries:
            size = source.expected_size or 0
            size_limit_reached = bool(
                current
                and current_size + size > target_artifact_size_bytes
            )
            count_limit_reached = bool(
                current
                and max_sources_per_artifact is not None
                and len(current) >= max_sources_per_artifact
            )
            if size_limit_reached or count_limit_reached:
                flush()
            current.append(source)
            current_size += size
        flush()
        return tuple(plans)


# A descriptive implementation name remains useful to application code; the
# public value returned by the planner is BackupPackPlan.
PlannedBackupPack = BackupPackPlan


__all__ = ["PlannedBackupPack", "StoreBackupPlanner"]
