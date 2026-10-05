"""
Plan SquashFS backup declarations from configured Store inventory.

The planner groups source-size estimates and captures digest/catalogue evidence without
building or reserving output. Plans use the public BackupPackPlan value.
Inventory, digest, and Replica reads can observe different moments in time.
"""

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
    ReplicaState,
    StoreUUID,
)
from LiuXin_alpha.storage.utils.workflow import normalize_archive_path


class StoreBackupPlanner(BackupPlannerAPI):
    """
    Collect a Store inventory and group its ordered sources into proposed SquashFS packs.

    Planning borrows a manager, obtains missing digests through Store reads, and retains available
    non-deleted Replica provenance. It does not adopt unknown sources, execute a build, persist
    intent, reserve destinations, or lock the source inventory. Targets measure summed source sizes
    rather than final compressed-image size; an oversized individual source still receives a pack.

    Example:
        >>> plans = StoreBackupPlanner(manager).plan_store_backup(  # doctest: +SKIP
        ...     source_store_ref=source.store_ref,
        ...     destination_store_ref=destination.store_ref,
        ...     target_artifact_size_bytes=1024**3,
        ... )
    """

    def __init__(self, storage_manager) -> None:
        """
        Retain the manager used for Store lookup and optional source provenance.

        No runtime conformance check, startup, or ownership transfer occurs. The caller controls
        manager lifetime.

        Example:
            >>> planner = StoreBackupPlanner(manager)  # doctest: +SKIP


        :param storage_manager: Borrowed manager exposing Store lookup and Replica-record iteration.
        :return: None after retaining the supplied manager reference.
        """
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
        """
        Partition qualifying inventory into deterministic source-order pack declarations.

        Reject nonpositive size/count targets before Store lookup. Materialize the source inventory,
        compute absent digests, and associate the last encountered non-deleted Replica at each
        Location. Replica health is not otherwise filtered. A later read failure aborts the call
        rather than returning partial plans. Source bytes and catalogue metadata are not a
        version-pinned snapshot.

        Normalize archive names and sort them lexically. Greedily accumulate source sizes, treating
        an absent size as zero, and flush a nonempty pack before another source would exceed the
        size/count target. An oversized source is included alone; the estimate excludes compression
        and archive overhead. Member-name collisions are rejected when a declaration is built,
        within that pack.

        Extension filtering uses the final POSIX suffix, case-insensitively. Supplied filters are
        stringified, stripped, and stripped of leading dots. None selects all entries, while an
        empty collection selects none. Pack names receive one-based four-digit ordinals and .sqsh;
        destination location construction validates its own key policy without reserving or
        publishing an output.

        Example:
            >>> packs = planner.plan_store_backup(  # doctest: +SKIP
            ...     source_store_ref=source.store_ref,
            ...     destination_store_ref=destination.store_ref,
            ...     target_artifact_size_bytes=25,
            ...     max_sources_per_artifact=100,
            ...     allowed_extensions=["epub"],
            ... )


        :param source_store_ref: Configured Store UUID whose inventory and non-deleted Replica records supply source evidence.
        :param destination_store_ref: Configured Store UUID used to construct proposed output Locations.
        :param target_artifact_size_bytes: Positive target for summed source bytes per pack, not a sealed-image byte ceiling.
        :param workflow_name_prefix: Truthy pack-name prefix, otherwise the source Store name or a Store-UUID fallback.
        :param output_key_prefix: Output key prefix; empty text places generated filenames at the Store root.
        :param max_sources_per_artifact: Optional positive source-count target; comparisons do not require exact integer types.
        :param allowed_extensions: Optional iterable of suffix spellings; None accepts all suffixes and an empty iterable accepts none.
        :return: Ordered tuple of BackupPackPlan values, or an empty tuple when no entries qualify.
        """
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
        replicas_by_location = {
            replica.location: replica
            for replica in self.storage_manager.iter_replica_records(
                store_ref=source_store_ref,
            )
            if replica.state is not ReplicaState.DELETED
        }
        for info in source_store.iter_file_infos():
            extension = pathlib.PurePosixPath(info.location.key).suffix.lower().lstrip(".")
            if extension_filter is not None and extension not in extension_filter:
                continue
            archive_path = normalize_archive_path(info.location.key)
            digest = info.digest or source_store.compute_digest(info.location)
            replica = replicas_by_location.get(info.location)
            source_asset_id = (
                None if replica is None else replica.digital_asset_id
            )
            entries.append(
                BackupSourceDeclaration(
                    BackupSourceKind.STORE_LOCATION,
                    info.location,
                    archive_path=archive_path,
                    expected_size=info.size,
                    expected_digest=digest,
                    source_digital_asset_id=source_asset_id,
                    source_replica_id=(
                        None if replica is None else replica.replica_id
                    ),
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
            """
            Append a declaration for the currently accumulated sources and reset pack accounting.

            The closure uses the borrowed destination Store, selected prefixes, current source list,
            size total, and prior plans. An empty group is a no-op. Location/declaration
            construction precedes append and reset, so a failure leaves captured accounting
            unchanged and propagates to the planner. It does not create a destination object.

            Example:
                Within the enclosing planner, call flush() when adding another source would exceed a
                size/count target, and once after inventory traversal to retain the final group.


            :return: None after appending a numbered plan and clearing its source/size accumulator, or when the group is empty.
            """
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


__all__ = [
    "StoreBackupPlanner",
]
