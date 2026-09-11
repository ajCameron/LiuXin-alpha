"""
Separate inventory-based backup planning from archive execution.

Plans carry intended sources and output Locations. Inventory reads and digest computation
may be needed, but this interface does not publish artifacts or reserve destinations.
"""

from __future__ import annotations

import abc

from collections.abc import Iterable
from uuid import UUID

from LiuXin_alpha.storage.api.models import StoreUUID
from LiuXin_alpha.storage.api.workflow_api.backup_api.models import BackupPackPlan


class BackupPlannerAPI(abc.ABC):
    """
    Partition Store inventory into declarations without building or publishing artifacts.

    Planning may inspect metadata and read source bytes to obtain digests. Returned plans describe
    intended packs; they neither reserve destination names nor guarantee capacity, immutable source
    versions, or sealed output sizes. Repository persistence and execution are separate operations.

    Example:
        >>> plans = planner.plan_store_backup(  # doctest: +SKIP
        ...     source_store_ref=UUID(int=1), destination_store_ref=UUID(int=2),
        ...     target_artifact_size_bytes=4 * 1024**3,
        ... )
    """

    @abc.abstractmethod
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
        Plan ordered artifact groups from a source Store's inventory.

        The StoreBackupPlanner implementation sorts normalized member paths and groups their
        uncompressed sizes, flushing a nonempty pack before the next source would exceed a size or
        count target. An oversized source receives its own pack. Missing inventory digests are
        computed, and available non-deleted Replica identities are carried into declarations without
        adopting uncatalogued sources. Inventory and metadata reads are not a single pinned
        snapshot.

        That implementation filters the final filename suffix case-insensitively, stripping
        surrounding whitespace and leading dots from allowed extensions. None accepts all entries;
        an empty supplied collection accepts none. Output names are proposed without creating bytes
        or testing collisions.

        Example:
            >>> plans = planner.plan_store_backup(  # doctest: +SKIP
            ...     source_store_ref=UUID(int=1),
            ...     destination_store_ref=UUID(int=2),
            ...     target_artifact_size_bytes=1_000_000_000,
            ... )


        :param source_store_ref: UUID of the configured Store whose full inventory supplies members.
        :param destination_store_ref: UUID of the configured Store used to construct output Locations.
        :param target_artifact_size_bytes: Positive target for summed source bytes per pack; not a hard limit on an individual source or sealed output.
        :param workflow_name_prefix: Optional pack-name prefix; StoreBackupPlanner uses the source Store name or UUID fallback when falsey.
        :param output_key_prefix: Destination key prefix, default backup-packs; an empty string places proposed names at the Store root.
        :param max_sources_per_artifact: Optional positive upper target for member count per pack.
        :param allowed_extensions: Optional iterable of suffix spellings; None disables filtering and a supplied empty iterable selects no members.
        :return: Tuple of BackupPackPlan values in pack order, or an empty tuple when no inventory entries qualify.
        """
        ...


__all__ = ["BackupPlannerAPI"]
