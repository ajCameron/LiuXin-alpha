"""Store-inventory to backup-pack planning facade."""

from __future__ import annotations

import abc

from collections.abc import Iterable

from LiuXin_alpha.storage.api2.models import StoreRef
from LiuXin_alpha.storage.api2.workflow_api.backup_api.models import BackupPackPlan


class BackupPlannerAPI(abc.ABC):
    """Plan size-bounded backup artifacts without executing them.

    The planner may inspect catalogue and replica state through a manager or
    repository, but returned plans are immutable workflow intent.

    Example:
        >>> plans = planner.plan_store_backup(  # doctest: +SKIP
        ...     source_store_ref="primary", destination_store_ref="archive",
        ...     target_artifact_size_bytes=4 * 1024**3,
        ... )
    """

    @abc.abstractmethod
    def plan_store_backup(
        self,
        *,
        source_store_ref: StoreRef,
        destination_store_ref: StoreRef,
        target_artifact_size_bytes: int,
        workflow_name_prefix: str | None = None,
        output_key_prefix: str = "backup-packs",
        max_sources_per_artifact: int | None = None,
        allowed_extensions: Iterable[str] | None = None,
    ) -> tuple[BackupPackPlan, ...]:
        """Partition one store's inventory into durable artifact plans.

        Example:
            >>> plans = planner.plan_store_backup(  # doctest: +SKIP
            ...     source_store_ref="primary",
            ...     destination_store_ref="archive",
            ...     target_artifact_size_bytes=1_000_000_000,
            ... )
        """
        ...


__all__ = ["BackupPlannerAPI"]
