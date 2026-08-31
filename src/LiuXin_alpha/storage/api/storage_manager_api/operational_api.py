"""Aggregate operational health contract for storage managers."""

from __future__ import annotations

import abc

from collections.abc import Mapping
from uuid import UUID

from LiuXin_alpha.storage.api.storage_manager_api.models.operational import (
    StorageOperationalStatus,
)
from LiuXin_alpha.storage.api.storage_manager_api.models.replicas import (
    DigitalAssetIngestResult,
)


class StorageOperationalStatusAPI(abc.ABC):
    """Expose actionable health without leaking repository rows.

    Example:
        >>> isinstance(manager, StorageOperationalStatusAPI)  # doctest: +SKIP
        True
    """

    @abc.abstractmethod
    def get_operational_status(
        self,
        *,
        refresh_stores: bool = False,
    ) -> StorageOperationalStatus:
        """Inspect Store reachability, journal, Replica, and policy state.

        Example:
            >>> status = manager.get_operational_status()  # doctest: +SKIP
            >>> status.healthy  # doctest: +SKIP
            True
        """

        ...

    @abc.abstractmethod
    def list_ingest_operations(self) -> tuple[Mapping[str, object], ...]:
        """Return operator-safe durable ingest-journal summaries."""

        ...

    @abc.abstractmethod
    def recover_pending_ingests(
        self,
        operation_id: UUID | None = None,
    ) -> tuple[str, ...]:
        """Recover all, or one selected, interrupted publication."""

        ...

    @abc.abstractmethod
    def retry_ingest_operation(
        self,
        operation_id: UUID,
    ) -> DigitalAssetIngestResult:
        """Replay one durable ingest when its original source is recoverable."""

        ...


__all__ = ["StorageOperationalStatusAPI"]
