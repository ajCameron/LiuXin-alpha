"""
Aggregate operational health contract for storage managers.
"""

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
    """
    Expose actionable health without leaking repository rows.

    Status collection is metadata-read-only. A caller may explicitly request
    fresh Store-plugin observations, but recovery remains a separate public
    action so inspecting health cannot silently change catalogue state.

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
        """
        Inspect Store reachability, journal, Replica, and policy state.

        Example:
            >>> status = manager.get_operational_status()  # doctest: +SKIP
            >>> status.healthy  # doctest: +SKIP
            True


        :param refresh_stores:
        :return:
        """

        ...

    @abc.abstractmethod
    def list_ingest_operations(self) -> tuple[Mapping[str, object], ...]:
        """
        Return operator-safe durable ingest-journal summaries.

        Example:
            >>> operations = manager.list_ingest_operations()  # doctest: +SKIP
            >>> isinstance(operations, tuple)  # doctest: +SKIP
            True


        :return:
        """

        ...

    @abc.abstractmethod
    def recover_pending_ingests(
        self,
        operation_id: UUID | None = None,
    ) -> tuple[str, ...]:
        """
        Recover all, or one selected, interrupted publication.

        Example:
            >>> messages = manager.recover_pending_ingests()  # doctest: +SKIP
            >>> isinstance(messages, tuple)  # doctest: +SKIP
            True


        :param operation_id:
        :return:
        """

        ...

    @abc.abstractmethod
    def retry_ingest_operation(
        self,
        operation_id: UUID,
    ) -> DigitalAssetIngestResult:
        """
        Replay one durable ingest when its original source is recoverable.

        Example:
            >>> result = manager.retry_ingest_operation(operation_id)  # doctest: +SKIP
            >>> result.asset_record.content_hash.algorithm  # doctest: +SKIP
            'sha256'


        :param operation_id:
        :return:
        """

        ...


__all__ = ["StorageOperationalStatusAPI"]
