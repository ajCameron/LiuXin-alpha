"""
Define storage health inspection and explicit durable-ingest recovery entry points.

Reports carry attributable observations and suggestions; recovery/retry are
separate operations whose journal and publication effects belong to the manager.
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


# Todo: This makes sense to be larger, if it's all operational matters
class StorageOperationalStatusAPI(abc.ABC):
    """
    Separate health/journal inspection from explicit recovery and retry operations. Health
    inspection reports metadata and optional fresh Store observations without implicitly applying
    recovery. Concrete managers own persistence, source replayability, and failure boundaries;
    transient implementations may have no durable journal.

    Example:
        >>> status = manager.get_operational_status(refresh_stores=True)  # doctest: +SKIP
    """

    @abc.abstractmethod
    def get_operational_status(
        self,
        *,
        refresh_stores: bool = False,
    ) -> StorageOperationalStatus:
        """
        Assemble attributable Store, ingest, Replica, and policy findings with suggested actions.
        Optional Store refresh may update plugin observation state. The report does not itself
        execute suggestions or establish an atomic cross-subsystem snapshot.

        Example:
            >>> status = manager.get_operational_status()  # doctest: +SKIP


        :param refresh_stores: Whether Store plugins are explicitly asked for fresh status observations.
        :return: Timestamped StorageOperationalStatus assembled from the implementation's observations.
        """

        ...

    @abc.abstractmethod
    def list_ingest_operations(self) -> tuple[Mapping[str, object], ...]:
        """
        Expose operator-facing summaries of journalled ingest operations when available.

        Entries are observation mappings rather than database row objects; field availability and durable versus
        transient behavior belong to the implementation.

        Example:
            >>> operations = manager.list_ingest_operations()  # doctest: +SKIP


        :return: Tuple of ingest summary mappings, possibly empty when no durable journal is available.
        """

        ...

    @abc.abstractmethod
    def recover_pending_ingests(
        self,
        operation_id: UUID | None = None,
    ) -> tuple[str, ...]:
        """
        Explicitly attempt recovery of all or one interrupted publication. Durable implementations
        may verify published bytes and complete or fail journalled work; transient implementations
        may return an empty result. Recovery can mutate catalogue/journal state and is separate from
        inspection.

        Example:
            >>> messages = manager.recover_pending_ingests(operation_id)  # doctest: +SKIP


        :param operation_id: Optional exact ingest UUID restricting recovery; None selects all pending work.
        :return: Tuple of implementation-provided recovery messages; empty does not by itself count recovered operations.
        """

        ...

    @abc.abstractmethod
    def retry_ingest_operation(
        self,
        operation_id: UUID,
    ) -> DigitalAssetIngestResult:
        """
        Request retry or recovery under the original journalled operation identity. Success depends
        on stored evidence and a recoverable source; the API does not imply that lost input streams
        can be recreated. Implementations may return an already completed result or raise a
        precondition error.

        Example:
            >>> result = manager.retry_ingest_operation(operation_id)  # doctest: +SKIP


        :param operation_id: UUID of the original ingest operation to retry or recover.
        :return: DigitalAssetIngestResult for the completed/recovered operation.
        """

        ...


__all__ = ["StorageOperationalStatusAPI"]
