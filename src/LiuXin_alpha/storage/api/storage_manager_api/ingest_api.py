"""Transactional asset ingest facade."""

import abc
import io

from typing import BinaryIO
from uuid import UUID

from LiuXin_alpha.storage.api.models import Digest, Location, StoreRef
from LiuXin_alpha.storage.api.storage_manager_api.models import (
    DigitalAssetID,
    DigitalAssetMetadata,
    IngestResult,
    ItemID,
    ReplicaMode,
)


class AssetIngestAPI(abc.ABC):
    """Transactional entry points for creating or adopting managed assets.

    ``ingest_bytes`` is the small-payload convenience wrapper; implementations
    provide streaming ingest and adoption of already stored bytes.

    Example:
        >>> def ingest_cover(manager: AssetIngestAPI, payload: bytes) -> IngestResult:
        ...     return manager.ingest_bytes(payload, role="cover")
    """

    @abc.abstractmethod
    def ingest_stream(
        self,
        stream: BinaryIO,
        *,
        operation_id: UUID | None = None,
        expected_size: int | None = None,
        expected_digests: tuple[Digest, ...] = (),
        item_id: ItemID | None = None,
        role: str | None = None,
        metadata: DigitalAssetMetadata | None = None,
        preferred_store: StoreRef | None = None,
        replica_mode: ReplicaMode = ReplicaMode.ACTIVE,
        verify: bool = True,
    ) -> IngestResult:
        """
        Recoverably stage, verify, publish, and register a binary stream.

        Store publication and metadata persistence are separate transactions.
        Implementations use ``operation_id`` plus staged Replica state so a
        failure between them can be resumed or reconciled. Supplying the same
        operation UUID retries that logical ingest rather than silently
        creating a second operation.

        Example:
            >>> import io
            >>> result = manager.ingest_stream(  # doctest: +SKIP
            ...     io.BytesIO(b"book"), expected_size=4,
            ...     preferred_store=UUID(int=1),
            ... )

        :param stream:
        """
        ...

    def ingest_bytes(
        self, data: bytes, *, operation_id: UUID | None = None,
        expected_digests: tuple[Digest, ...] = (),
        item_id: ItemID | None = None, role: str | None = None,
        metadata: DigitalAssetMetadata | None = None,
        preferred_store: StoreRef | None = None,
        replica_mode: ReplicaMode = ReplicaMode.ACTIVE, verify: bool = True,
    ) -> IngestResult:
        """Ingest an in-memory payload with an exact size expectation.

        Example:
            >>> result = manager.ingest_bytes(  # doctest: +SKIP
            ...     b"cover", item_id=ItemID(9), role="cover",
            ... )
        """

        return self.ingest_stream(
            io.BytesIO(data), operation_id=operation_id,
            expected_size=len(data), expected_digests=expected_digests,
            item_id=item_id, role=role,
            metadata=metadata, preferred_store=preferred_store,
            replica_mode=replica_mode, verify=verify,
        )

    @abc.abstractmethod
    def adopt_location(
        self, location: Location, *, operation_id: UUID | None = None,
        digital_asset_id: DigitalAssetID | None = None,
        item_id: ItemID | None = None, role: str | None = None,
        replica_mode: ReplicaMode = ReplicaMode.UNMANAGED, verify: bool = False,
    ) -> IngestResult:
        """Inspect and register bytes already present at a Store Location.

        If ``digital_asset_id`` is supplied, the observed bytes must match that
        Asset. Otherwise the manager identifies or declares the Asset. The
        operation UUID provides retry identity across Store and repository
        boundaries.

        Example:
            >>> result = manager.adopt_location(  # doctest: +SKIP
            ...     Location(UUID(int=1), "legacy/book.epub"), verify=True,
            ... )
        """
        ...


__all__ = ["AssetIngestAPI"]
