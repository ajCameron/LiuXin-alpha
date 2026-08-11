"""Transactional asset ingest facade."""

import abc
import io

from typing import BinaryIO

from LiuXin_alpha.storage.api2.models import Location, StoreRef
from LiuXin_alpha.storage.api2.storage_manager_api.models import (
    DigitalAssetID, IngestResult, ItemID, ReplicaMode,
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
        self, stream: BinaryIO, *, size_bytes: int | None = None,
        expected_sha256: str | None = None, item_id: ItemID | None = None,
        role: str | None = None, metadata: object | None = None,
        preferred_store: StoreRef | None = None,
        replica_mode: ReplicaMode = ReplicaMode.ACTIVE, verify: bool = True,
    ) -> IngestResult:
        """Stage, verify, publish, and catalogue bytes from a binary stream.

        Example:
            >>> import io
            >>> result = manager.ingest_stream(  # doctest: +SKIP
            ...     io.BytesIO(b"book"), size_bytes=4,
            ...     preferred_store="primary",
            ... )
        """
        ...

    def ingest_bytes(
        self, data: bytes, *, item_id: ItemID | None = None,
        role: str | None = None, metadata: object | None = None,
        preferred_store: StoreRef | None = None,
        replica_mode: ReplicaMode = ReplicaMode.ACTIVE, verify: bool = True,
    ) -> IngestResult:
        """Ingest an in-memory payload with an exact size expectation.

        Example:
            >>> result = manager.ingest_bytes(  # doctest: +SKIP
            ...     b"cover", item_id=9, role="cover",
            ... )
        """

        return self.ingest_stream(
            io.BytesIO(data), size_bytes=len(data), item_id=item_id, role=role,
            metadata=metadata, preferred_store=preferred_store,
            replica_mode=replica_mode, verify=verify,
        )

    @abc.abstractmethod
    def adopt_location(
        self, location: Location, *, digital_asset_id: DigitalAssetID | None = None,
        item_id: ItemID | None = None, role: str | None = None,
        replica_mode: ReplicaMode = ReplicaMode.UNMANAGED, verify: bool = False,
    ) -> IngestResult:
        """Catalogue bytes that already exist at a concrete store location.

        Example:
            >>> result = manager.adopt_location(  # doctest: +SKIP
            ...     Location("archive", "legacy/book.epub"), verify=True,
            ... )
        """
        ...


__all__ = ["AssetIngestAPI"]
