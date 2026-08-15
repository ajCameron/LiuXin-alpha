"""
Transactional asset ingest facade.
"""

import abc
import io

from typing import BinaryIO
from uuid import UUID

from LiuXin_alpha.storage.api.models import Digest, Location, StoreUUID
from LiuXin_alpha.storage.api.placement_hints_api import StoragePlacementHints
from LiuXin_alpha.storage.api.storage_manager_api.models import (
    DigitalAssetID,
    DigitalAssetMetadata,
    DigitalAssetIngestResult,
    ItemID,
    ReplicaMode,
)


class DigitalAssetIngestAPI(abc.ABC):
    """
    Transactional entry points for creating or adopting managed assets.

    ``ingest_bytes`` is the small-payload convenience wrapper; implementations
    provide streaming ingest and adoption of already stored bytes.

    Example:
        >>> def ingest_cover(
        ...     manager: DigitalAssetIngestAPI, payload: bytes,
        ... ) -> DigitalAssetIngestResult:
        ...     return manager.ingest_bytes(payload, role="cover")
    """

    # Todo: In digital assets, make_digital_asset (from file or binary) should point to here

    # Todo: ingest_path

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
        placement_hints: StoragePlacementHints | None = None,
        preferred_store_ref: StoreUUID | None = None,
        replica_mode: ReplicaMode = ReplicaMode.ACTIVE,
        verify: bool = True,
    ) -> DigitalAssetIngestResult:
        """
        Recoverably stage, verify, publish, and register a binary stream.

        Store publication and metadata persistence are separate transactions.
        Implementations use ``operation_id`` plus staged Replica state so a
        failure between them can be resumed or reconciled. Supplying the same
        operation UUID retries that logical ingest rather than silently
        creating a second operation. Reusing it with different bytes,
        metadata, Item role, placement, Replica mode, or verification intent
        raises ``StoragePreconditionFailed``.
        ``metadata`` describes the Digital Asset in the manager catalogue;
        ``placement_hints`` is advisory library metadata forwarded to a Store
        while it selects a location and publishes the bytes.

        Example:
            >>> import io
            >>> result = manager.ingest_stream(  # doctest: +SKIP
            ...     io.BytesIO(b"book"), expected_size=4,
            ...     preferred_store_ref=UUID(int=1),
            ... )


        :param stream:
        :param operation_id:
        :param expected_size:
        :param expected_digests:
        :param item_id:
        :param role:
        :param metadata:
        :param placement_hints:
        :param preferred_store_ref:
        :param replica_mode:
        :param verify:
        :return:
        """
        ...

    def ingest_bytes(
        self, data: bytes, *, operation_id: UUID | None = None,
        expected_digests: tuple[Digest, ...] = (),
        item_id: ItemID | None = None, role: str | None = None,
        metadata: DigitalAssetMetadata | None = None,
        placement_hints: StoragePlacementHints | None = None,
        preferred_store_ref: StoreUUID | None = None,
        replica_mode: ReplicaMode = ReplicaMode.ACTIVE, verify: bool = True,
    ) -> DigitalAssetIngestResult:
        """
        Ingest an in-memory payload with an exact size expectation.

        Example:
            >>> result = manager.ingest_bytes(  # doctest: +SKIP
            ...     b"cover", item_id=ItemID(9), role="cover",
            ... )


        :param data:
        :param operation_id:
        :param expected_digests:
        :param item_id:
        :param role:
        :param metadata:
        :param placement_hints:
        :param preferred_store_ref:
        :param replica_mode:
        :param verify:
        :return:
        """

        return self.ingest_stream(
            io.BytesIO(data), operation_id=operation_id,
            expected_size=len(data), expected_digests=expected_digests,
            item_id=item_id, role=role,
            metadata=metadata, placement_hints=placement_hints,
            preferred_store_ref=preferred_store_ref,
            replica_mode=replica_mode, verify=verify,
        )

    @abc.abstractmethod
    def adopt_location(
        self, location: Location, *, operation_id: UUID | None = None,
        digital_asset_id: DigitalAssetID | None = None,
        item_id: ItemID | None = None, role: str | None = None,
        replica_mode: ReplicaMode = ReplicaMode.UNMANAGED, verify: bool = False,
    ) -> DigitalAssetIngestResult:
        """
        Inspect and register bytes already present at a Store Location.

        If ``digital_asset_id`` is supplied, the observed bytes must match that Asset.

        Otherwise, the manager identifies or declares the Asset.
        The operation UUID provides retry identity across Store and repository
        boundaries.

        Example:
            >>> result = manager.adopt_location(  # doctest: +SKIP
            ...     Location(UUID(int=1), "legacy/book.epub"), verify=True,
            ... )


        :param location:
        :param operation_id:
        :param digital_asset_id:
        :param item_id:
        :param role:
        :param replica_mode:
        :param verify:
        :return:
        """
        ...


__all__ = ["DigitalAssetIngestAPI"]
