"""
Transactional asset ingest facade.
"""

import abc
import dataclasses
import io
import os

from pathlib import Path
from typing import BinaryIO
from uuid import UUID

from LiuXin_alpha.storage.api.errors import StorageIntegrityError
from LiuXin_alpha.storage.api.models import (
    Digest,
    FileInfo,
    Location,
    StoreInventoryEntry,
    StoreUUID,
)
from LiuXin_alpha.storage.api.placement_hints_api import StoragePlacementHints
from LiuXin_alpha.storage.api.storage_manager_api.models import (
    DigitalAssetID,
    DigitalAssetMetadata,
    DigitalAssetIngestResult,
    ItemID,
    ReplicaMode,
)
from LiuXin_alpha.storage.api.store_api.facade_api import StoreAPI
from LiuXin_alpha.storage.api.store_api.ingest_source_api import (
    IngestSourceStoreAPI,
    PreparedIngestObject,
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

    def ingest_identified_stream(
        self,
        stream: BinaryIO,
        *,
        size_bytes: int,
        authoritative_digests: tuple[Digest, ...],
        operation_id: UUID | None = None,
        item_id: ItemID | None = None,
        role: str | None = None,
        metadata: DigitalAssetMetadata | None = None,
        placement_hints: StoragePlacementHints | None = None,
        preferred_store_ref: StoreUUID | None = None,
        replica_mode: ReplicaMode = ReplicaMode.ACTIVE,
        verify: bool = True,
    ) -> DigitalAssetIngestResult:
        """Publish a stream whose identity was authoritatively established.

        This fast path avoids manager-side spooling. It requires an exact size
        and an authoritative SHA-256 digest; the destination Store verifies
        both while committing, so bytes cannot be registered under an
        unchecked identity. Callers must use this only with source metadata
        that the source Store explicitly marks authoritative.

        Example:
            >>> result = manager.ingest_identified_stream(  # doctest: +SKIP
            ...     stream, size_bytes=4,
            ...     authoritative_digests=(Digest("sha256", digest),),
            ... )

        :param stream: Binary source positioned at its start.
        :param size_bytes: Authoritative exact byte count.
        :param authoritative_digests: Authoritative source digests including SHA-256.
        :param operation_id: Optional idempotency key.
        :param item_id: Optional Item to link.
        :param role: Optional Item asset role.
        :param metadata: Digital Asset catalogue metadata.
        :param placement_hints: Advisory destination placement metadata.
        :param preferred_store_ref: Optional destination Store.
        :param replica_mode: Registered Replica mode.
        :param verify: Whether to verify the committed Replica.
        :return: Completed ingest result.
        """
        return self.ingest_stream(
            stream,
            operation_id=operation_id,
            expected_size=size_bytes,
            expected_digests=authoritative_digests,
            item_id=item_id,
            role=role,
            metadata=metadata,
            placement_hints=placement_hints,
            preferred_store_ref=preferred_store_ref,
            replica_mode=replica_mode,
            verify=verify,
        )

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

    def ingest_file(
        self,
        path: str | os.PathLike[str],
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
        """Ingest one local file and return the complete ingest result.

        The observed file size is pinned as the stream expectation so a file
        that changes between inspection and reading fails rather than being
        registered under ambiguous metadata. The basename becomes
        ``original_name`` when the caller did not supply one.

        Example:
            >>> result = manager.ingest_file(  # doctest: +SKIP
            ...     "/incoming/book.epub", item_id=ItemID(9),
            ... )
        """

        source_path = Path(path)
        observed_size = source_path.stat().st_size
        if expected_size is not None and expected_size != observed_size:
            raise StorageIntegrityError(
                f"expected {expected_size} bytes, found {observed_size}."
            )
        if metadata is None:
            normalized_metadata = DigitalAssetMetadata(
                original_name=source_path.name
            )
        elif metadata.original_name is None:
            normalized_metadata = dataclasses.replace(
                metadata,
                original_name=source_path.name,
            )
        else:
            normalized_metadata = metadata
        with source_path.open("rb") as source:
            return self.ingest_stream(
                source,
                operation_id=operation_id,
                expected_size=observed_size,
                expected_digests=expected_digests,
                item_id=item_id,
                role=role,
                metadata=normalized_metadata,
                placement_hints=placement_hints,
                preferred_store_ref=preferred_store_ref,
                replica_mode=replica_mode,
                verify=verify,
            )

    def ingest_store_object(
        self,
        source: StoreAPI,
        info: FileInfo | StoreInventoryEntry,
        *,
        operation_id: UUID | None = None,
        item_id: ItemID | None = None,
        role: str | None = None,
        metadata: DigitalAssetMetadata | None = None,
        placement_hints: StoragePlacementHints | None = None,
        preferred_store_ref: StoreUUID | None = None,
        replica_mode: ReplicaMode = ReplicaMode.ACTIVE,
        verify: bool = True,
    ) -> DigitalAssetIngestResult:
        """Ingest one Store object using the safest available transfer path.

        Implementations may use a native cross-Store transfer when the source
        identity is authoritative. The default opens a version-pinned stream
        when supported and chooses trusted-identity or ordinary stream ingest.

        Example:
            >>> result = manager.ingest_store_object(  # doctest: +SKIP
            ...     source_store, source_store.stat(location),
            ... )

        :param source: Configured source Store.
        :param info: Source inventory or stat information.
        :param operation_id: Optional idempotency key.
        :param item_id: Optional Item to link.
        :param role: Optional Item asset role.
        :param metadata: Digital Asset catalogue metadata.
        :param placement_hints: Advisory destination placement metadata.
        :param preferred_store_ref: Optional destination Store.
        :param replica_mode: Registered Replica mode.
        :param verify: Whether to verify the committed Replica.
        :return: Completed ingest result.
        """

        if isinstance(source, IngestSourceStoreAPI):
            prepared = source.prepare_ingest(info, inspect=False)
            if prepared.info.location != info.location:
                raise StorageIntegrityError(
                    "prepared ingest metadata describes another Location."
                )
            return self.ingest_prepared_store_object(
                source,
                prepared,
                operation_id=operation_id,
                item_id=item_id,
                role=role,
                metadata=metadata,
                placement_hints=placement_hints,
                preferred_store_ref=preferred_store_ref,
                replica_mode=replica_mode,
                verify=verify,
            )
        authoritative = (
            source.capabilities.stat_digest_authoritative
            and info.digest is not None
        )
        digests = (
            (info.digest,)
            if authoritative and info.digest is not None
            else ()
        )
        version = (
            info.version if source.capabilities.conditional_read else None
        )
        reader = (
            source.open_read(info.location)
            if version is None
            else source.open_read(info.location, if_version=version)
        )
        with reader as stream:
            if (
                info.size is not None
                and any(digest.algorithm == "sha256" for digest in digests)
            ):
                return self.ingest_identified_stream(
                    stream,
                    size_bytes=info.size,
                    authoritative_digests=digests,
                    operation_id=operation_id,
                    item_id=item_id,
                    role=role,
                    metadata=metadata,
                    placement_hints=placement_hints,
                    preferred_store_ref=preferred_store_ref,
                    replica_mode=replica_mode,
                    verify=verify,
                )
            return self.ingest_stream(
                stream,
                operation_id=operation_id,
                expected_size=info.size,
                expected_digests=digests,
                item_id=item_id,
                role=role,
                metadata=metadata,
                placement_hints=placement_hints,
                preferred_store_ref=preferred_store_ref,
                replica_mode=replica_mode,
                verify=verify,
            )

    def ingest_prepared_store_object(
        self,
        source: StoreAPI,
        prepared: PreparedIngestObject,
        *,
        operation_id: UUID | None = None,
        item_id: ItemID | None = None,
        role: str | None = None,
        metadata: DigitalAssetMetadata | None = None,
        placement_hints: StoragePlacementHints | None = None,
        preferred_store_ref: StoreUUID | None = None,
        replica_mode: ReplicaMode = ReplicaMode.ACTIVE,
        verify: bool = True,
    ) -> DigitalAssetIngestResult:
        """Ingest an object already prepared by its optional source Store.

        This is the efficient companion to :meth:`ingest_store_object` for
        discovery pipelines. It preserves rich inspection results and avoids
        asking a remote plugin to prepare the same object twice.

        Example:
            >>> prepared = source.prepare_ingest(entry)  # doctest: +SKIP
            >>> result = manager.ingest_prepared_store_object(  # doctest: +SKIP
            ...     source, prepared,
            ... )

        :param source: Store that produced ``prepared``.
        :param prepared: Bound per-object ingest observations.
        :param operation_id: Optional idempotency key.
        :param item_id: Optional Item to link.
        :param role: Optional Item asset role.
        :param metadata: Digital Asset catalogue metadata.
        :param placement_hints: Advisory destination placement metadata.
        :param preferred_store_ref: Optional destination Store.
        :param replica_mode: Registered Replica mode.
        :param verify: Whether to verify the committed Replica.
        :return: Completed ingest result.
        """

        if not isinstance(source, IngestSourceStoreAPI):
            raise TypeError(
                "prepared Store ingest requires IngestSourceStoreAPI."
            )
        source.require_location(prepared.info.location)
        try:
            source.ingest_capabilities.validate_prepared(prepared)
        except ValueError as error:
            raise StorageIntegrityError(str(error)) from error
        digests = prepared.authoritative_digests
        with source.open_prepared_ingest(prepared) as stream:
            if (
                prepared.info.size is not None
                and any(digest.algorithm == "sha256" for digest in digests)
            ):
                return self.ingest_identified_stream(
                    stream,
                    size_bytes=prepared.info.size,
                    authoritative_digests=digests,
                    operation_id=operation_id,
                    item_id=item_id,
                    role=role,
                    metadata=metadata,
                    placement_hints=placement_hints,
                    preferred_store_ref=preferred_store_ref,
                    replica_mode=replica_mode,
                    verify=verify,
                )
            return self.ingest_stream(
                stream,
                operation_id=operation_id,
                expected_size=prepared.info.size,
                expected_digests=digests,
                item_id=item_id,
                role=role,
                metadata=metadata,
                placement_hints=placement_hints,
                preferred_store_ref=preferred_store_ref,
                replica_mode=replica_mode,
                verify=verify,
            )

    @abc.abstractmethod
    def adopt_location(
        self, location: Location, *, operation_id: UUID | None = None,
        digital_asset_id: DigitalAssetID | None = None,
        item_id: ItemID | None = None, role: str | None = None,
        metadata: DigitalAssetMetadata | None = None,
        replica_mode: ReplicaMode = ReplicaMode.UNMANAGED, verify: bool = False,
    ) -> DigitalAssetIngestResult:
        """
        Inspect and register bytes already present at a Store Location.

        If ``digital_asset_id`` is supplied, the observed bytes must match that Asset.

        Otherwise, the manager identifies or declares the Asset. ``metadata``
        describes a newly declared Asset and participates in operation retry
        identity; it does not overwrite an already-known deduplicated Asset.
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
        :param metadata:
        :param replica_mode:
        :param verify:
        :return:
        """
        ...


__all__ = ["DigitalAssetIngestAPI"]
