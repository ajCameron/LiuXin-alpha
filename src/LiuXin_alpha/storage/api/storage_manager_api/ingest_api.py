"""
Define manager ingest contracts and bytes, local-file, and prepared-source conveniences.

Publication, identity evidence, metadata registration, and retry records have
distinct boundaries. Source readers are closed by wrappers that open them;
caller-provided stream ownership remains with the caller. Concrete managers supply
publication/recovery behavior and may override identified or native transfer paths.
"""
# Todo: Add this to all files - and switch to using annotations primarily as type hinting
from __future__ import annotations

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


# Todo: Formatting wise, we have some run on lines in the docstrings
class DigitalAssetIngestAPI(abc.ABC):
    """
    Define streamed publication, source-Store transfer, and adoption of existing bytes.

    Conveniences wrap bytes/files, validate prepared-source contracts, and select ordinary or
    identified streams. Store publication and manager metadata are distinct commit boundaries;
    durable implementations need recovery journaling. Completed-operation identity includes
    normalized request details, without promising that a retry repeats byte verification or remains
    valid after later external changes.

    Example:
        >>> result = manager.ingest_bytes(b"cover", role="cover")  # doctest: +SKIP
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
        Identify and publish the remaining binary stream, then register Asset/Replica and optional
        Item-link metadata.

        The composed manager spools and hashes the entire remaining stream, validates expected
        size/digests, and only then checks for a completed operation. Reusing an operation UUID
        requires the normalized bytes, expectations, metadata, link, placement, mode, and
        verification request to agree. A matching completed retry returns its recorded result
        without another publication.

        Publication and metadata persistence are separate; failures after writing bytes may require
        recovery. verify requests a later Replica inspection and does not disable input-identity
        checks when false. Metadata describes newly registered Assets; placement hints advise
        destination layout.

        Example:
            >>> import io
            >>> result = manager.ingest_stream(io.BytesIO(b"book"), expected_size=4)  # doctest: +SKIP


        :param stream: Caller-owned binary reader consumed from its current position; the manager does not close it.
        :param operation_id: Optional logical-operation UUID; None allocates a new one. Completed retries require an equal normalized request.
        :param expected_size: Optional exact remaining byte count checked after consumption; it is not a read limit.
        :param expected_digests: Expected digests to compare with computed input bytes; ordinary ingest also computes SHA-256.
        # Todo: We should probably record that an injest event has occured somewhere "what are the unregistered file on an injested store" is a good question to ask
        :param item_id: Optional Item identity to link after registration; None omits linking.
        # Todo: This should, probably, be an enumerate list
        :param role: Optional link role; None selects primary_payload when item_id is supplied.
        :param metadata: Optional Asset description used for a new identity; existing deduplicated Asset metadata is retained.
        :param placement_hints: Advisory destination placement metadata, retained in retry identity and forwarded during publication.
        :param preferred_store_ref: Optional destination Store UUID; None selects the manager default.
        :param replica_mode: Requested mode for a new or selected destination Replica; default ACTIVE.
        :param verify: Whether to inspect the registered Replica after publication/reuse; False still permits identity hashing and Store commit checks.

        :return: Completed ingest result, including creation/deduplication flags and reported verification; failures may follow publication or earlier metadata writes.
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
        """
        Delegate a trusted identity to ordinary ingest unless an implementation provides a direct
        publication path.

        This default forwards size/digests as ordinary stream expectations, adding no separate
        authority or SHA-256 check. The composed manager overrides it to require a unique digest set
        including SHA-256 and to avoid spooling. That override trusts the caller's identity, can
        reuse completed results/readable Replicas without consuming the stream, and delegates
        new-byte checks to the Store commit and requested verification.

        Example:
            >>> result = manager.ingest_identified_stream(  # doctest: +SKIP
            ...     stream, size_bytes=4, authoritative_digests=(Digest("sha256", digest),),
            ... )


        :param stream: Caller-owned stream positioned at the identified bytes; a completed retry or reused Replica may leave it unread.
        :param size_bytes: Authoritative remaining byte count; the composed implementation rejects values below zero.
        :param authoritative_digests: Trusted source identity including SHA-256; the composed implementation requires unique algorithms.
        :param operation_id: Optional logical-operation UUID; None allocates a new one. Completed retries require an equal normalized request.
        :param item_id: Optional Item identity to link after registration; None omits linking.
        :param role: Optional link role; None selects primary_payload when item_id is supplied.
        :param metadata: Optional Asset description used for a new identity; existing deduplicated Asset metadata is retained.
        :param placement_hints: Advisory destination placement metadata, retained in retry identity and forwarded during publication.
        :param preferred_store_ref: Optional destination Store UUID; None selects the manager default.
        :param replica_mode: Requested mode for a new or selected destination Replica; default ACTIVE.
        :param verify: Whether to inspect the registered Replica after publication/reuse; False still permits identity hashing and Store commit checks.

        :return: Completed ingest result, including creation/deduplication flags and reported verification; failures may follow publication or earlier metadata writes.
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
        self, data: bytes,
        *,
        operation_id: UUID | None = None,
        expected_digests: tuple[Digest, ...] = (),
        item_id: ItemID | None = None, role: str | None = None,
        metadata: DigitalAssetMetadata | None = None,
        placement_hints: StoragePlacementHints | None = None,
        preferred_store_ref: StoreUUID | None = None,
        replica_mode: ReplicaMode = ReplicaMode.ACTIVE, verify: bool = True,
    ) -> DigitalAssetIngestResult:
        """
        # Todo: Check - just call this to add bytes to the system?
        Wrap an in-memory payload in BytesIO and delegate with its exact length as the size
        expectation.

        All remaining options pass through unchanged. The composed streaming implementation still
        consumes and hashes this buffer on completed-operation retries; the wrapper does not add a
        separate publication or transaction boundary.

        Example:
            >>> result = manager.ingest_bytes(b"cover", item_id=ItemID(9), role="cover")  # doctest: +SKIP

        :param data: In-memory bytes copied into a new BytesIO reader.
        :param operation_id: Optional logical-operation UUID; None allocates a new one. Completed retries require an equal normalized request.
        :param expected_digests: Expected digests to compare with computed input bytes; ordinary ingest also computes SHA-256.
        :param item_id: Optional Item identity to link after registration; None omits linking.
        :param role: Optional link role; None selects primary_payload when item_id is supplied.
        :param metadata: Optional Asset description used for a new identity; existing deduplicated Asset metadata is retained.
        :param placement_hints: Advisory destination placement metadata, retained in retry identity and forwarded during publication.
        :param preferred_store_ref: Optional destination Store UUID; None selects the manager default.
        :param replica_mode: Requested mode for a new or selected destination Replica; default ACTIVE.
        :param verify: Whether to inspect the registered Replica after publication/reuse; False still permits identity hashing and Store commit checks.

        :return: Completed ingest result, including creation/deduplication flags and reported verification; failures may follow publication or earlier metadata writes.
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
        """
        Stat a local path, fill a missing original filename, and ingest an opened binary reader.

        A supplied expected size must match the initial stat before opening. The observed size
        becomes the stream expectation, catching length changes but not every same-size replacement;
        supplied digests add identity checks. Missing metadata/original_name is filled from the
        basename without changing the caller's record. The context closes the file on success or
        failure; paths and symlinks are resolved by normal filesystem operations.

        Example:
            >>> result = manager.ingest_file("/incoming/book.epub", item_id=ItemID(9))  # doctest: +SKIP


        :param path: Local path accepted by pathlib.Path and opened in binary mode.
        :param operation_id: Optional logical-operation UUID; None allocates a new one. Completed retries require an equal normalized request.
        :param expected_size: Optional byte count compared with stat before opening; observed size is passed to ingest_stream.
        :param expected_digests: Expected digests to compare with computed input bytes; ordinary ingest also computes SHA-256.
        :param item_id: Optional Item identity to link after registration; None omits linking.
        :param role: Optional link role; None selects primary_payload when item_id is supplied.
        :param metadata: Optional Asset description used for a new identity; existing deduplicated Asset metadata is retained.
        :param placement_hints: Advisory destination placement metadata, retained in retry identity and forwarded during publication.
        :param preferred_store_ref: Optional destination Store UUID; None selects the manager default.
        :param replica_mode: Requested mode for a new or selected destination Replica; default ACTIVE.
        :param verify: Whether to inspect the registered Replica after publication/reuse; False still permits identity hashing and Store commit checks.

        :return: Completed ingest result, including creation/deduplication flags and reported verification; failures may follow publication or earlier metadata writes.
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

    # Todo: Clearer name - ingest_object_from_store - but keep this as an alias
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
        """
        Prepare an advanced source once or open a conventional source using its advertised
        identity/version evidence.

        An IngestSourceStoreAPI source receives prepare_ingest(inspect=False); a changed preparation
        Location raises StorageIntegrityError before delegation to prepared ingest. Other sources
        contribute their stat digest only when advertised authoritative. Conditional reads pass the
        supplied version when supported and non-None, without inventing one.

        An available size and authoritative SHA-256 select identified ingest; other cases use
        ordinary stream expectations. The reader context closes after delegation. The composed
        manager may attempt native transfer before this conventional fallback.

        Example:
            >>> result = manager.ingest_store_object(source_store, source_store.stat(location))  # doctest: +SKIP

        :param source: Configured source Store supplying inventory/stat metadata and readable bytes.
        :param info: Source Location, size, digest, and optional version evidence; no fresh stat is implicit here.
        :param operation_id: Optional logical-operation UUID; None allocates a new one. Completed retries require an equal normalized request.
        :param item_id: Optional Item identity to link after registration; None omits linking.
        :param role: Optional link role; None selects primary_payload when item_id is supplied.
        :param metadata: Optional Asset description used for a new identity; existing deduplicated Asset metadata is retained.
        :param placement_hints: Advisory destination placement metadata, retained in retry identity and forwarded during publication.
        :param preferred_store_ref: Optional destination Store UUID; None selects the manager default.
        :param replica_mode: Requested mode for a new or selected destination Replica; default ACTIVE.
        :param verify: Whether to inspect the registered Replica after publication/reuse; False still permits identity hashing and Store commit checks.

        :return: Completed ingest result, including creation/deduplication flags and reported verification; failures may follow publication or earlier metadata writes.
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

    # Todo: Bad naming - it's doesn't have to be a prepared object in a store? Just prepared_object? Or clarify the doc string.
    def ingest_prepared_store_object(
        self,
        source: StoreAPI,
        prepared: "PreparedIngestObject",
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
        """
        Validate a retained preparation and ingest its reader without preparing the source again.

        Require IngestSourceStoreAPI and matching Location ownership, then validate the preparation
        against advertised source capabilities. ValueError from capability validation becomes
        StorageIntegrityError; other failures propagate. Open the prepared reader in a context and
        choose identified ingest only when size and authoritative SHA-256 are present, otherwise
        ordinary stream ingest. Inspection, original-name, and metadata enrichment are not inferred
        from the preparation by this wrapper.

        Example:
            >>> prepared = source.prepare_ingest(entry)  # doctest: +SKIP
            >>> result = manager.ingest_prepared_store_object(source, prepared)  # doctest: +SKIP


        :param source: Store implementing IngestSourceStoreAPI and owning the preparation Location.
        :param prepared: Existing preparation whose advertised authority/read-consistency constraints are validated before opening.
        :param operation_id: Optional logical-operation UUID; None allocates a new one. Completed retries require an equal normalized request.
        :param item_id: Optional Item identity to link after registration; None omits linking.
        :param role: Optional link role; None selects primary_payload when item_id is supplied.
        :param metadata: Optional Asset description used for a new identity; existing deduplicated Asset metadata is retained.
        :param placement_hints: Advisory destination placement metadata, retained in retry identity and forwarded during publication.
        :param preferred_store_ref: Optional destination Store UUID; None selects the manager default.
        :param replica_mode: Requested mode for a new or selected destination Replica; default ACTIVE.
        :param verify: Whether to inspect the registered Replica after publication/reuse; False still permits identity hashing and Store commit checks.
        :return: Completed ingest result, including creation/deduplication flags and reported verification; failures may follow publication or earlier metadata writes.
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

    # Todo: If we have a location... why do we need to adopt it? Spec use case
    @abc.abstractmethod
    def adopt_location(
        self,
        location: Location,
        *,
        operation_id: UUID | None = None,
        digital_asset_id: DigitalAssetID | None = None,
        item_id: ItemID | None = None, role: str | None = None,
        metadata: DigitalAssetMetadata | None = None,
        replica_mode: ReplicaMode = ReplicaMode.UNMANAGED, verify: bool = False,
    ) -> DigitalAssetIngestResult:
        """
        Identify and register bytes already stored at a Location without copying them elsewhere.

        The composed manager hashes for identity even when verify is false. A supplied Asset ID must
        match size and comparable digests; otherwise identity is found or declared. New metadata
        does not overwrite a deduplicated Asset, and an existing matching Location claim retains its
        mode. Completed equal operation requests return the recorded result without another stat or
        read. Earlier declaration/Replica writes may survive a later linking or verification
        failure.

        Example:
            >>> result = manager.adopt_location(  # doctest: +SKIP
            ...     Location(UUID(int=1), "legacy/book.epub"), verify=True,
            ... )


        :param location: Concrete registered Store address whose existing bytes should be identified and claimed.
        :param operation_id: Optional logical-operation UUID; None allocates a new one. Completed retries require an equal normalized request.
        :param digital_asset_id: Optional existing Asset identity that the observed size and comparable digests must match.
        :param item_id: Optional Item identity to link after registration; None omits linking.
        :param role: Optional link role; None selects primary_payload when item_id is supplied.
        :param metadata: Optional Asset description used for a new identity; existing deduplicated Asset metadata is retained.
        :param replica_mode: Mode for a newly created claim, default UNMANAGED; a reused claim retains its mode.
        :param verify: Request another Replica verification after registration; False still hashes bytes for adoption identity.

        :return: Completed ingest result, including creation/deduplication flags and reported verification;
                 failures may follow publication or earlier metadata writes.
        """
        ...


__all__ = ["DigitalAssetIngestAPI"]
