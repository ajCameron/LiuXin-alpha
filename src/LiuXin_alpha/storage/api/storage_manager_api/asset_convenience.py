"""Responsibility-specific manager convenience adapter."""

from __future__ import annotations

import os
import shutil
import tempfile
import zipfile
from collections.abc import Iterable, Mapping
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import BinaryIO, cast
from uuid import UUID

from LiuXin_alpha.storage.api.errors import StorageIntegrityError
from LiuXin_alpha.storage.api.models import Digest, StoreUUID
from LiuXin_alpha.storage.api.placement_hints_api import (
    StorageHintSource,
    StoragePlacementHints,
    derive_storage_hints,
)
from LiuXin_alpha.storage.api.storage_manager_api.catalog_api import (
    DigitalAssetRegistryAPI,
)
from LiuXin_alpha.storage.api.storage_manager_api.composites_api import (
    CompositeDigitalAssetAPI,
)
from LiuXin_alpha.storage.api.storage_manager_api.derivations_api import (
    DigitalAssetDerivationRegistryAPI,
)
from LiuXin_alpha.storage.api.storage_manager_api.errors import (
    DigitalAssetNotFound,
)
from LiuXin_alpha.storage.api.storage_manager_api.ingest_api import (
    DigitalAssetIngestAPI,
)
from LiuXin_alpha.storage.api.storage_manager_api.item_links_api import (
    ItemDigitalAssetLinkAPI,
)
from LiuXin_alpha.storage.api.storage_manager_api.models import (
    BackupPolicy,
    BackupPolicyID,
    BackupPolicyRecord,
    CompositeDigitalAssetDeclaration,
    CompositeDigitalAssetID,
    CompositeDigitalAssetMemberResolution,
    CompositeDigitalAssetMembership,
    CompositeDigitalAssetRecord,
    DigitalAssetDeclaration,
    DigitalAssetDerivationDeclaration,
    DigitalAssetDerivationKind,
    DigitalAssetDerivationRecord,
    DigitalAssetDerivationSourceReference,
    DigitalAssetID,
    DigitalAssetIngestResult,
    DigitalAssetLossAction,
    DigitalAssetMetadata,
    DigitalAssetRecord,
    DigitalAssetResolution,
    ItemID,
    ReplicaID,
    ReplicaMode,
    ReplicaRecord,
    ReplicaSeparationDimension,
    ReplicationPolicy,
    ReplicationPolicyID,
    ReplicationPolicyRecord,
    ReproductionRecipe,
    StoreConfiguration,
)
from LiuXin_alpha.storage.api.storage_manager_api.policies_api import (
    StoragePolicyAPI,
)
from LiuXin_alpha.storage.api.storage_manager_api.replicas_api import (
    ReplicaLifecycleAPI,
)
from LiuXin_alpha.storage.api.storage_manager_api.retrieval_api import (
    DigitalAssetRetrievalAPI,
)
from LiuXin_alpha.storage.api.storage_manager_api.router_api import (
    StorageRouterAPI,
)
from LiuXin_alpha.storage.api.store_api.facade_api import StoreAPI

from LiuXin_alpha.storage.api.storage_manager_api._convenience_support import *  # noqa: F403


class DigitalAssetConvenienceMixin:
    """
    Convert ordinary caller values into explicit manager operations without owning state.

    The mixin relies on manager methods supplied by its host; typing casts add no runtime
    implementation check. Ingest conveniences return only Asset records, while explicit ingest APIs
    retain detailed Replica, retry, and verification results. Flat Asset metadata and advisory
    library placement hints remain separate, and lower workflow exceptions and partial effects
    propagate.

    Replica mode defaults to ACTIVE. Store arguments used for reading are preferences rather than
    exact-copy guarantees, and verified selects recorded state without requesting fresh hashes.
    Stream-returning methods transfer ownership to callers; read helpers close readers and
    materialize bytes. Composite ingest/export consists of ordered operations without a new
    cross-member transaction or rollback boundary.

    Example:
        >>> book = manager.store_bytes(  # doctest: +SKIP
        ...     b"book", name="book.epub", item=9,
        ... )
        >>> manager.read_asset(book)  # doctest: +SKIP
        b'book'
    """

    def store(
        self,
        source: bytes | bytearray | memoryview | BinaryIO | str | os.PathLike[str],
        *,
        name: str | None = None,
        media_type: str | None = None,
        original_name: str | None = None,
        attributes: Mapping[str, str] | Iterable[tuple[str, str]] = (),
        metadata: StorageHintSource | None = None,
        item: ItemID | int | None = None,
        role: str = "primary_payload",
        store: StoreUUID | StoreConfiguration | StoreAPI | None = None,
        replica_mode: ReplicaMode | str | None = None,
        verify: bool = True,
        operation_id: UUID | None = None,
        expected_size: int | None = None,
        expected_digests: Iterable[Digest] = (),
        mode: ReplicaMode | str | None = None,
    ) -> DigitalAssetRecord:
        """
        Dispatch bytes-like data, a local path, or a read-bearing object to typed ingest helpers.

        Bytes, bytearray, and memoryview become bytes; a supplied size mismatch rejects before
        store_bytes is called. Strings and PathLike objects go to store_file, including strings that
        resemble URLs or digests. Other objects need only expose a read attribute here, not prove it
        is callable or binary, before store_stream is called. Unsupported shapes raise TypeError;
        attribute-access errors propagate.

        Forward the remaining controls to the selected convenience method. Source ownership, retry
        consumption, verification, and later publication/metadata failures follow that path. The
        returned record omits the detailed ingest result.

        Example:
            >>> asset = manager.store(  # doctest: +SKIP
            ...     b"cover", name="cover.jpg", metadata=item_metadata,
            ... )


        :param source: Bytes-like data copied to bytes, a local path, or an object with a read attribute.
        :param name: Optional Asset display name, retained without stripping; None leaves it unspecified.
        :param media_type: Optional Asset media-type text; this wrapper does not infer it from bytes or a filename.
        :param original_name: Optional original filename/source label for Asset metadata, separate from a physical Store key.
        :param attributes: Ordered string name/value pairs or a mapping for Asset metadata; pairs are collected before delegation.
        :param metadata: Optional rich library metadata projected into advisory Store placement hints, separate from Asset identity metadata.
        :param item: Optional positive non-bool integer Item ID to link after ingest; None omits linking.
        :param role: Exact Item-link role forwarded to ingest; defaults to primary_payload.
        :param store: Destination Store UUID or UUID-bearing configuration/facade; None delegates default selection to the manager.
        :param replica_mode: Requested Replica mode or exact enum-value string; None selects ACTIVE when mode is also None.
        :param verify: Whether ingest inspects the registered Replica; False still permits input hashing and Store commit checks.
        :param operation_id: Optional logical-ingest UUID forwarded for provider retry handling; None lets the provider allocate one.
        :param expected_size: Optional expected total byte count forwarded to ingest for validation.
        :param expected_digests: Iterable of expected Digest values collected into a tuple before the underlying ingest call.
        :param mode: Compatibility alias for replica_mode; supplying both non-None arguments raises TypeError even when equal.
        :return: The Asset record returned by the selected ingest convenience, including an existing record on deduplication.
        """

        if isinstance(source, (bytes, bytearray, memoryview)):
            data = bytes(source)
            if expected_size is not None and expected_size != len(data):
                raise StorageIntegrityError(
                    f"expected {expected_size} bytes, received {len(data)}."
                )
            return self.store_bytes(
                data,
                name=name,
                media_type=media_type,
                original_name=original_name,
                attributes=attributes,
                metadata=metadata,
                item=item,
                role=role,
                store=store,
                replica_mode=replica_mode,
                verify=verify,
                operation_id=operation_id,
                expected_digests=expected_digests,
                mode=mode,
            )
        if isinstance(source, (str, os.PathLike)):
            return self.store_file(
                source,
                expected_size=expected_size,
                expected_digests=expected_digests,
                name=name,
                media_type=media_type,
                original_name=original_name,
                attributes=attributes,
                metadata=metadata,
                item=item,
                role=role,
                store=store,
                replica_mode=replica_mode,
                verify=verify,
                operation_id=operation_id,
                mode=mode,
            )
        if not hasattr(source, "read"):
            raise TypeError("source must be bytes, a binary stream, or a local path.")
        return self.store_stream(
            source,
            expected_size=expected_size,
            expected_digests=expected_digests,
            name=name,
            media_type=media_type,
            original_name=original_name,
            attributes=attributes,
            metadata=metadata,
            item=item,
            role=role,
            store=store,
            replica_mode=replica_mode,
            verify=verify,
            operation_id=operation_id,
            mode=mode,
        )

    def store_bytes(
        self,
        data: bytes,
        *,
        name: str | None = None,
        media_type: str | None = None,
        original_name: str | None = None,
        attributes: Mapping[str, str] | Iterable[tuple[str, str]] = (),
        metadata: StorageHintSource | None = None,
        item: ItemID | int | None = None,
        role: str = "primary_payload",
        store: StoreUUID | StoreConfiguration | StoreAPI | None = None,
        replica_mode: ReplicaMode | str | None = None,
        verify: bool = True,
        operation_id: UUID | None = None,
        expected_digests: Iterable[Digest] = (),
        mode: ReplicaMode | str | None = None,
    ) -> DigitalAssetRecord:
        """
        Normalize ingest controls and return only ingest_bytes(...).asset_record.

        Collect expected digests, validate an optional Item ID, build flat Asset metadata, derive
        optional placement hints, extract the Store UUID, and select the Replica mode before calling
        the host ingest API. The data argument itself is forwarded without copying or a new runtime
        type check. Existing deduplicated Asset metadata follows the provider's rules rather than
        being replaced here.

        verify requests post-registration inspection but does not make this wrapper check a healthy
        result. Creation/deduplication flags, warnings, and detailed verification evidence are
        discarded when the Asset record is projected.

        Example:
            >>> asset = manager.store_bytes(  # doctest: +SKIP
            ...     b"book", original_name="book.epub",
            ... )


        :param data: Byte payload forwarded unchanged to ingest_bytes; use store for bytes-like conversion.
        :param name: Optional Asset display name, retained without stripping; None leaves it unspecified.
        :param media_type: Optional Asset media-type text; this wrapper does not infer it from bytes or a filename.
        :param original_name: Optional original filename/source label for Asset metadata, separate from a physical Store key.
        :param attributes: Ordered string name/value pairs or a mapping for Asset metadata; pairs are collected before delegation.
        :param metadata: Optional rich library metadata projected into advisory Store placement hints, separate from Asset identity metadata.
        :param item: Optional positive non-bool integer Item ID to link after ingest; None omits linking.
        :param role: Exact Item-link role forwarded to ingest; defaults to primary_payload.
        :param store: Destination Store UUID or UUID-bearing configuration/facade; None delegates default selection to the manager.
        :param replica_mode: Requested Replica mode or exact enum-value string; None selects ACTIVE when mode is also None.
        :param verify: Whether ingest inspects the registered Replica; False still permits input hashing and Store commit checks.
        :param operation_id: Optional logical-ingest UUID forwarded for provider retry handling; None lets the provider allocate one.
        :param expected_digests: Iterable of expected Digest values collected into a tuple before the underlying ingest call.
        :param mode: Compatibility alias for replica_mode; supplying both non-None arguments raises TypeError even when equal.
        :return: The ingest result's Asset record; detailed outcome flags and Replica evidence remain available through the explicit ingest API.
        """

        result = cast(
            DigitalAssetIngestAPI,
            cast(object, self),
        ).ingest_bytes(
            data,
            operation_id=operation_id,
            expected_digests=tuple(expected_digests),
            item_id=_item_id(item),
            role=role,
            metadata=_metadata(
                name,
                media_type,
                original_name,
                attributes,
            ),
            placement_hints=_placement_hints(metadata),
            preferred_store_ref=_store_ref(store),
            replica_mode=_replica_mode_argument(replica_mode, mode),
            verify=verify,
        )
        return result.asset_record

    def store_stream(
        self,
        source: BinaryIO,
        *,
        expected_size: int | None = None,
        expected_digests: Iterable[Digest] = (),
        name: str | None = None,
        media_type: str | None = None,
        original_name: str | None = None,
        attributes: Mapping[str, str] | Iterable[tuple[str, str]] = (),
        metadata: StorageHintSource | None = None,
        item: ItemID | int | None = None,
        role: str = "primary_payload",
        store: StoreUUID | StoreConfiguration | StoreAPI | None = None,
        replica_mode: ReplicaMode | str | None = None,
        verify: bool = True,
        operation_id: UUID | None = None,
        mode: ReplicaMode | str | None = None,
    ) -> DigitalAssetRecord:
        """
        Forward a caller-owned stream and normalized metadata/expectations to ingest_stream.

        The wrapper neither enters, rewinds, nor closes the stream. Input consumption and retry
        handling belong to ingest_stream; a retry may still consume supplied bytes. Digest
        expectations are collected once and mode aliases are resolved before delegation. Identity
        hashing and Store commit checks can still occur when verify is false. Return only the Asset
        record without inspecting detailed health flags or undoing earlier publication/metadata on a
        later failure.

        Example:
            >>> asset = manager.store_stream(  # doctest: +SKIP
            ...     source, expected_size=4, name="book",
            ... )


        :param source: Caller-owned binary stream consumed by the ingest implementation from its current position.
        :param expected_size: Optional expected total byte count forwarded to ingest for validation.
        :param expected_digests: Iterable of expected Digest values collected into a tuple before the underlying ingest call.
        :param name: Optional Asset display name, retained without stripping; None leaves it unspecified.
        :param media_type: Optional Asset media-type text; this wrapper does not infer it from bytes or a filename.
        :param original_name: Optional original filename/source label for Asset metadata, separate from a physical Store key.
        :param attributes: Ordered string name/value pairs or a mapping for Asset metadata; pairs are collected before delegation.
        :param metadata: Optional rich library metadata projected into advisory Store placement hints, separate from Asset identity metadata.
        :param item: Optional positive non-bool integer Item ID to link after ingest; None omits linking.
        :param role: Exact Item-link role forwarded to ingest; defaults to primary_payload.
        :param store: Destination Store UUID or UUID-bearing configuration/facade; None delegates default selection to the manager.
        :param replica_mode: Requested Replica mode or exact enum-value string; None selects ACTIVE when mode is also None.
        :param verify: Whether ingest inspects the registered Replica; False still permits input hashing and Store commit checks.
        :param operation_id: Optional logical-ingest UUID forwarded for provider retry handling; None lets the provider allocate one.
        :param mode: Compatibility alias for replica_mode; supplying both non-None arguments raises TypeError even when equal.
        :return: The Asset record from the completed ingest result.
        """

        result = cast(
            DigitalAssetIngestAPI,
            cast(object, self),
        ).ingest_stream(
            source,
            operation_id=operation_id,
            expected_size=expected_size,
            expected_digests=tuple(expected_digests),
            item_id=_item_id(item),
            role=role,
            metadata=_metadata(
                name,
                media_type,
                original_name,
                attributes,
            ),
            placement_hints=_placement_hints(metadata),
            preferred_store_ref=_store_ref(store),
            replica_mode=_replica_mode_argument(replica_mode, mode),
            verify=verify,
        )
        return result.asset_record

    def store_file(
        self,
        path: str | os.PathLike[str],
        *,
        expected_size: int | None = None,
        expected_digests: Iterable[Digest] = (),
        name: str | None = None,
        media_type: str | None = None,
        original_name: str | None = None,
        attributes: Mapping[str, str] | Iterable[tuple[str, str]] = (),
        metadata: StorageHintSource | None = None,
        item: ItemID | int | None = None,
        role: str = "primary_payload",
        store: StoreUUID | StoreConfiguration | StoreAPI | None = None,
        replica_mode: ReplicaMode | str | None = None,
        verify: bool = True,
        operation_id: UUID | None = None,
        mode: ReplicaMode | str | None = None,
    ) -> DigitalAssetRecord:
        """
        Delegate local-file ingest with normalized description, placement, and retry controls.

        The inherited ingest_file implementation stats before opening, checks a supplied expected
        size, fills a None original_name from the basename, and closes its own binary file context
        after ingest. Its observed size becomes the stream expectation; that is not protection
        against every same-size file replacement. This wrapper does not expand a leading tilde,
        interpret a URL, infer a media type, or open the file itself. Host overrides and provider
        failures remain authoritative.

        Example:
            >>> asset = manager.store_file(  # doctest: +SKIP
            ...     "/incoming/book.epub", media_type="application/epub+zip",
            ... )


        :param path: Local filesystem path forwarded to ingest_file and ordinarily interpreted by pathlib.Path.
        :param expected_size: Optional expected total byte count forwarded to ingest for validation.
        :param expected_digests: Iterable of expected Digest values collected into a tuple before the underlying ingest call.
        :param name: Optional Asset display name, retained without stripping; None leaves it unspecified.
        :param media_type: Optional Asset media-type text; this wrapper does not infer it from bytes or a filename.
        :param original_name: Optional original filename/source label for Asset metadata, separate from a physical Store key.
        :param attributes: Ordered string name/value pairs or a mapping for Asset metadata; pairs are collected before delegation.
        :param metadata: Optional rich library metadata projected into advisory Store placement hints, separate from Asset identity metadata.
        :param item: Optional positive non-bool integer Item ID to link after ingest; None omits linking.
        :param role: Exact Item-link role forwarded to ingest; defaults to primary_payload.
        :param store: Destination Store UUID or UUID-bearing configuration/facade; None delegates default selection to the manager.
        :param replica_mode: Requested Replica mode or exact enum-value string; None selects ACTIVE when mode is also None.
        :param verify: Whether ingest inspects the registered Replica; False still permits input hashing and Store commit checks.
        :param operation_id: Optional logical-ingest UUID forwarded for provider retry handling; None lets the provider allocate one.
        :param mode: Compatibility alias for replica_mode; supplying both non-None arguments raises TypeError even when equal.
        :return: The Asset record from the file ingest result, without its detailed flags or Replica report.
        """

        result = cast(
            DigitalAssetIngestAPI,
            cast(object, self),
        ).ingest_file(
            path,
            operation_id=operation_id,
            expected_size=expected_size,
            expected_digests=tuple(expected_digests),
            item_id=_item_id(item),
            role=role,
            metadata=_metadata(
                name,
                media_type,
                original_name,
                attributes,
            ),
            placement_hints=_placement_hints(metadata),
            preferred_store_ref=_store_ref(store),
            replica_mode=_replica_mode_argument(replica_mode, mode),
            verify=verify,
        )
        return result.asset_record

    def declare_asset(
        self,
        size: int,
        digests: Mapping[str, str] | Iterable[Digest],
        *,
        name: str | None = None,
        media_type: str | None = None,
        original_name: str | None = None,
        attributes: Mapping[str, str] | Iterable[tuple[str, str]] = (),
        replication: ReplicationPolicyID | ReplicationPolicyRecord | None = None,
        backup: BackupPolicyID | BackupPolicyRecord | None = None,
    ) -> DigitalAssetRecord:
        """
        Build a content declaration and delegate registration without supplying bytes.

        Convert digest mappings to Digest objects, validate/collect flat attributes, extract
        optional policy IDs, then construct the declaration. Constructors perform their selected
        value checks and the manager owns identity/reference registration. No file is opened,
        Replica published, or digest computed by this convenience; declaration evidence can describe
        currently absent bytes.

        Example:
            >>> asset = manager.declare_asset(  # doctest: +SKIP
            ...     4, {"sha256": "abcd"}, name="known object",
            ... )


        :param size: Declared byte count passed unchanged to DigitalAssetDeclaration for validation.
        :param digests: Algorithm/value mapping or iterable of Digest objects; declaration validation requires nonempty unique algorithms.
        :param name: Optional Asset display name, retained without stripping; None leaves it unspecified.
        :param media_type: Optional Asset media-type text; this wrapper does not infer it from bytes or a filename.
        :param original_name: Optional original filename/source label for Asset metadata, separate from a physical Store key.
        :param attributes: Ordered string name/value pairs or a mapping for Asset metadata; pairs are collected before delegation.
        :param replication: Optional replication-policy record or positive non-bool integer ID; record IDs are retained without revalidation.
        :param backup: Optional backup-policy record or positive non-bool integer ID; record IDs are retained without revalidation.
        :return: The registered Asset record returned by declare_digital_asset.
        """

        return cast(
            DigitalAssetRegistryAPI,
            cast(object, self),
        ).declare_digital_asset(
            DigitalAssetDeclaration(
                size,
                _digests(digests),
                _metadata(
                    name,
                    media_type,
                    original_name,
                    attributes,
                ),
                _replication_policy_id(replication),
                _backup_policy_id(backup),
            )
        )

    def open_asset(
        self,
        asset: (
            DigitalAssetID
            | DigitalAssetRecord
            | DigitalAssetIngestResult
            | DigitalAssetResolution
        ),
        *,
        store: StoreUUID | StoreConfiguration | StoreAPI | None = None,
        replica_mode: ReplicaMode | str | None = None,
        verified: bool = False,
        offset: int = 0,
        length: int | None = None,
        mode: ReplicaMode | str | None = None,
    ) -> BinaryIO:
        """
        Resolve an atomic Asset ID, select a Replica, and open its Location for binary reading.

        Supplied records/results/resolutions contribute only their Asset ID; an existing resolution
        does not pin its Replica. Normalize the Store preference and mode, request recorded
        verification when selected, then call router.get with offset and length. No version
        precondition is forwarded between selection and reading. Lookup, selection, and open
        failures propagate. The caller owns the reader, and this wrapper does not rehash returned
        bytes or enter its context.

        Example:
            >>> with manager.open_asset(asset) as source:  # doctest: +SKIP
            ...     header = source.read(4)


        :param asset: Positive integer Asset ID or an Asset record, ingest result, or resolution whose retained Asset ID is extracted.
        :param store: Optional Store UUID or UUID-bearing object used as a preference; the default selector can choose another eligible Store.
        :param replica_mode: Requested Replica mode or exact enum-value string; None selects ACTIVE when mode is also None.
        :param verified: Whether selection requires recorded VERIFIED state; it does not request fresh digest verification.
        :param offset: Zero-based byte offset forwarded to the selected reader without validation by this wrapper.
        :param length: Optional requested byte-range length, or None for the remaining object; no independent memory limit is added.
        :param mode: Compatibility alias for replica_mode; supplying both non-None arguments raises TypeError even when equal.
        :return: The selected Store reader, which the caller must close.
        """

        resolution = cast(
            DigitalAssetRetrievalAPI,
            cast(object, self),
        ).resolve_digital_asset(
            _asset_id(asset),
            preferred_store_ref=_store_ref(store),
            mode=_replica_mode_argument(replica_mode, mode),
            require_verified=verified,
        )
        return cast(StorageRouterAPI, cast(object, self)).get(
            resolution.location,
            offset=offset,
            length=length,
        )

    def read_asset(
        self,
        asset: (
            DigitalAssetID
            | DigitalAssetRecord
            | DigitalAssetIngestResult
            | DigitalAssetResolution
        ),
        *,
        store: StoreUUID | StoreConfiguration | StoreAPI | None = None,
        replica_mode: ReplicaMode | str | None = None,
        verified: bool = False,
        offset: int = 0,
        length: int | None = None,
        mode: ReplicaMode | str | None = None,
    ) -> bytes:
        """
        Open an Asset through self.open_asset, read it once to completion, and close the reader.

        Forward all selection and range controls dynamically. The method calls read() without a size
        argument and adds no memory cap or byte-type validation beyond the provider contract. Read
        and context-cleanup failures propagate; a close failure can prevent return even after all
        bytes were read.

        Example:
            >>> manager.read_asset(asset, length=4)  # doctest: +SKIP
            b'book'


        :param asset: Positive integer Asset ID or an Asset record, ingest result, or resolution whose retained Asset ID is extracted.
        :param store: Optional Store UUID or UUID-bearing object used as a preference; the default selector can choose another eligible Store.
        :param replica_mode: Requested Replica mode or exact enum-value string; None selects ACTIVE when mode is also None.
        :param verified: Whether selection requires recorded VERIFIED state; it does not request fresh digest verification.
        :param offset: Zero-based byte offset forwarded to the selected reader without validation by this wrapper.
        :param length: Optional requested byte-range length, or None for the remaining object; no independent memory limit is added.
        :param mode: Compatibility alias for replica_mode; supplying both non-None arguments raises TypeError even when equal.
        :return: The reader's read() result, expected to be bytes for the requested range.
        """

        with self.open_asset(
            asset,
            store=store,
            replica_mode=replica_mode,
            verified=verified,
            offset=offset,
            length=length,
            mode=mode,
        ) as source:
            return source.read()

    def open_file(
        self,
        identifier: DigitalAssetFileIdentifier,
        *,
        algorithm: str = "sha256",
        size: int | None = None,
        store: StoreUUID | StoreConfiguration | StoreAPI | None = None,
        replica_mode: ReplicaMode | str | None = None,
        verified: bool = False,
        offset: int = 0,
        length: int | None = None,
        mode: ReplicaMode | str | None = None,
    ) -> BinaryIO:
        """
        Resolve an Asset by ID or digest, then delegate a read-only open to self.open_asset.

        Strings are digest values using algorithm, never paths or textual IDs. Digest objects retain
        their own algorithm, while direct ID/record/result/resolution inputs ignore algorithm and
        size. Missing digest matches raise DigitalAssetNotFound; later selection can raise
        NoReadableReplica. Mode names describe Replica roles, not file write modes. The stream
        remains caller-owned and selection adds no version pin or fresh digest verification.

        Example:
            >>> with manager.open_file(7) as source:  # doctest: +SKIP
            ...     payload = source.read()
            >>> with manager.open_file("a" * 64) as source:  # doctest: +SKIP
            ...     same_payload = source.read()


        :param identifier: Asset ID/record/result/resolution, a Digest, or digest text; strings are never local paths here.
        :param algorithm: Digest algorithm used only for a string identifier; Digest values retain their own algorithm.
        :param size: Optional exact Asset size used only with digest lookup; ignored for direct ID/record inputs.
        :param store: Optional Store UUID or UUID-bearing object used as a preference; the default selector can choose another eligible Store.
        :param replica_mode: Requested Replica mode or exact enum-value string; None selects ACTIVE when mode is also None.
        :param verified: Whether selection requires recorded VERIFIED state; it does not request fresh digest verification.
        :param offset: Zero-based byte offset forwarded to the selected reader without validation by this wrapper.
        :param length: Optional requested byte-range length, or None for the remaining object; no independent memory limit is added.
        :param mode: Compatibility alias for replica_mode; supplying both non-None arguments raises TypeError even when equal.
        :return: The caller-owned reader returned by open_asset after identifier resolution.
        """

        asset_id = _file_asset_id(
            self,
            identifier,
            algorithm=algorithm,
            size=size,
        )
        return self.open_asset(
            asset_id,
            store=store,
            replica_mode=replica_mode,
            verified=verified,
            offset=offset,
            length=length,
            mode=mode,
        )

    def get_file(
        self,
        identifier: DigitalAssetFileIdentifier,
        *,
        algorithm: str = "sha256",
        size: int | None = None,
        store: StoreUUID | StoreConfiguration | StoreAPI | None = None,
        replica_mode: ReplicaMode | str | None = None,
        verified: bool = False,
        offset: int = 0,
        length: int | None = None,
        mode: ReplicaMode | str | None = None,
    ) -> BinaryIO:
        """
        Forward every argument to self.open_file and return its reader unchanged. Dynamic overrides
        retain control of lookup and reader creation; this alias does not cache a result, enter the
        stream context, or add cleanup.

        Example:
            >>> with manager.get_file(7) as source:  # doctest: +SKIP
            ...     payload = source.read()


        :param identifier: Asset ID/record/result/resolution, a Digest, or digest text; strings are never local paths here.
        :param algorithm: Digest algorithm used only for a string identifier; Digest values retain their own algorithm.
        :param size: Optional exact Asset size used only with digest lookup; ignored for direct ID/record inputs.
        :param store: Optional Store UUID or UUID-bearing object used as a preference; the default selector can choose another eligible Store.
        :param replica_mode: Requested Replica mode or exact enum-value string; None selects ACTIVE when mode is also None.
        :param verified: Whether selection requires recorded VERIFIED state; it does not request fresh digest verification.
        :param offset: Zero-based byte offset forwarded to the selected reader without validation by this wrapper.
        :param length: Optional requested byte-range length, or None for the remaining object; no independent memory limit is added.
        :param mode: Compatibility alias for replica_mode; supplying both non-None arguments raises TypeError even when equal.
        :return: The same caller-owned stream returned by open_file.
        """

        return self.open_file(
            identifier,
            algorithm=algorithm,
            size=size,
            store=store,
            replica_mode=replica_mode,
            verified=verified,
            offset=offset,
            length=length,
            mode=mode,
        )

    def read_file(
        self,
        identifier: DigitalAssetFileIdentifier,
        *,
        algorithm: str = "sha256",
        size: int | None = None,
        store: StoreUUID | StoreConfiguration | StoreAPI | None = None,
        replica_mode: ReplicaMode | str | None = None,
        verified: bool = False,
        offset: int = 0,
        length: int | None = None,
        mode: ReplicaMode | str | None = None,
    ) -> bytes:
        """
        Open by ID or digest through self.open_file, materialize read(), and close the reader.

        Identifier interpretation, selection, and ranges follow open_file. No additional total-byte
        cap or result-type check is imposed. Lookup, read, and cleanup errors propagate, including a
        close failure after a complete read.

        Example:
            >>> manager.read_file(7)  # doctest: +SKIP
            b'book'
            >>> manager.read_file(Digest("sha256", "a" * 64))  # doctest: +SKIP
            b'book'


        :param identifier: Asset ID/record/result/resolution, a Digest, or digest text; strings are never local paths here.
        :param algorithm: Digest algorithm used only for a string identifier; Digest values retain their own algorithm.
        :param size: Optional exact Asset size used only with digest lookup; ignored for direct ID/record inputs.
        :param store: Optional Store UUID or UUID-bearing object used as a preference; the default selector can choose another eligible Store.
        :param replica_mode: Requested Replica mode or exact enum-value string; None selects ACTIVE when mode is also None.
        :param verified: Whether selection requires recorded VERIFIED state; it does not request fresh digest verification.
        :param offset: Zero-based byte offset forwarded to the selected reader without validation by this wrapper.
        :param length: Optional requested byte-range length, or None for the remaining object; no independent memory limit is added.
        :param mode: Compatibility alias for replica_mode; supplying both non-None arguments raises TypeError even when equal.
        :return: The binary reader's complete read() result for the selected range.
        """

        with self.open_file(
            identifier,
            algorithm=algorithm,
            size=size,
            store=store,
            replica_mode=replica_mode,
            verified=verified,
            offset=offset,
            length=length,
            mode=mode,
        ) as source:
            return source.read()

    def replicate_asset(
        self,
        asset: (
            DigitalAssetID
            | DigitalAssetRecord
            | DigitalAssetIngestResult
            | DigitalAssetResolution
        ),
        *,
        to: StoreUUID | StoreConfiguration | StoreAPI | None = None,
        from_replica: ReplicaID | ReplicaRecord | None = None,
        metadata: StorageHintSource | None = None,
        replica_mode: ReplicaMode | str | None = None,
        verify: bool = True,
        mode: ReplicaMode | str | None = None,
    ) -> ReplicaRecord:
        """
        Normalize Asset/source/destination controls and delegate Replica publication.

        Extract IDs from retained records without pinning their revision or state. None placement
        metadata becomes None hints, allowing the concrete workflow to inherit source hints; an
        explicit projection overrides them. Mode defaults to ACTIVE. The underlying workflow owns
        source validation, publication, claim registration, and optional inspection. Its returned
        claim can be unhealthy after verification, and this wrapper adds no rollback or health
        assertion.

        Example:
            >>> replica = manager.replicate_asset(  # doctest: +SKIP
            ...     asset, to=archive_store,
            ... )


        :param asset: Positive integer Asset ID or an Asset record, ingest result, or resolution whose retained Asset ID is extracted.
        :param to: Destination Store UUID or UUID-bearing configuration/facade; None delegates default selection to the manager.
        :param from_replica: Optional source Replica record or positive non-bool integer ID; None delegates source selection.
        :param metadata: Optional rich library metadata projected into advisory Store placement hints, separate from Asset identity metadata.
        :param replica_mode: Requested Replica mode or exact enum-value string; None selects ACTIVE when mode is also None.
        :param verify: Whether the underlying replication workflow inspects the new claim after publication; healthy state is not guaranteed.
        :param mode: Compatibility alias for replica_mode; supplying both non-None arguments raises TypeError even when equal.
        :return: The Replica record returned by the replication workflow.
        """

        return cast(
            ReplicaLifecycleAPI,
            cast(object, self),
        ).replicate_digital_asset(
            _asset_id(asset),
            destination_store_ref=_store_ref(to),
            source_replica_id=_replica_id(from_replica),
            placement_hints=_placement_hints(metadata),
            mode=_replica_mode_argument(replica_mode, mode),
            verify=verify,
        )


__all__ = ["DigitalAssetConvenienceMixin"]
