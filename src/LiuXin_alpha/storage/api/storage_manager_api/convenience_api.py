"""
Adapt ordinary storage inputs to explicit manager ingest, retrieval, and policy APIs.

This state-free mixin normalizes IDs, enum values, descriptions, and advisory
placement hints, then delegates to host manager methods. Record-returning ingest
shortcuts omit detailed result evidence. Read helpers distinguish caller-owned
streams from materialized bytes; string file identifiers are digests, whereas
string ingest sources are local paths.

Composite ingest and directory/ZIP delivery perform ordered operations without
an aggregate transaction or pinned Replica snapshot. Path preflight, reader
cleanup, and partial-publication limits are described at those operations.
Private helpers document their exact validation and coercion boundaries.
"""

# Todo: This is a kitchien sink module - split it down into convenience sub-classes which can live in the same modules as
#  the regular parts - e.g. the derivations part should live in the derivations_api class

from __future__ import annotations

import os
import shutil
import tempfile
import zipfile

from collections.abc import Iterable, Mapping
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import BinaryIO, TypeAlias, cast
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
from LiuXin_alpha.storage.api.storage_manager_api.ingest_api import (
    DigitalAssetIngestAPI,
)
from LiuXin_alpha.storage.api.storage_manager_api.errors import (
    DigitalAssetNotFound,
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


_AssetInput: TypeAlias = (
    DigitalAssetID
    | DigitalAssetRecord
    | DigitalAssetIngestResult
    | DigitalAssetResolution
)
DigitalAssetFileIdentifier: TypeAlias = _AssetInput | int | Digest | str
_CompositeInput: TypeAlias = (
    CompositeDigitalAssetID | CompositeDigitalAssetRecord
)
_StoreInput: TypeAlias = StoreUUID | StoreConfiguration | StoreAPI
_ReplicaInput: TypeAlias = ReplicaID | ReplicaRecord
_AttributeInput: TypeAlias = (
    Mapping[str, str] | Iterable[tuple[str, str]]
)
_DigestInput: TypeAlias = Mapping[str, str] | Iterable[Digest]
_DerivationSourceInput: TypeAlias = (
    _AssetInput | CompositeDigitalAssetRecord
)
_StorableSource: TypeAlias = (
    bytes | bytearray | memoryview | BinaryIO | str | os.PathLike[str]
)


# Todo: These are good! But we need extension - to handle more things the StorageManager can do
# Todo: This module is already too long
class StorageConvenienceAPI:
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
            raise TypeError(
                "source must be bytes, a binary stream, or a local path."
            )
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

    def link(
        self,
        item: ItemID | int,
        asset: (
            DigitalAssetID
            | DigitalAssetRecord
            | DigitalAssetIngestResult
            | DigitalAssetResolution
            | CompositeDigitalAssetID
            | CompositeDigitalAssetRecord
        ),
        *,
        role: str = "primary_payload",
        composite: bool = False,
    ) -> None:
        """
        Associate an Item role with an atomic or Composite Asset through the relevant manager API.

        Require a positive non-bool integer Item ID first. A Composite record selects the Composite
        path automatically; otherwise a truthy composite flag selects it. Nominal IDs are runtime
        integers, so a raw Composite ID needs that flag to avoid atomic interpretation. Exact role
        text is forwarded and no revision, reference lookup, or metadata transaction is added by
        this wrapper.

        Example:
            >>> manager.link(9, cover, role="cover")  # doctest: +SKIP


        :param item: Required positive non-bool integer Item identity.
        :param asset: Atomic ID/record/result/resolution, or Composite record/ID for the Composite path.
        :param role: Exact role text identifying the association; defaults to primary_payload.
        :param composite: Whether to interpret a raw identifier as Composite; Composite records select that path automatically.
        :return: None after the selected link operation returns.
        """

        item_id = _required_item_id(item)
        if composite or isinstance(asset, CompositeDigitalAssetRecord):
            cast(
                ItemDigitalAssetLinkAPI,
                cast(object, self),
            ).link_item_to_composite_digital_asset(
                item_id,
                _composite_id(cast(_CompositeInput, asset)),
                role=role,
            )
            return
        cast(
            ItemDigitalAssetLinkAPI,
            cast(object, self),
        ).link_item_to_digital_asset(
            item_id,
            _asset_id(cast(_AssetInput, asset)),
            role=role,
        )

    def unlink(
        self,
        item: ItemID | int,
        *,
        role: str = "primary_payload",
    ) -> bool:
        """
        Validate the Item ID and delegate removal of its exact role association. The underlying
        manager determines whether a link existed and applies persistence/reference behavior; the
        wrapper performs no physical-byte deletion.

        Example:
            >>> manager.unlink(9, role="cover")  # doctest: +SKIP
            True


        :param item: Required positive non-bool integer Item ID.
        :param role: Exact Item-link role to remove, defaulting to primary_payload.
        :return: The boolean reported by unlink_item_digital_asset for the requested association.
        """

        return cast(
            ItemDigitalAssetLinkAPI,
            cast(object, self),
        ).unlink_item_digital_asset(
            _required_item_id(item),
            role=role,
        )

    def create_composite(
        self,
        members: (
            Mapping[
                str,
                DigitalAssetID
                | DigitalAssetRecord
                | DigitalAssetIngestResult
                | DigitalAssetResolution,
            ]
            | Iterable[
                DigitalAssetID
                | DigitalAssetRecord
                | DigitalAssetIngestResult
                | DigitalAssetResolution
            ]
        ),
        *,
        name: str | None = None,
        attributes: Mapping[str, str] | Iterable[tuple[str, str]] = (),
    ) -> CompositeDigitalAssetRecord:
        """
        Declare ordered required memberships from atomic records/IDs without ingesting bytes.

        Mapping insertion order supplies sequence numbers and mapping keys become logical_path
        values. Other iterables supply unnamed memberships in iteration order. Values contribute
        only their Asset IDs. Collect all members, normalize attributes, construct a declaration,
        then delegate manager registration. Membership/declaration constructors validate selected
        fields, but this method does not run the stricter filesystem delivery-path check;
        traversal-like logical text may be stored and rejected later during export.

        Example:
            >>> composite = manager.create_composite(  # doctest: +SKIP
            ...     {"book.epub": book, "cover.jpg": cover},
            ...     name="book package",
            ... )


        :param members: Mapping from logical path to atomic Asset input, or ordered iterable of atomic inputs; all become required members.
        :param name: Optional nonblank Composite name retained by the declaration.
        :param attributes: Ordered string name/value pairs or mapping collected for Composite metadata.
        :return: The Composite record returned by declare_composite_digital_asset.
        """

        if isinstance(members, Mapping):
            member_mapping = cast(Mapping[str, _AssetInput], members)
            memberships = tuple(
                CompositeDigitalAssetMembership(
                    _asset_id(asset),
                    sequence_number,
                    logical_path=logical_path,
                )
                for sequence_number, (logical_path, asset) in enumerate(
                    member_mapping.items()
                )
            )
        else:
            member_values = members
            memberships = tuple(
                CompositeDigitalAssetMembership(
                    _asset_id(asset),
                    sequence_number,
                )
                for sequence_number, asset in enumerate(member_values)
            )
        return cast(
            CompositeDigitalAssetAPI,
            cast(object, self),
        ).declare_composite_digital_asset(
            CompositeDigitalAssetDeclaration(
                memberships,
                name=name,
                attributes=_attributes(attributes),
            )
        )

    def store_composite(
        self,
        members: Mapping[str, _StorableSource],
        *,
        name: str | None = None,
        attributes: Mapping[str, str] | Iterable[tuple[str, str]] = (),
        metadata: StorageHintSource | None = None,
        item: ItemID | int | None = None,
        role: str = "primary_payload",
        store: StoreUUID | StoreConfiguration | StoreAPI | None = None,
        replica_mode: ReplicaMode | str | None = None,
        verify: bool = True,
        mode: ReplicaMode | str | None = None,
    ) -> CompositeDigitalAssetRecord:
        """
        Ingest named members in mapping order, then declare their Composite and optionally link an
        Item.

        Reject an empty mapping. Validate each logical delivery path immediately before ingesting
        that member, using its basename as original_name and forwarding common
        placement/mode/verification controls. Per-member calls do not receive Item linkage, the
        Composite name/attributes, explicit expected digests, or a shared operation UUID. All
        members must finish before create_composite runs.

        There is no aggregate transaction or cleanup: a bad later path/source can leave earlier
        Assets and bytes. Composite metadata validation happens after member ingest, and Item
        ID/link validation happens after Composite registration, so those failures can leave the
        completed lower-level objects in place.

        Example:
            >>> package = manager.store_composite(  # doctest: +SKIP
            ...     {"book.epub": book_bytes, "images/cover.jpg": cover},
            ...     name="book package",
            ... )


        :param members: Nonempty mapping from strict relative POSIX delivery paths to sources accepted by store.
        :param name: Optional Composite name applied only after all members are ingested.
        :param attributes: Composite-only string attributes normalized during final declaration.
        :param metadata: Optional rich library metadata projected into advisory Store placement hints, separate from Asset identity metadata.
        :param item: Optional Item ID linked to the completed Composite; validation occurs after its declaration.
        :param role: Role used only for the final optional Composite/Item association.
        :param store: Destination Store UUID or UUID-bearing configuration/facade; None delegates default selection to the manager.
        :param replica_mode: Requested Replica mode or exact enum-value string; None selects ACTIVE when mode is also None.
        :param verify: Whether ingest inspects the registered Replica; False still permits input hashing and Store commit checks.
        :param mode: Compatibility alias for replica_mode; supplying both non-None arguments raises TypeError even when equal.
        :return: The declared Composite after any requested Item link succeeds.
        """

        if not members:
            raise ValueError("a Composite Digital Asset requires at least one member.")
        stored: dict[str, DigitalAssetRecord] = {}
        for logical_path, source in members.items():
            member_path = _composite_logical_path(logical_path)
            stored[member_path] = self.store(
                source,
                original_name=PurePosixPath(member_path).name,
                metadata=metadata,
                store=store,
                replica_mode=replica_mode,
                verify=verify,
                mode=mode,
            )
        composite = self.create_composite(
            stored,
            name=name,
            attributes=attributes,
        )
        if item is not None:
            self.link(item, composite, role=role)
        return composite

    def export_composite_to_directory(
        self,
        composite: CompositeDigitalAssetID | CompositeDigitalAssetRecord,
        destination: str | os.PathLike[str],
        *,
        overwrite: bool = False,
        store: StoreUUID | StoreConfiguration | StoreAPI | None = None,
        verified: bool = False,
    ) -> tuple[Path, ...]:
        """
        Resolve available Composite members, preflight local targets, then copy them in order.

        The default resolver omits unavailable optional members and requires ACTIVE copies for
        required members. Expand the destination tilde, resolve its root, reject a non-directory
        root, validate delivery names and current symlink containment, then check existing target
        collisions before creating directories. These checks do not prevent later filesystem races,
        detect every same-file alias, or preflight all ancestor-file conflicts.

        For each target, create parents, reopen the Asset through open_asset (selecting again rather
        than using the earlier Replica Location), and copy in 1 MiB chunks. overwrite chooses wb,
        otherwise xb. Both reader and output contexts close; later errors leave earlier files and
        can leave a partial or truncated current target. No temporary publication, cross-file
        rollback, version pin, or new digest validation is added by this export.

        Example:
            >>> paths = manager.export_composite_to_directory(  # doctest: +SKIP
            ...     package, "/exports/book",
            ... )


        :param composite: Composite record or positive non-bool integer ID; member resolution uses its ID and current manager state.
        :param destination: Local root path, expanded for a leading tilde and resolved before target construction.
        :param overwrite: Whether to allow existing targets and truncate each opened file; False uses exclusive creation.
        :param store: Optional Store UUID or UUID-bearing object used as a preference; the default selector can choose another eligible Store.
        :param verified: Whether selection requires recorded VERIFIED state; it does not request fresh digest verification.
        :return: A tuple of target Paths in resolution order after every copy and reader/output cleanup succeeds.
        """

        record = _composite_record(self, composite)
        resolutions = cast(
            CompositeDigitalAssetAPI,
            cast(object, self),
        ).resolve_composite_digital_asset(
            record.composite_digital_asset_id,
            preferred_store_ref=_store_ref(store),
            require_verified=verified,
        )
        root = Path(destination).expanduser().resolve(strict=False)
        if root.exists() and not root.is_dir():
            raise NotADirectoryError(root)
        targets = _resolved_composite_targets(root, resolutions)
        collisions = tuple(path for path in targets if path.exists())
        if collisions and not overwrite:
            raise FileExistsError(collisions[0])
        root.mkdir(parents=True, exist_ok=True)
        written: list[Path] = []
        for resolution, target in zip(resolutions, targets, strict=True):
            target.parent.mkdir(parents=True, exist_ok=True)
            mode_name = "wb" if overwrite else "xb"
            with self.open_asset(
                resolution.resolution.asset_record,
                store=store,
                verified=verified,
            ) as source, target.open(mode_name) as output:
                shutil.copyfileobj(source, output, length=1024 * 1024)
            written.append(target)
        return tuple(written)

    def open_composite_zip(
        self,
        composite: CompositeDigitalAssetID | CompositeDigitalAssetRecord,
        *,
        store: StoreUUID | StoreConfiguration | StoreAPI | None = None,
        verified: bool = False,
    ) -> BinaryIO:
        """
        Materialize resolved members into a seekable temporary DEFLATED ZIP, rewound for reading.

        Resolve the Composite, choose and validate delivery names, and reject duplicate names before
        creating the spool. The default resolver can omit unavailable optional members. Each Asset
        is selected again through open_asset, without pinning the earlier Replica or version. Copy
        in 1 MiB chunks; the 8 MiB spool threshold controls disk rollover, not a total archive-size
        limit.

        Reader and archive-member contexts close during assembly. UnicodeEncodeError from creating a
        ZIP member becomes StorageIntegrityError. Any BaseException during assembly/rewind closes
        the output before re-raising; a close failure can replace the original error. On success the
        caller owns the spool. This delivery representation is not ingested, registered as an Asset,
        or linked by provenance.

        Example:
            >>> with manager.open_composite_zip(package) as source:  # doctest: +SKIP
            ...     header = source.read(4)


        :param composite: Composite record or positive non-bool integer ID whose current members are resolved.
        :param store: Optional Store UUID or UUID-bearing object used as a preference; the default selector can choose another eligible Store.
        :param verified: Whether selection requires recorded VERIFIED state; it does not request fresh digest verification.
        :return: A caller-owned binary spool at offset zero containing the complete temporary ZIP.
        """

        record = _composite_record(self, composite)
        resolutions = cast(
            CompositeDigitalAssetAPI,
            cast(object, self),
        ).resolve_composite_digital_asset(
            record.composite_digital_asset_id,
            preferred_store_ref=_store_ref(store),
            require_verified=verified,
        )
        names = tuple(
            _member_delivery_path(resolution)
            for resolution in resolutions
        )
        if len(names) != len(set(names)):
            raise StorageIntegrityError(
                "Composite members resolve to duplicate delivery paths."
            )
        output = tempfile.SpooledTemporaryFile(max_size=8 * 1024 * 1024)
        try:
            with zipfile.ZipFile(
                output,
                mode="w",
                compression=zipfile.ZIP_DEFLATED,
            ) as archive:
                for resolution, member_name in zip(
                    resolutions,
                    names,
                    strict=True,
                ):
                    try:
                        destination = archive.open(member_name, mode="w")
                    except UnicodeEncodeError as error:
                        raise StorageIntegrityError(
                            "ZIP member names must be valid Unicode."
                        ) from error
                    with destination, self.open_asset(
                        resolution.resolution.asset_record,
                        store=store,
                        verified=verified,
                    ) as source:
                        shutil.copyfileobj(source, destination, length=1024 * 1024)
            _ = output.seek(0)
            return cast(BinaryIO, cast(object, output))
        except BaseException:
            output.close()
            raise

    # Todo: Do we have the granular control to define a specific replicaiton policy for any given ID
    def define_replication_policy(
        self,
        name: str,
        *,
        copies: int = 1,
        target: int | None = None,
        spread_by: Iterable[ReplicaSeparationDimension | str] = (
            ReplicaSeparationDimension.STORE,
        ),
        copies_per_location: int = 1,
        require_tags: Iterable[str] = (),
        prefer_tags: Iterable[str] = (),
        avoid_tags: Iterable[str] = (),
        synchronous_copies: int | None = None,
        auto_heal: bool = True,
        mode: ReplicaMode | str = ReplicaMode.ACTIVE,
        on_loss: DigitalAssetLossAction | str = (
            DigitalAssetLossAction.REQUIRE_COPY
        ),
        priority: int = 100,
    ) -> ReplicationPolicyRecord:
        """
        Construct and register a replication policy from ordinary count and placement controls.

        Normalize enum-value strings, tuple-collect dimensions, and frozenset-collect tag iterables.
        If synchronous_copies is None, choose zero only when the effective target equals zero,
        otherwise one. Policy validation still applies: a zero target must permit recreation or
        loss. Counts and flags are not broadly coerced, and a bare string tag iterable becomes
        characters. Registration does not assign this policy to an Asset, reserve capacity, or
        perform replication/repair.

        Example:
            >>> policy = manager.define_replication_policy(  # doctest: +SKIP
            ...     "durable", copies=2, spread_by=("host",),
            ... )


        :param name: Policy name retained without stripping or case normalization by this wrapper.
        :param copies: Minimum desired copy count, passed unchanged to policy validation.
        :param target: Optional desired target count; None lets the policy use copies.
        :param spread_by: Ordered separation enums or exact value strings converted to a tuple without deduplication.
        :param copies_per_location: Maximum copies per separation bucket, not a byte capacity or Store reservation.
        :param require_tags: Iterable collected as required Store tags in a frozenset without per-tag normalization.
        :param prefer_tags: Iterable collected as preferred Store tags in a frozenset.
        :param avoid_tags: Iterable collected as forbidden Store tags in a frozenset.
        :param synchronous_copies: Required synchronous publications, or None to choose zero for a zero target and one otherwise.
        :param auto_heal: Supplied automatic-healing setting retained in the policy; defining it does not execute repair.
        :param mode: Replica mode enum or exact value string normalized before policy construction.
        :param on_loss: Loss-action enum or exact value string describing the permitted response to unavailable copies.
        :param priority: Retention priority forwarded unchanged to policy validation.
        :return: The policy record returned by create_replication_policy.
        """

        effective_target = copies if target is None else target
        if synchronous_copies is None:
            synchronous_copies = 0 if effective_target == 0 else 1
        policy = ReplicationPolicy(
            name=name,
            min_copies=copies,
            target_copies=target,
            distinct_by=tuple(
                _separation_dimension(value) for value in spread_by
            ),
            max_copies_per_bucket=copies_per_location,
            required_store_tags=frozenset(require_tags),
            preferred_store_tags=frozenset(prefer_tags),
            forbidden_store_tags=frozenset(avoid_tags),
            synchronous_write_copies=synchronous_copies,
            auto_heal=auto_heal,
            mode=_replica_mode(mode),
            loss_action=_loss_action(on_loss),
            retention_priority=priority,
        )
        return cast(
            StoragePolicyAPI,
            cast(object, self),
        ).create_replication_policy(
            policy
        )

    def define_backup_policy(
        self,
        name: str,
        *,
        copies: int = 1,
        target: int | None = None,
        spread_by: Iterable[ReplicaSeparationDimension | str] = (
            ReplicaSeparationDimension.STORE,
        ),
        copies_per_location: int = 1,
        require_tags: Iterable[str] = (),
        prefer_tags: Iterable[str] = (),
        avoid_tags: Iterable[str] = (),
        auto_heal: bool = True,
        verify_after_write: bool = True,
        periodic_verification: bool = True,
        locked: bool = False,
        mode: ReplicaMode | str = ReplicaMode.BACKUP,
        priority: int = 100,
    ) -> BackupPolicyRecord:
        """
        Construct and register a backup policy without copying or scheduling content.

        Normalize mode/dimension strings and collect dimensions/tags before constructing the policy.
        Counts, flags, and name are retained subject to BackupPolicy's selected validation;
        backup/archive mode and zero-copy retention combinations are checked there. Tags are not
        individually stripped or validated by this wrapper. Creation adds no policy assignment or
        physical retention enforcement.

        Example:
            >>> policy = manager.define_backup_policy(  # doctest: +SKIP
            ...     "offsite", copies=2, require_tags={"offsite"},
            ... )


        :param name: Policy name retained without stripping or case normalization by this wrapper.
        :param copies: Minimum desired copy count, passed unchanged to policy validation.
        :param target: Optional desired target count; None lets the policy use copies.
        :param spread_by: Ordered separation enums or exact value strings converted to a tuple without deduplication.
        :param copies_per_location: Maximum copies per separation bucket, not a byte capacity or Store reservation.
        :param require_tags: Iterable collected as required Store tags in a frozenset without per-tag normalization.
        :param prefer_tags: Iterable collected as preferred Store tags in a frozenset.
        :param avoid_tags: Iterable collected as forbidden Store tags in a frozenset.
        :param auto_heal: Supplied automatic-healing setting retained in the policy; defining it does not execute repair.
        :param verify_after_write: Supplied setting for later backup verification after publication.
        :param periodic_verification: Supplied periodic-check setting recorded without scheduling a check here.
        :param locked: Supplied retention-lock setting; zero-target combinations are checked by BackupPolicy.
        :param mode: Replica mode enum or exact value string normalized before policy construction.
        :param priority: Retention priority forwarded unchanged to policy validation.
        :return: The policy record returned by create_backup_policy.
        """

        policy = BackupPolicy(
            name=name,
            min_copies=copies,
            target_copies=target,
            distinct_by=tuple(
                _separation_dimension(value) for value in spread_by
            ),
            max_copies_per_bucket=copies_per_location,
            required_store_tags=frozenset(require_tags),
            preferred_store_tags=frozenset(prefer_tags),
            forbidden_store_tags=frozenset(avoid_tags),
            auto_heal=auto_heal,
            verify_after_write=verify_after_write,
            periodic_verification=periodic_verification,
            retention_locked=locked,
            mode=_replica_mode(mode),
            retention_priority=priority,
        )
        return cast(
            StoragePolicyAPI,
            cast(object, self),
        ).create_backup_policy(
            policy
        )

    def record_derivation(
        self,
        result: (
            DigitalAssetID
            | DigitalAssetRecord
            | DigitalAssetIngestResult
            | DigitalAssetResolution
        ),
        sources: (
            Mapping[
                str,
                DigitalAssetID
                | DigitalAssetRecord
                | DigitalAssetIngestResult
                | DigitalAssetResolution
                | CompositeDigitalAssetRecord,
            ]
            | Iterable[
                DigitalAssetID
                | DigitalAssetRecord
                | DigitalAssetIngestResult
                | DigitalAssetResolution
                | CompositeDigitalAssetRecord
            ]
        ),
        *,
        kind: DigitalAssetDerivationKind | str = (
            DigitalAssetDerivationKind.OTHER
        ),
        recipe: ReproductionRecipe | None = None,
        output_role: str | None = None,
        created_at: datetime | None = None,
        operator: str | None = None,
        notes: str | None = None,
        workflow_id: int | None = None,
        workflow_reference: str | None = None,
    ) -> DigitalAssetDerivationRecord:
        """
        Collect ordered provenance sources and register one complete derivation declaration.

        Mapping keys become source roles; other iterables use None roles. Materialize the inputs,
        assign consecutive sequence numbers, and detect Composite records explicitly; raw nominal
        integers follow the atomic Asset path. Normalize kind through its enum constructor and
        retain the supplied recipe/workflow evidence. Value constructors and the manager own
        graph/reference checks. This wrapper does not execute a recipe, hash outputs, or prove exact
        recreation merely because recipe evidence was supplied.

        Example:
            >>> derivation = manager.record_derivation(  # doctest: +SKIP
            ...     cover, {"source": book}, kind="extract",
            ... )


        :param result: Positive integer Asset ID or an Asset record, ingest result, or resolution whose retained Asset ID is extracted.
        :param sources: Role-to-source mapping or ordered iterable of atomic inputs and Composite records.
        :param kind: Derivation-kind enum or exact value string; defaults to OTHER.
        :param recipe: Optional retained reproduction recipe describing supplied replay evidence.
        :param output_role: Optional role identifying this declaration's result within recipe outputs.
        :param created_at: Optional provenance timestamp forwarded for declaration validation.
        :param operator: Optional operator attribution retained in the declaration.
        :param notes: Optional provenance notes retained without wrapper normalization.
        :param workflow_id: Optional legacy workflow execution ID grouping this step.
        :param workflow_reference: Optional namespaced external workflow reference retained separately from workflow_id.
        :return: The record returned by record_digital_asset_derivation.
        """

        source_values: tuple[
            tuple[_DerivationSourceInput, str | None],
            ...,
        ]
        if isinstance(sources, Mapping):
            source_mapping = cast(
                Mapping[str, _DerivationSourceInput],
                sources,
            )
            source_values = tuple(
                (source, role) for role, source in source_mapping.items()
            )
        else:
            source_iterable = sources
            source_values = tuple(
                (source, None) for source in source_iterable
            )
        references = tuple(
            _derivation_source(sequence_number, source, role)
            for sequence_number, (source, role) in enumerate(source_values)
        )
        declaration = DigitalAssetDerivationDeclaration(
            result_digital_asset_id=_asset_id(result),
            sources=references,
            kind=_derivation_kind(kind),
            recipe=recipe,
            output_role=output_role,
            created_at=created_at,
            operator=operator,
            notes=notes,
            workflow_id=workflow_id,
            workflow_reference=workflow_reference,
        )
        return cast(
            DigitalAssetDerivationRegistryAPI,
            cast(object, self),
        ).record_digital_asset_derivation(
            declaration
    )


# Todo: This remain NOT API concerns
def _file_asset_id(
    manager: object,
    identifier: DigitalAssetFileIdentifier,
    *,
    algorithm: str,
    size: int | None,
) -> DigitalAssetID:
    """
    Resolve a digest through the registry or extract a direct Asset identity.

    Non-string/non-Digest inputs go directly to _asset_id and ignore algorithm and size. A string
    constructs Digest(algorithm, identifier), applying the Digest value's text normalization; an
    existing Digest is retained. Query find_digital_asset_record_by_digest with the optional size
    and raise DigitalAssetNotFound only for a None result. Other lookup errors propagate, and
    returned record IDs are retained without a second validation or byte read.

    Example:
        >>> asset_id = _file_asset_id(  # doctest: +SKIP
        ...     manager, "a" * 64, algorithm="sha256", size=None,
        ... )


    :param manager: Host exposing the digest lookup contract; no runtime cast validation is performed.
    :param identifier: Asset ID/record/result/resolution, a Digest, or digest text; strings are never local paths here.
    :param algorithm: Digest algorithm used only for a string identifier; Digest values retain their own algorithm.
    :param size: Optional exact Asset size used only with digest lookup; ignored for direct ID/record inputs.
    :return: The extracted or digest-matched Asset ID.
    """

    if not isinstance(identifier, (str, Digest)):
        return _asset_id(identifier)
    digest = (
        identifier
        if isinstance(identifier, Digest)
        else Digest(algorithm, identifier)
    )
    record = cast(
        DigitalAssetRegistryAPI,
        manager,
    ).find_digital_asset_record_by_digest(
        digest,
        size_bytes=size,
    )
    if record is None:
        size_detail = "" if size is None else f" with size {size}"
        raise DigitalAssetNotFound(
            f"No Digital Asset is registered for {digest.algorithm}:"
            + f"{digest.value}{size_detail}."
        )
    return record.digital_asset_id


def _positive_integer(value: object) -> int | None:
    """
    Accept positive int instances except bool, returning the original value. Numeric strings,
    fractional numbers, zero, and negatives return None; no int coercion occurs. Accepted integer
    subclasses are retained rather than converted to plain int.

    Example:
        >>> _positive_integer(7)
        7
        >>> _positive_integer(True) is None
        True


    :param value: Candidate scalar to check using isinstance and a positive comparison.
    :return: The original positive non-bool integer, or None.
    """

    return (
        value
        if isinstance(value, int)
        and not isinstance(value, bool)
        and value > 0
        else None
    )


def _asset_id(value: _AssetInput | int) -> DigitalAssetID:
    """
    Extract a retained Asset ID from a record, ingest result, or resolution before checking scalar
    input. Record IDs are not revalidated. Otherwise require a positive non-bool int and apply the
    nominal ID constructor; other forms raise TypeError without a registry lookup.

    Example:
        >>> _asset_id(DigitalAssetID(7))
        7


    :param value: Atomic record/result/resolution or positive integer identity; a Composite record is not an atomic input.
    :return: The retained or nominally wrapped Asset identity.
    """

    if isinstance(value, DigitalAssetRecord):
        return value.digital_asset_id
    if isinstance(value, DigitalAssetIngestResult):
        return value.asset_record.digital_asset_id
    if isinstance(value, DigitalAssetResolution):
        return value.asset_record.digital_asset_id
    identifier = _positive_integer(value)
    if identifier is not None:
        return DigitalAssetID(identifier)
    raise TypeError("asset must be a positive ID or an atomic Asset result/record.")


def _composite_id(value: _CompositeInput) -> CompositeDigitalAssetID:
    """
    Return a Composite record's retained ID without revalidation, otherwise require a positive
    non-bool int. Nominal ID types are not distinguished at runtime, so an integer from another
    identity family is accepted here; no catalogue lookup occurs.

    Example:
        >>> _composite_id(CompositeDigitalAssetID(3))
        3


    :param value: Composite record or positive integer identity.
    :return: The retained or nominally wrapped Composite identity.
    """

    if isinstance(value, CompositeDigitalAssetRecord):
        return value.composite_digital_asset_id
    identifier = _positive_integer(value)
    if identifier is not None:
        return CompositeDigitalAssetID(identifier)
    raise TypeError("composite must be a positive ID or Composite record.")


def _store_ref(value: _StoreInput | None) -> StoreUUID | None:
    """
    Return None or a UUID unchanged; otherwise prefer a UUID-valued store_uuid attribute, then a
    UUID-valued store_ref. Structural attribute access accepts objects beyond the annotated classes
    and does not inspect configuration/availability. Other getter failures propagate, and no usable
    UUID raises TypeError.

    Example:
        >>> _store_ref(UUID(int=1))
        UUID('00000000-0000-0000-0000-000000000001')


    :param value: Optional UUID, configuration, facade, or object exposing one of the recognized UUID attributes.
    :return: The selected UUID, or None when no Store preference/destination was supplied.
    """

    if value is None:
        return None
    if isinstance(value, UUID):
        return value
    configured = getattr(value, "store_uuid", None)
    if isinstance(configured, UUID):
        return configured
    live = getattr(value, "store_ref", None)
    if isinstance(live, UUID):
        return live
    raise TypeError("store must be a Store UUID, configuration, or Store facade.")


def _replica_id(value: _ReplicaInput | None) -> ReplicaID | None:
    """
    Return None unchanged or extract a Replica record's retained ID without revalidation. Other
    inputs must be positive non-bool integers; no existence, Asset-ownership, state, or revision
    check is performed.

    Example:
        >>> _replica_id(ReplicaID(2))
        2


    :param value: Optional Replica record or positive integer identity.
    :return: The retained/nominal Replica ID, or None for omitted source selection.
    """

    if value is None:
        return None
    if isinstance(value, ReplicaRecord):
        return value.replica_id
    identifier = _positive_integer(value)
    if identifier is not None:
        return ReplicaID(identifier)
    raise TypeError("replica must be a positive ID or Replica record.")


def _item_id(value: ItemID | int | None) -> ItemID | None:
    """
    Preserve None for omitted Item linkage; otherwise delegate to the required positive integer
    check. No Item catalogue lookup or coercion from strings is performed.

    Example:
        >>> _item_id(None) is None
        True


    :param value: Optional positive non-bool integer Item identity.
    :return: The nominal Item ID, or None.
    """

    return None if value is None else _required_item_id(value)


def _required_item_id(value: ItemID | int) -> ItemID:
    """
    Accept a positive int instance other than bool and apply the nominal ItemID constructor. Invalid
    types and nonpositive values raise TypeError rather than being int-coerced;
    registration/existence is left to the manager.

    Example:
        >>> _required_item_id(9)
        9


    :param value: Required Item identity checked before link/ingest delegation.
    :return: The positive nominal Item ID, retaining an accepted integer value.
    """

    if isinstance(value, int) and not isinstance(value, bool) and value > 0:
        return ItemID(value)
    raise TypeError("item must be a positive integer ID.")


def _attributes(value: _AttributeInput) -> tuple[tuple[str, str], ...]:
    """
    Collect mapping items or an iterable, then require each entry to unpack into two strings.

    Preserve order, whitespace, duplicate names, and the original non-mapping entry objects; only
    the outer sequence becomes a tuple. A two-character string or two-string list can therefore pass
    without becoming a canonical tuple pair. Wrong element types raise TypeError, while malformed
    unpacking can raise its own error. Blank/duplicate-name rejection, where required, belongs to
    the later metadata constructor.

    Example:
        >>> _attributes({"language": "en"})
        (('language', 'en'),)


    :param value: Attribute mapping or iterable of entries unpacking into string name/value pairs.
    :return: The collected tuple of original items after the two-string check.
    """

    if isinstance(value, Mapping):
        attribute_mapping = cast(Mapping[str, str], value)
        normalized = tuple(attribute_mapping.items())
    else:
        normalized = tuple(value)
    if any(
        not isinstance(name, str) or not isinstance(item, str)
        for name, item in normalized
    ):
        raise TypeError("attribute names and values must be strings.")
    return normalized


def _metadata(
    name: str | None,
    media_type: str | None,
    original_name: str | None,
    attributes: _AttributeInput,
) -> DigitalAssetMetadata:
    """
    Construct flat DigitalAssetMetadata using the supplied labels and collected string attributes.
    Its constructor validates selected nonblank/unique names; this helper adds no MIME inference,
    filename sanitization, rich-hint projection, or deep copying.

    Example:
        >>> _metadata("book", None, None, ()).name
        'book'


    :param name: Optional Asset display name, retained without stripping; None leaves it unspecified.
    :param media_type: Optional Asset media-type text; this wrapper does not infer it from bytes or a filename.
    :param original_name: Optional original filename/source label for Asset metadata, separate from a physical Store key.
    :param attributes: Ordered string name/value pairs or a mapping for Asset metadata; pairs are collected before delegation.
    :return: A new Asset metadata record containing the retained labels and normalized outer attribute sequence.
    """

    return DigitalAssetMetadata(
        name=name,
        media_type=media_type,
        original_name=original_name,
        attributes=_attributes(attributes),
    )


def _placement_hints(
    metadata: StorageHintSource | None,
) -> StoragePlacementHints | None:
    """
    Return None for omitted rich metadata, otherwise delegate to derive_storage_hints. Provider
    projection and validation errors propagate; the result is advisory placement information, not
    Asset identity metadata or proof that a Store will honor it.

    Example:
        >>> _placement_hints({"title": "Book"})["title"]
        'Book'


    :param metadata: Optional library metadata container/mapping or existing placement-hints value.
    :return: The derived placement-hints value, or None for omitted input.
    """

    return None if metadata is None else derive_storage_hints(metadata)


# Todo: Should not be here in the API
# Todo: As a general rule, these helpers should also not be private functions...
def _digests(value: _DigestInput) -> tuple[Digest, ...]:
    """
    Convert mapping entries to Digest objects in mapping order, or tuple-collect an iterable and
    require every value to be a Digest instance. Mapping construction applies Digest text
    normalization. No hashing, nonempty requirement, or duplicate-algorithm rejection is performed
    by this helper; later declaration validation handles those constraints.

    Example:
        >>> _digests({"sha256": "abcd"})[0].algorithm
        'sha256'


    :param value: Algorithm/value mapping or iterable of Digest instances.
    :return: A tuple of normalized mapping-derived or retained supplied Digest objects.
    """

    if isinstance(value, Mapping):
        digest_mapping = cast(Mapping[str, str], value)
        return tuple(
            Digest(algorithm, digest)
            for algorithm, digest in digest_mapping.items()
        )
    digests = tuple(value)
    if any(not isinstance(digest, Digest) for digest in digests):
        raise TypeError("digests must contain Digest values.")
    return digests


def _replica_mode(value: ReplicaMode | str) -> ReplicaMode:
    """
    Return an existing ReplicaMode unchanged, otherwise invoke its enum constructor with the
    original input. No stripping, case folding, or synonym conversion is added; unsupported values
    propagate the constructor error.

    Example:
        >>> _replica_mode("active") is ReplicaMode.ACTIVE
        True


    :param value: Replica mode enum or exact enum-value string to normalize.
    :return: The corresponding ReplicaMode value.
    """

    return value if isinstance(value, ReplicaMode) else ReplicaMode(value)


def _replica_mode_argument(
    replica_mode: ReplicaMode | str | None,
    mode: ReplicaMode | str | None,
) -> ReplicaMode:
    """
    Reject simultaneous non-None replica_mode and mode values even if they agree, then choose the
    supplied spelling. If both are None, return ACTIVE; otherwise normalize the selected enum/value
    string. This alias selects a Replica role, never a read/write file-open mode.

    Example:
        >>> _replica_mode_argument("backup", None) is ReplicaMode.BACKUP
        True


    :param replica_mode: Preferred parameter name for a requested Replica mode, or None.
    :param mode: Historical alias, mutually exclusive with a non-None replica_mode.
    :return: The selected normalized mode, defaulting to ReplicaMode.ACTIVE.
    """

    if replica_mode is not None and mode is not None:
        raise TypeError("use replica_mode or mode, not both.")
    selected = replica_mode if replica_mode is not None else mode
    return ReplicaMode.ACTIVE if selected is None else _replica_mode(selected)


def _separation_dimension(
    value: ReplicaSeparationDimension | str,
) -> ReplicaSeparationDimension:
    """
    Return an existing ReplicaSeparationDimension unchanged, otherwise invoke its enum constructor
    with the original input. No stripping, case folding, or synonym conversion is added; unsupported
    values propagate the constructor error.

    Example:
        >>> _separation_dimension("host") is ReplicaSeparationDimension.HOST
        True


    :param value: Separation dimension enum or exact enum-value string to normalize.
    :return: The corresponding ReplicaSeparationDimension value.
    """

    return (
        value
        if isinstance(value, ReplicaSeparationDimension)
        else ReplicaSeparationDimension(value)
    )


def _loss_action(
    value: DigitalAssetLossAction | str,
) -> DigitalAssetLossAction:
    """
    Return an existing DigitalAssetLossAction unchanged, otherwise invoke its enum constructor with
    the original input. No stripping, case folding, or synonym conversion is added; unsupported
    values propagate the constructor error.

    Example:
        >>> _loss_action("accept_loss") is DigitalAssetLossAction.ACCEPT_LOSS
        True


    :param value: Loss action enum or exact enum-value string to normalize.
    :return: The corresponding DigitalAssetLossAction value.
    """

    return (
        value
        if isinstance(value, DigitalAssetLossAction)
        else DigitalAssetLossAction(value)
    )


def _derivation_kind(
    value: DigitalAssetDerivationKind | str,
) -> DigitalAssetDerivationKind:
    """
    Return an existing DigitalAssetDerivationKind unchanged, otherwise invoke its enum constructor
    with the original input. No stripping, case folding, or synonym conversion is added; unsupported
    values propagate the constructor error.

    Example:
        >>> _derivation_kind("extract") is DigitalAssetDerivationKind.EXTRACT
        True


    :param value: Derivation kind enum or exact enum-value string to normalize.
    :return: The corresponding DigitalAssetDerivationKind value.
    """

    return (
        value
        if isinstance(value, DigitalAssetDerivationKind)
        else DigitalAssetDerivationKind(value)
    )


def _replication_policy_id(
    value: ReplicationPolicyID | ReplicationPolicyRecord | None,
) -> ReplicationPolicyID | None:
    """
    Preserve None and extract a ReplicationPolicyRecord's retained ID without rechecking positivity.
    Otherwise require a positive non-bool integer; unlike some internal policy helpers, this
    function does not use int conversion on strings or fractional values. No policy lookup is
    performed.

    Example:
        >>> _replication_policy_id(ReplicationPolicyID(4))
        4


    :param value: Optional replication-policy record or positive integer ID.
    :return: The retained/nominal replication-policy ID, or None.
    """

    if value is None:
        return None
    if isinstance(value, ReplicationPolicyRecord):
        return value.replication_policy_id
    identifier = _positive_integer(value)
    if identifier is not None:
        return ReplicationPolicyID(identifier)
    raise TypeError("replication must be a positive policy ID or policy record.")


def _backup_policy_id(
    value: BackupPolicyID | BackupPolicyRecord | None,
) -> BackupPolicyID | None:
    """
    Preserve None and extract a BackupPolicyRecord's retained ID without rechecking positivity.
    Otherwise require a positive non-bool integer; unlike some internal policy helpers, this
    function does not use int conversion on strings or fractional values. No policy lookup is
    performed.

    Example:
        >>> _backup_policy_id(BackupPolicyID(5))
        5


    :param value: Optional backup-policy record or positive integer ID.
    :return: The retained/nominal backup-policy ID, or None.
    """

    if value is None:
        return None
    if isinstance(value, BackupPolicyRecord):
        return value.backup_policy_id
    identifier = _positive_integer(value)
    if identifier is not None:
        return BackupPolicyID(identifier)
    raise TypeError("backup must be a positive policy ID or policy record.")


def _derivation_source(
    sequence_number: int,
    value: _DerivationSourceInput,
    role: str | None,
) -> DigitalAssetDerivationSourceReference:
    """
    Construct one ordered provenance source, recognizing Composite records explicitly. Other
    supported inputs contribute an atomic ID through _asset_id, so raw nominal Composite integers
    are not distinguished. Pass sequence/role through source-reference validation without resolving
    content or pinning a Composite revision.

    Example:
        >>> _derivation_source(0, DigitalAssetID(7), "source").role
        'source'


    :param sequence_number: Supplied source position forwarded to the reference constructor.
    :param value: Atomic ID/record/result/resolution or a Composite record for an explicit Composite reference.
    :param role: Optional source role retained subject to the reference value's validation.
    :return: A new atomic or Composite derivation-source reference.
    """

    if isinstance(value, CompositeDigitalAssetRecord):
        return DigitalAssetDerivationSourceReference(
            sequence_number,
            composite_digital_asset_id=value.composite_digital_asset_id,
            role=role,
        )
    return DigitalAssetDerivationSourceReference(
        sequence_number,
        digital_asset_id=_asset_id(value),
        role=role,
    )


def _composite_record(
    manager: object,
    value: CompositeDigitalAssetID | CompositeDigitalAssetRecord,
) -> CompositeDigitalAssetRecord:
    """
    Return an existing Composite record unchanged without consulting the manager. Otherwise
    validate/extract its integer ID and call get_composite_digital_asset_record. This helper does
    not refresh a supplied record or assess member availability; callers can subsequently resolve by
    its ID.

    Example:
        >>> _composite_record(manager, record) is record  # doctest: +SKIP
        True


    :param manager: Host providing Composite-record lookup for ID input.
    :param value: Existing Composite record or positive integer identity.
    :return: The original supplied record, or the manager lookup result.
    """

    if isinstance(value, CompositeDigitalAssetRecord):
        return value
    return cast(
        CompositeDigitalAssetAPI,
        manager,
    ).get_composite_digital_asset_record(_composite_id(value))


def _composite_logical_path(value: str) -> str:
    """
    Require canonical relative POSIX path syntax and return the original string.

    Reject nonstrings with TypeError. Empty text, NUL, backslashes, absolute paths, empty/dot/parent
    components, or spelling changed by PurePosixPath raise ValueError. No whitespace stripping,
    Unicode-encoding check, host-specific reserved-name validation, or filesystem/symlink inspection
    is performed. Other control characters and surrogate code points are not rejected by this syntax
    check.

    Example:
        >>> _composite_logical_path("images/cover.jpg")
        'images/cover.jpg'


    :param value: Logical member path to validate as exact relative POSIX text.
    :return: The unchanged path string after syntax validation.
    """

    if not isinstance(value, str):
        raise TypeError("Composite logical paths must be strings.")
    path = PurePosixPath(value)
    parts = value.split("/")
    if (
        not value
        or "\x00" in value
        or "\\" in value
        or path.is_absolute()
        or any(part in {"", ".", ".."} for part in parts)
        or path.as_posix() != value
    ):
        raise ValueError(
            f"invalid relative Composite logical path: {value!r}"
        )
    return value


def _member_delivery_path(
    member: CompositeDigitalAssetMemberResolution,
) -> str:
    """
    Choose the first truthy logical_path, logical_name, or Asset original_name; otherwise use
    member- followed by the sequence number. Validate only that chosen value with
    _composite_logical_path. A truthy invalid higher-priority name raises rather than trying a later
    fallback; role and Store key are not used.

    Example:
        >>> _member_delivery_path(member)  # doctest: +SKIP
        'images/cover.jpg'


    :param member: Resolved membership carrying relationship labels and an Asset description.
    :return: The selected and syntax-validated relative delivery name.
    """

    membership = member.membership
    asset = member.resolution.asset_record
    candidate = (
        membership.logical_path
        or membership.logical_name
        or asset.metadata.original_name
        or f"member-{membership.sequence_number}"
    )
    return _composite_logical_path(candidate)


def _resolved_composite_targets(
    root: Path,
    resolutions: tuple[CompositeDigitalAssetMemberResolution, ...],
) -> tuple[Path, ...]:
    """
    Preflight delivery names and current resolved containment, returning lexical target Paths.

    Resolve root once, validate each selected member name, join its POSIX components, and require
    the currently resolved target to be under the resolved root. Current symlink escape raises
    StorageIntegrityError. Duplicate lexical Paths also raise, but distinct in-root aliases to the
    same file are not compared by resolved identity. No directories are created, existing files
    checked, or handles/locks retained; the check does not prevent filesystem changes before later
    writes.

    Example:
        >>> targets = _resolved_composite_targets(root, members)  # doctest: +SKIP


    :param root: Destination root Path whose current resolution bounds the target preflight.
    :param resolutions: Ordered resolved members whose selected names become targets.
    :return: A tuple of joined target Paths in input order, without replacing them by resolved paths.
    """

    targets: list[Path] = []
    root_resolved = root.resolve(strict=False)
    for resolution in resolutions:
        relative = _member_delivery_path(resolution)
        target = root.joinpath(*PurePosixPath(relative).parts)
        resolved = target.resolve(strict=False)
        try:
            resolved.relative_to(root_resolved)
        except ValueError as error:
            raise StorageIntegrityError(
                f"Composite member path escapes destination: {relative!r}"
            ) from error
        targets.append(target)
    if len(targets) != len(set(targets)):
        raise StorageIntegrityError(
            "Composite members resolve to duplicate delivery paths."
        )
    return tuple(targets)


__all__ = ["DigitalAssetFileIdentifier", "StorageConvenienceAPI"]
