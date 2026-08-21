"""
Low-cognitive-overhead operations layered over the explicit manager contract.
"""

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
from LiuXin_alpha.storage.api.storage_manager_api.stores_api import (
    StoreAdministrationAPI,
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


class StorageConvenienceAPI:
    """
    Familiar operations that delegate to the precise storage-manager methods.

    This mixin owns no state and adds no implementation requirements. Rich
    declarations and detailed result objects remain available through the
    underlying methods whenever callers need full transactional control.

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
        Store bytes, a stream, or a local file using one obvious entry point.

        Strings and path-like values name local files; bytes-like values are
        stored directly; objects with ``read()`` are streamed.
        ``metadata`` accepts a WEMI metadata container, a plain mapping, or an
        existing storage-hints value. It is projected into advisory hints for
        rich Stores and remains separate from Digital Asset identity metadata.

        Example:
            >>> asset = manager.store(  # doctest: +SKIP
            ...     b"cover", name="cover.jpg", metadata=item_metadata,
            ... )


        :param source:
        :param name:
        :param media_type:
        :param original_name:
        :param attributes:
        :param metadata:
        :param item:
        :param role:
        :param store:
        :param replica_mode:
        :param verify:
        :param operation_id:
        :param expected_size:
        :param expected_digests:
        :param mode: Backward-compatible alias for ``replica_mode``.
        :return:
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
        Store a small byte string and return its Digital Asset record.

        Use ``ingest_bytes`` when the detailed ingest and Replica result is
        required.
        ``metadata`` is advisory library metadata used by rich Stores for
        placement; the flat name and media arguments describe the Asset.

        Example:
            >>> asset = manager.store_bytes(  # doctest: +SKIP
            ...     b"book", original_name="book.epub",
            ... )


        :param data:
        :param name:
        :param media_type:
        :param original_name:
        :param attributes:
        :param metadata:
        :param item:
        :param role:
        :param store:
        :param replica_mode:
        :param verify:
        :param operation_id:
        :param expected_digests:
        :param mode: Backward-compatible alias for ``replica_mode``.
        :return:
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
        Stream bytes into managed storage and return their Asset record.

        Example:
            >>> asset = manager.store_stream(  # doctest: +SKIP
            ...     source, expected_size=4, name="book",
            ... )


        :param source:
        :param expected_size:
        :param expected_digests:
        :param name:
        :param media_type:
        :param original_name:
        :param attributes:
        :param metadata:
        :param item:
        :param role:
        :param store:
        :param replica_mode:
        :param verify:
        :param operation_id:
        :param mode: Backward-compatible alias for ``replica_mode``.
        :return:
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
        Store one local file without requiring callers to open it themselves.

        The file name becomes ``original_name`` unless explicitly overridden.

        Example:
            >>> asset = manager.store_file(  # doctest: +SKIP
            ...     "/incoming/book.epub", media_type="application/epub+zip",
            ... )


        :param path:
        :param expected_size:
        :param expected_digests:
        :param name:
        :param media_type:
        :param original_name:
        :param attributes:
        :param metadata:
        :param item:
        :param role:
        :param store:
        :param replica_mode:
        :param verify:
        :param operation_id:
        :param mode: Backward-compatible alias for ``replica_mode``.
        :return:
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
        Declare known bytes that are not currently being ingested.

        Digest mappings such as ``{"sha256": value}`` are accepted directly.

        Example:
            >>> asset = manager.declare_asset(  # doctest: +SKIP
            ...     4, {"sha256": "abcd"}, name="known object",
            ... )


        :param size:
        :param digests:
        :param name:
        :param media_type:
        :param original_name:
        :param attributes:
        :param replication:
        :param backup:
        :return:
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
        Open a readable Replica of an Asset as a binary stream.

        Example:
            >>> with manager.open_asset(asset) as source:  # doctest: +SKIP
            ...     header = source.read(4)


        :param asset:
        :param store:
        :param replica_mode:
        :param verified:
        :param offset:
        :param length:
        :param mode: Backward-compatible alias for ``replica_mode``.
        :return:
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
        Read an Asset or byte range fully into memory.

        Example:
            >>> manager.read_asset(asset, length=4)  # doctest: +SKIP
            b'book'


        :param asset:
        :param store:
        :param replica_mode:
        :param verified:
        :param offset:
        :param length:
        :param mode: Backward-compatible alias for ``replica_mode``.
        :return:
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
        Open an Asset file as a read-only binary stream.

        This method never opens an Asset for mutation and accepts no write
        mode. Use ``store()``, ``store_stream()``, or the explicit ingest API
        for commit-based writes. Close the returned stream, preferably by
        using it as a context manager.

        Integer values are Digital Asset IDs. A bare string is a digest value
        using ``algorithm`` (SHA-256 by default). Supplying a ``Digest`` keeps
        its own algorithm. Missing hashes raise ``DigitalAssetNotFound``;
        known Assets without a readable copy raise ``NoReadableReplica``.

        Example:
            >>> with manager.open_file(7) as source:  # doctest: +SKIP
            ...     payload = source.read()
            >>> with manager.open_file("a" * 64) as source:  # doctest: +SKIP
            ...     same_payload = source.read()


        :param identifier:
        :param algorithm:
        :param size:
        :param store:
        :param replica_mode:
        :param verified:
        :param offset:
        :param length:
        :param mode: Backward-compatible alias for ``replica_mode``.
        :return:
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
        Return the read-only ``open_file`` stream using familiar vocabulary.

        Example:
            >>> with manager.get_file(7) as source:  # doctest: +SKIP
            ...     payload = source.read()


        :param identifier:
        :param algorithm:
        :param size:
        :param store:
        :param replica_mode:
        :param verified:
        :param offset:
        :param length:
        :param mode: Backward-compatible alias for ``replica_mode``.
        :return:
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
        Read an Asset file by ID or hash fully into memory.

        Example:
            >>> manager.read_file(7)  # doctest: +SKIP
            b'book'
            >>> manager.read_file(Digest("sha256", "a" * 64))  # doctest: +SKIP
            b'book'


        :param identifier:
        :param algorithm:
        :param size:
        :param store:
        :param replica_mode:
        :param verified:
        :param offset:
        :param length:
        :param mode: Backward-compatible alias for ``replica_mode``.
        :return:
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
        Create another Replica using records or IDs already in hand.

        Example:
            >>> replica = manager.replicate_asset(  # doctest: +SKIP
            ...     asset, to=archive_store,
            ... )


        :param asset:
        :param to:
        :param from_replica:
        :param metadata: Optional rich metadata used to place this Replica.
            When omitted, the source Replica's placement snapshot is reused.
        :param replica_mode:
        :param verify:
        :param mode: Backward-compatible alias for ``replica_mode``.
        :return:
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
        Link an Item role to an atomic or Composite Asset.

        Composite records are detected automatically. Pass ``composite=True``
        when supplying only a raw Composite ID.

        Example:
            >>> manager.link(9, cover, role="cover")  # doctest: +SKIP


        :param item:
        :param asset:
        :param role:
        :param composite:
        :return:
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
        Remove one Item-role association.

        Example:
            >>> manager.unlink(9, role="cover")  # doctest: +SKIP
            True


        :param item:
        :param role:
        :return:
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
        Create an ordered Composite from Asset records or IDs.

        A mapping treats each key as the member's logical path. An iterable
        creates plain required members in its existing order.

        Example:
            >>> composite = manager.create_composite(  # doctest: +SKIP
            ...     {"book.epub": book, "cover.jpg": cover},
            ...     name="book package",
            ... )


        :param members:
        :param name:
        :param attributes:
        :return:
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
        """Ingest named atomic members and declare their Composite.

        Mapping keys are safe relative logical paths in the resulting
        Composite. Each value uses the same source forms as :meth:`store`.
        Successfully ingested atomic Assets remain valid if a later member
        fails; the Composite record is declared only after every member has
        completed.

        Example:
            >>> package = manager.store_composite(  # doctest: +SKIP
            ...     {"book.epub": book_bytes, "images/cover.jpg": cover},
            ...     name="book package",
            ... )
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
        """Write resolved Composite members beneath one local directory.

        Logical member paths are validated as relative POSIX paths and
        resolved against the destination to prevent traversal or symlink
        escape. Existing files are preserved unless ``overwrite`` is true.

        Example:
            >>> paths = manager.export_composite_to_directory(  # doctest: +SKIP
            ...     package, "/exports/book",
            ... )
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
        """Return a seekable temporary ZIP stream of resolved members.

        This is a transient delivery representation. Callers that persist it
        should ingest the ZIP as a new atomic Asset and record its derivation
        from the Composite explicitly.

        Example:
            >>> with manager.open_composite_zip(package) as source:  # doctest: +SKIP
            ...     header = source.read(4)
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

    def add_store(
        self,
        name: str,
        kind: str,
        root: str | os.PathLike[str],
        *,
        store_uuid: StoreUUID | None = None,
        url: str | None = None,
        protocol: str | None = None,
        failure_domain: str | None = None,
        region: str | None = None,
        host: UUID | None = None,
        device: UUID | None = None,
        tags: Iterable[str] = (),
        replication: ReplicationPolicyID | ReplicationPolicyRecord | None = None,
        backup: BackupPolicyID | BackupPolicyRecord | None = None,
        modes: Iterable[ReplicaMode | str] = (
            ReplicaMode.ACTIVE,
            ReplicaMode.BACKUP,
            ReplicaMode.ARCHIVE,
        ),
        operational_role: str | None = None,
        read_only: bool = False,
        folders: bool = True,
        options: (
            Mapping[str, object] | Iterable[tuple[str, object]]
        ) = (),
        start: bool = True,
    ) -> StoreConfiguration:
        """
        Configure and start a Store without constructing its configuration.

        The manager's configured Store factory still selects the concrete
        backend implementation.

        Example:
            >>> archive = manager.add_store(  # doctest: +SKIP
            ...     "archive", "filesystem", "file:///srv/archive",
            ...     tags={"offsite"},
            ... )


        :param name:
        :param kind:
        :param root:
        :param store_uuid:
        :param url:
        :param protocol:
        :param failure_domain:
        :param region:
        :param host:
        :param device:
        :param tags:
        :param replication:
        :param backup:
        :param modes:
        :param operational_role:
        :param read_only:
        :param folders:
        :param options: Non-secret backend-specific configuration values.
        :param start:
        :return:
        """

        configuration = StoreConfiguration.for_backend(
            name,
            kind,
            root,
            store_uuid=store_uuid,
            url=url,
            protocol=protocol,
            failure_domain=failure_domain,
            region=region,
            host=host,
            device=device,
            tags=tags,
            replication_policy=_replication_policy_id(replication),
            backup_policy=_backup_policy_id(backup),
            modes=(_replica_mode(mode) for mode in modes),
            operational_role=operational_role,
            read_only=read_only,
            folders=folders,
            options=options,
        )
        return cast(
            StoreAdministrationAPI,
            cast(object, self),
        ).create_store(
            configuration,
            startup=start,
        )

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
        Define live-copy policy using ordinary copy and placement terms.

        Example:
            >>> policy = manager.define_replication_policy(  # doctest: +SKIP
            ...     "durable", copies=2, spread_by=("host",),
            ... )


        :param name:
        :param copies:
        :param target:
        :param spread_by:
        :param copies_per_location:
        :param require_tags:
        :param prefer_tags:
        :param avoid_tags:
        :param synchronous_copies:
        :param auto_heal:
        :param mode:
        :param on_loss:
        :param priority:
        :return:
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
        Define backup or archive policy using ordinary copy terms.

        Example:
            >>> policy = manager.define_backup_policy(  # doctest: +SKIP
            ...     "offsite", copies=2, require_tags={"offsite"},
            ... )


        :param name:
        :param copies:
        :param target:
        :param spread_by:
        :param copies_per_location:
        :param require_tags:
        :param prefer_tags:
        :param avoid_tags:
        :param auto_heal:
        :param verify_after_write:
        :param periodic_verification:
        :param locked:
        :param mode:
        :param priority:
        :return:
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
    ) -> DigitalAssetDerivationRecord:
        """
        Record ordinary provenance without constructing source references.

        A mapping uses its keys as source roles. Composite records are detected
        automatically. Exact recipes may still be supplied through the rich
        recipe value when replay safety matters.

        Example:
            >>> derivation = manager.record_derivation(  # doctest: +SKIP
            ...     cover, {"source": book}, kind="extract",
            ... )


        :param result:
        :param sources:
        :param kind:
        :param recipe:
        :param output_role:
        :param created_at:
        :param operator:
        :param notes:
        :param workflow_id: Optional workflow execution grouping this step.
        :return:
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
        )
        return cast(
            DigitalAssetDerivationRegistryAPI,
            cast(object, self),
        ).record_digital_asset_derivation(
            declaration
    )


def _file_asset_id(
    manager: object,
    identifier: DigitalAssetFileIdentifier,
    *,
    algorithm: str,
    size: int | None,
) -> DigitalAssetID:
    """
    Resolve an Asset ID directly or through the manager's digest index.

    Example:
        >>> asset_id = _file_asset_id(  # doctest: +SKIP
        ...     manager, "a" * 64, algorithm="sha256", size=None,
        ... )
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
    Return a positive ordinary integer while rejecting booleans.

    Example:
        >>> _positive_integer(7)
        7
        >>> _positive_integer(True) is None
        True
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
    Extract and validate one atomic Asset ID from a convenient input.

    Example:
        >>> _asset_id(DigitalAssetID(7))
        7
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
    Extract and validate one Composite Asset ID.

    Example:
        >>> _composite_id(CompositeDigitalAssetID(3))
        3
    """

    if isinstance(value, CompositeDigitalAssetRecord):
        return value.composite_digital_asset_id
    identifier = _positive_integer(value)
    if identifier is not None:
        return CompositeDigitalAssetID(identifier)
    raise TypeError("composite must be a positive ID or Composite record.")


def _store_ref(value: _StoreInput | None) -> StoreUUID | None:
    """
    Extract a Store UUID from a UUID, configuration, or live Store.

    Example:
        >>> _store_ref(UUID(int=1))
        UUID('00000000-0000-0000-0000-000000000001')
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
    Extract an optional Replica ID from a record or positive integer.

    Example:
        >>> _replica_id(ReplicaID(2))
        2
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
    Normalize an optional Item identity.

    Example:
        >>> _item_id(None) is None
        True
    """

    return None if value is None else _required_item_id(value)


def _required_item_id(value: ItemID | int) -> ItemID:
    """
    Normalize one required positive Item identity.

    Example:
        >>> _required_item_id(9)
        9
    """

    if isinstance(value, int) and not isinstance(value, bool) and value > 0:
        return ItemID(value)
    raise TypeError("item must be a positive integer ID.")


def _attributes(value: _AttributeInput) -> tuple[tuple[str, str], ...]:
    """
    Normalize metadata attributes while retaining caller order.

    Example:
        >>> _attributes({"language": "en"})
        (('language', 'en'),)
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
    Build Digital Asset metadata from ordinary keyword arguments.

    Example:
        >>> _metadata("book", None, None, ()).name
        'book'
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
    Project optional library metadata into Store-facing placement hints.

    Example:
        >>> _placement_hints({"title": "Book"})["title"]
        'Book'
    """

    return None if metadata is None else derive_storage_hints(metadata)


def _digests(value: _DigestInput) -> tuple[Digest, ...]:
    """
    Normalize digest objects or an algorithm-to-value mapping.

    Example:
        >>> _digests({"sha256": "abcd"})[0].algorithm
        'sha256'
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
    Normalize a Replica mode enum or its string value.

    Example:
        >>> _replica_mode("active") is ReplicaMode.ACTIVE
        True
    """

    return value if isinstance(value, ReplicaMode) else ReplicaMode(value)


def _replica_mode_argument(
    replica_mode: ReplicaMode | str | None,
    mode: ReplicaMode | str | None,
) -> ReplicaMode:
    """
    Select the clear Replica-mode name while retaining the former alias.

    Example:
        >>> _replica_mode_argument("backup", None) is ReplicaMode.BACKUP
        True
    """

    if replica_mode is not None and mode is not None:
        raise TypeError("use replica_mode or mode, not both.")
    selected = replica_mode if replica_mode is not None else mode
    return ReplicaMode.ACTIVE if selected is None else _replica_mode(selected)


def _separation_dimension(
    value: ReplicaSeparationDimension | str,
) -> ReplicaSeparationDimension:
    """
    Normalize a failure-separation enum or its string value.

    Example:
        >>> _separation_dimension("host") is ReplicaSeparationDimension.HOST
        True
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
    Normalize an on-loss action enum or its string value.

    Example:
        >>> _loss_action("accept_loss") is DigitalAssetLossAction.ACCEPT_LOSS
        True
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
    Normalize a derivation kind enum or its string value.

    Example:
        >>> _derivation_kind("extract") is DigitalAssetDerivationKind.EXTRACT
        True
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
    Extract an optional replication-policy identity.

    Example:
        >>> _replication_policy_id(ReplicationPolicyID(4))
        4
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
    Extract an optional backup-policy identity.

    Example:
        >>> _backup_policy_id(BackupPolicyID(5))
        5
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
    Build one atomic or Composite provenance source reference.

    Example:
        >>> _derivation_source(0, DigitalAssetID(7), "source").role
        'source'
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
    """Resolve a Composite ID while preserving an existing record.

    Example:
        >>> _composite_record(manager, record) is record  # doctest: +SKIP
        True
    """

    if isinstance(value, CompositeDigitalAssetRecord):
        return value
    return cast(
        CompositeDigitalAssetAPI,
        manager,
    ).get_composite_digital_asset_record(_composite_id(value))


def _composite_logical_path(value: str) -> str:
    """Validate one portable, relative Composite delivery path.

    Example:
        >>> _composite_logical_path("images/cover.jpg")
        'images/cover.jpg'
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
    """Choose and validate the portable path for one resolved member.

    Example:
        >>> _member_delivery_path(member)  # doctest: +SKIP
        'images/cover.jpg'
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
    """Resolve unique member targets without permitting root escape.

    Example:
        >>> targets = _resolved_composite_targets(root, members)  # doctest: +SKIP
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
