"""Responsibility-specific manager convenience adapter."""

from __future__ import annotations

import os
import shutil
import tempfile
import zipfile
from collections.abc import Iterable, Mapping
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Any, BinaryIO, Protocol, cast
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


class _CompositeConvenienceHost(Protocol):
    """Declare sibling conveniences required by Composite ingest and delivery.

    Example:
        >>> def export(host: _CompositeConvenienceHost, composite):  # doctest: +SKIP
        ...     return host.open_asset(composite)
    """

    def store(self, source: _StorableSource, **kwargs: Any) -> DigitalAssetRecord:  # noqa: F405
        """Store one member source.

        Example:
            >>> record = host.store(source)  # doctest: +SKIP

        :param source: Member source accepted by the host's atomic storage convenience.
        :param kwargs: Named ingest controls forwarded by the Composite adapter.
        :return: Registered atomic Asset record for the member.
        """
        ...

    def link(self, item: ItemID | int, asset: object, **kwargs: Any) -> None:
        """Link a completed Composite to an Item.

        Example:
            >>> host.link(7, composite)  # doctest: +SKIP

        :param item: Item identity to associate with the completed Composite.
        :param asset: Composite record supplied by the adapter.
        :param kwargs: Named role and interpretation controls.
        :return: None after the host records the association.
        """
        ...

    def open_asset(self, asset: _AssetInput, **kwargs: Any) -> BinaryIO:  # noqa: F405
        """Open one resolved member for delivery.

        Example:
            >>> source = host.open_asset(asset)  # doctest: +SKIP

        :param asset: Atomic member identity or record selected for delivery.
        :param kwargs: Named Store preference and verification controls.
        :return: Caller-owned binary reader for the selected member.
        """
        ...


class CompositeConvenienceMixin:
    """Provide declaration, ingest, and delivery helpers for Composite Assets.

    A host supplies the Composite contract and the atomic ``store``/``open_asset``
    operations used by ingest and export. Ordered member operations retain their
    existing partial-publication boundaries.

    Example:
        >>> package = manager.create_composite({"book.epub": book})  # doctest: +SKIP
    """

    def create_composite(
        self,
        members: _CompositeMembersInput,
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

        return cast(
            CompositeDigitalAssetAPI,
            cast(object, self),
        ).declare_composite_digital_asset(
            CompositeDigitalAssetDeclaration(
                _composite_memberships(members),
                name=name,
                attributes=_attributes(attributes),
            )
        )

    def replace_composite(
        self,
        composite: _CompositeInput,
        members: _CompositeMembersInput,
        *,
        name: str | None = None,
        attributes: Mapping[str, str] | Iterable[tuple[str, str]] = (),
        if_revision: str | None = None,
    ) -> CompositeDigitalAssetRecord:
        """
        Build complete Composite replacement intent and delegate to the declaration API.

        Normalize the Composite identity, memberships, and attributes exactly as create_composite
        does. The underlying replacement operation remains responsible for reference checks,
        revision enforcement, persistence, and revision allocation; this façade performs no partial
        update or byte operation.

        Example:
            >>> updated = manager.replace_composite(  # doctest: +SKIP
            ...     composite, {"book.epub": book},
            ...     if_revision=composite.revision,
            ... )


        :param composite: Positive Composite ID or Composite record whose identity is retained.
        :param members: Mapping from logical paths to atomic Asset inputs, or an ordered iterable of atomic inputs.
        :param name: Optional replacement Composite display name.
        :param attributes: Complete ordered replacement extension pairs or mapping.
        :param if_revision: Optional expected revision forwarded to the underlying replacement.
        :return: The replacement record returned by replace_composite_digital_asset.
        """

        return cast(
            CompositeDigitalAssetAPI,
            cast(object, self),
        ).replace_composite_digital_asset(
            _composite_id(composite),
            CompositeDigitalAssetDeclaration(
                _composite_memberships(members),
                name=name,
                attributes=_attributes(attributes),
            ),
            if_revision=if_revision,
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
        host = cast(_CompositeConvenienceHost, self)
        stored: dict[str, DigitalAssetRecord] = {}
        for logical_path, source in members.items():
            member_path = _composite_logical_path(logical_path)
            stored[member_path] = host.store(
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
            host.link(
                item,
                composite,
                role=role,
            )
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

        host = cast(_CompositeConvenienceHost, self)
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
            with (
                host.open_asset(
                    resolution.resolution.asset_record,
                    store=store,
                    verified=verified,
                ) as source,
                target.open(mode_name) as output,
            ):
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

        host = cast(_CompositeConvenienceHost, self)
        record = _composite_record(self, composite)
        resolutions = cast(
            CompositeDigitalAssetAPI,
            cast(object, self),
        ).resolve_composite_digital_asset(
            record.composite_digital_asset_id,
            preferred_store_ref=_store_ref(store),
            require_verified=verified,
        )
        names = tuple(_member_delivery_path(resolution) for resolution in resolutions)
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
                    with (
                        destination,
                        host.open_asset(
                            resolution.resolution.asset_record,
                            store=store,
                            verified=verified,
                        ) as source,
                    ):
                        shutil.copyfileobj(source, destination, length=1024 * 1024)
            _ = output.seek(0)
            return cast(BinaryIO, cast(object, output))
        except BaseException:
            output.close()
            raise


__all__ = ["CompositeConvenienceMixin"]
