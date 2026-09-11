"""
Define the public registry contract for expected atomic Asset identities.

Catalogue declarations, metadata replacement, lookup, and forgetting operate on
domain records. Byte publication, availability observation, and physical deletion
belong to separate ingest and Replica operations.
"""

import abc

from collections.abc import Iterator

from LiuXin_alpha.storage.api.models import Digest
from LiuXin_alpha.storage.api.storage_manager_api.models import (
    DigitalAssetDeclaration,
    DigitalAssetID,
    DigitalAssetMetadata,
    DigitalAssetRecord,
)


class DigitalAssetRegistryAPI(abc.ABC):
    """
    Define catalogue operations for expected atomic byte identities and descriptive metadata.

    Callers exchange domain declarations and records rather than database rows. These operations do
    not publish bytes or establish current Replica availability; implementations own repository
    persistence, deduplication, and reference constraints.

    Example:
        >>> asset = registry.get_digital_asset_record(asset_id)  # doctest: +SKIP
    """

    @abc.abstractmethod
    def declare_digital_asset(
        self,
        declaration: DigitalAssetDeclaration,
    ) -> DigitalAssetRecord:
        """
        Register an expected byte identity without publishing bytes or creating a Replica.

        This supports manifests, restoration catalogues, and known-but-missing Assets.
        Implementations may reuse a matching registered identity and enforce policy prerequisites;
        use ingest when source bytes should also be stored.

        Example:
            >>> asset = registry.declare_digital_asset(declaration)  # doctest: +SKIP


        :param declaration: Expected size, digests, metadata, and optional registered policy references.
        :return: Registered or reused Asset record; no physical Replica is implied.
        """
        ...

    @abc.abstractmethod
    def get_digital_asset_record(
        self,
        digital_asset_id: DigitalAssetID,
    ) -> DigitalAssetRecord:
        """
        Resolve one catalogue identity or raise DigitalAssetNotFound; repository failures remain
        visible.

        Example:
            >>> asset = registry.get_digital_asset_record(DigitalAssetID(7))  # doctest: +SKIP


        :param digital_asset_id: Manager-assigned atomic Asset ID to resolve.
        :return: Asset domain record for the requested ID, without a physical availability guarantee.
        """
        ...

    @abc.abstractmethod
    def update_digital_asset_metadata(
        self,
        digital_asset_id: DigitalAssetID,
        metadata: DigitalAssetMetadata,
        *,
        if_revision: str | None = None,
    ) -> DigitalAssetRecord:
        """
        Replace descriptive metadata while retaining Asset size and digests.

        A stale supplied revision raises StoragePreconditionFailed. The implementation owns revision
        advancement and persistence; this operation does not rename or rewrite existing Store
        objects.

        Example:
            >>> updated = registry.update_digital_asset_metadata(asset_id, metadata, if_revision=asset.revision)  # doctest: +SKIP


        :param digital_asset_id: Manager-assigned atomic Asset ID to resolve.
        :param metadata: Complete replacement descriptive metadata, not a field merge.
        :param if_revision: Expected current revision, or None to omit the optimistic precondition.
        :return: Asset record carrying the replacement metadata and resulting revision.
        """
        ...

    @abc.abstractmethod
    def iter_digital_asset_records(self) -> Iterator[DigitalAssetRecord]:
        """
        Iterate known Asset domain records without discovering physical Store contents. Ordering and
        snapshot guarantees belong to the implementation.

        Example:
            >>> assets = tuple(registry.iter_digital_asset_records())  # doctest: +SKIP


        :return: Iterator of registered Asset records, including identities with no available Replica.
        """
        ...

    @abc.abstractmethod
    def find_digital_asset_record_by_digest(
        self,
        digest: Digest,
        *,
        size_bytes: int | None = None,
    ) -> DigitalAssetRecord | None:
        """
        Find a registered deduplication candidate by digest and optional exact byte count.

        Only genuine absence returns None; repository or connection failures propagate. A catalogue
        match does not establish that a readable physical copy remains.

        Example:
            >>> candidate = registry.find_digital_asset_record_by_digest(digest, size_bytes=42)  # doctest: +SKIP


        :param digest: Expected digest whose algorithm and value must match registered identity evidence.
        :param size_bytes: Optional exact expected byte count; None leaves size unconstrained.
        :return: Matching Asset record, or None when no catalogue candidate matches.
        """
        ...

    @abc.abstractmethod
    def forget_digital_asset(
        self,
        digital_asset_id: DigitalAssetID,
        *,
        require_no_replicas: bool = True,
        if_revision: str | None = None,
    ) -> bool:
        """
        Forget a catalogue identity without deleting Store bytes.

        The default refuses existing Replica claims. Disabling that check does not waive all other
        reference constraints; implementations may still reject composite membership, Item links, or
        derivation provenance. A supplied revision guards the record mutation.

        Example:
            >>> removed = registry.forget_digital_asset(asset_id, if_revision=asset.revision)  # doctest: +SKIP


        :param digital_asset_id: Manager-assigned atomic Asset ID to resolve.
        :param require_no_replicas: Whether any remaining Replica claim must prevent removal.
        :param if_revision: Expected current revision, or None to omit the optimistic precondition.
        :return: True when the record is removed, or False when it is already absent; constraint and repository errors propagate.
        """
        ...


__all__ = ["DigitalAssetRegistryAPI"]
