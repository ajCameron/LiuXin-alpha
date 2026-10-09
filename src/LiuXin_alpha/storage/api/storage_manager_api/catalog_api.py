"""
Define the public registry contract for expected atomic Asset identities.

Catalogue declarations, metadata replacement, lookup, and forgetting operate on
domain records. Byte publication, availability observation, and physical deletion
belong to separate ingest and Replica operations.
"""

import abc

from collections.abc import Iterator
from enum import StrEnum

from LiuXin_alpha.storage.api.models import Digest
from LiuXin_alpha.storage.api.storage_manager_api.models import (
    DigitalAssetDeclaration,
    DigitalAssetID,
    DigitalAssetMetadata,
    DigitalAssetRecord,
)


class DigitalAssetRecordOrder(StrEnum):
    """
    Name stable catalogue ordering keys for Asset-record iteration.

    ID follows manager identity, SIZE follows expected logical bytes, and descriptive fields compare
    case-folded text. Ordering reads catalogue metadata only and says nothing about Replica
    availability or physical Store layout.
    """

    ID = "id"
    SIZE = "size"
    NAME = "name"
    MEDIA_TYPE = "media_type"
    ORIGINAL_NAME = "original_name"


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
    def set_digital_asset_name(
        self,
        digital_asset_id: DigitalAssetID,
        name: str | None,
        *,
        if_revision: str | None = None,
    ) -> DigitalAssetRecord:
        """Replace or clear only an Asset's display name while preserving other metadata fields.

        :param digital_asset_id: Manager-assigned atomic Asset identity to update.
        :param name: Nonblank display name, or None to clear it.
        :param if_revision: Expected current revision, or None to omit the optimistic precondition.
        :return: Updated Asset record with a new revision.
        """
        ...

    @abc.abstractmethod
    def set_digital_asset_media_type(
        self,
        digital_asset_id: DigitalAssetID,
        media_type: str | None,
        *,
        if_revision: str | None = None,
    ) -> DigitalAssetRecord:
        """Replace or clear only an Asset's media type while preserving other metadata fields.

        :param digital_asset_id: Manager-assigned atomic Asset identity to update.
        :param media_type: Nonblank media-type text, or None to clear it.
        :param if_revision: Expected current revision, or None to omit the optimistic precondition.
        :return: Updated Asset record with a new revision.
        """
        ...

    @abc.abstractmethod
    def set_digital_asset_original_name(
        self,
        digital_asset_id: DigitalAssetID,
        original_name: str | None,
        *,
        if_revision: str | None = None,
    ) -> DigitalAssetRecord:
        """Replace or clear only an Asset's original-name label.

        :param digital_asset_id: Manager-assigned atomic Asset identity to update.
        :param original_name: Nonblank filename/source label, or None to clear it.
        :param if_revision: Expected current revision, or None to omit the optimistic precondition.
        :return: Updated Asset record with a new revision.
        """
        ...

    @abc.abstractmethod
    def set_digital_asset_attributes(
        self,
        digital_asset_id: DigitalAssetID,
        attributes: tuple[tuple[str, str], ...],
        *,
        if_revision: str | None = None,
    ) -> DigitalAssetRecord:
        """Replace only an Asset's ordered extension attributes.

        :param digital_asset_id: Manager-assigned atomic Asset identity to update.
        :param attributes: Complete replacement attribute pairs validated by DigitalAssetMetadata.
        :param if_revision: Expected current revision, or None to omit the optimistic precondition.
        :return: Updated Asset record with a new revision.
        """
        ...

    @abc.abstractmethod
    def iter_digital_asset_records(
        self,
        *,
        order_by: DigitalAssetRecordOrder | str = DigitalAssetRecordOrder.ID,
        descending: bool = False,
    ) -> Iterator[DigitalAssetRecord]:
        """
        Iterate known Asset records in a requested deterministic catalogue order.

        Descriptive ordering is case-insensitive and uses Asset ID as a stable tie breaker. Missing
        descriptive values sort after present values in ascending order and before them when the
        complete ordering is reversed. The operation does not discover physical Store contents.

        Example:
            >>> assets = tuple(registry.iter_digital_asset_records())  # doctest: +SKIP


        :param order_by: ID, size, name, media type, or original-name enum/value.
        :param descending: Whether to reverse the complete selected ordering.
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


__all__ = ["DigitalAssetRecordOrder", "DigitalAssetRegistryAPI"]
