"""
Define Composite catalogue mutation, member resolution, and availability assessment.

Operations retain the distinction between logical membership relationships and the
atomic Assets/Replicas that carry bytes. Metadata changes do not assemble or delete
member payloads.
"""

import abc

from collections.abc import Iterable, Iterator

from LiuXin_alpha.storage.api.models import Digest, StoreUUID
from LiuXin_alpha.storage.api.storage_manager_api.models import (
    CompositeDigitalAssetAvailabilityAssessment,
    CompositeDigitalAssetDeclaration,
    CompositeDigitalAssetID,
    CompositeDigitalAssetMembership,
    CompositeDigitalAssetRecord,
    CompositeDigitalAssetResolution,
    ReplicaMode,
)


class CompositeDigitalAssetAPI(abc.ABC):
    """
    Define catalogue and resolution operations for logical assemblies of atomic Assets.

    Composites own membership relationships and metadata rather than direct Replica claims or byte
    streams. Resolution pairs each available member relationship with an atomic selection,
    preserving its role, labels, path, and position.

    Example:
        >>> members = manager.resolve_composite_digital_asset(composite_id)  # doctest: +SKIP
    """

    @abc.abstractmethod
    def declare_composite_digital_asset(
        self,
        declaration: CompositeDigitalAssetDeclaration,
    ) -> CompositeDigitalAssetRecord:
        """
        Register a new logical assembly whose referenced atomic Assets must be known. This creates
        Composite metadata without publishing or combining member bytes.

        Example:
            >>> composite = manager.declare_composite_digital_asset(declaration)  # doctest: +SKIP


        :param declaration: Membership sequence, optional name, and attributes for the new Composite.
        :return: New Composite record with its assigned identity and revision.
        """
        ...

    @abc.abstractmethod
    def get_composite_digital_asset_record(
        self,
        composite_digital_asset_id: CompositeDigitalAssetID,
    ) -> CompositeDigitalAssetRecord:
        """
        Resolve a Composite catalogue identity or raise CompositeDigitalAssetNotFound without
        probing member availability.

        Example:
            >>> composite = manager.get_composite_digital_asset_record(composite_id)  # doctest: +SKIP


        :param composite_digital_asset_id: Manager-assigned Composite identity to resolve.
        :return: Registered Composite record for the requested identity.
        """
        ...

    @abc.abstractmethod
    def calculate_composite_digital_asset_digest(
        self,
        composite_digital_asset_id: CompositeDigitalAssetID,
        *,
        algorithm: str = "sha256",
    ) -> Digest:
        """
        Hash a canonical manifest of ordered relationships and atomic byte identities.

        The versioned manifest includes each member's sequence, delivery labels/path, role, title,
        required flag, expected size, and sorted digest set. It deliberately excludes manager IDs,
        Composite name/attributes/revision, Asset descriptive metadata, Replica Locations, and
        current availability. Equivalent logical assemblies therefore retain the same digest across
        catalogues and storage layouts.

        Algorithm names remain runtime strings because hashlib providers may extend the available
        set. The operation reads catalogue metadata only; it does not hash member bytes.

        Example:
            >>> digest = manager.calculate_composite_digital_asset_digest(composite_id)  # doctest: +SKIP

        :param composite_digital_asset_id: Registered Composite identity whose canonical manifest is hashed.
        :param algorithm: Runtime-supported hashlib algorithm name, defaulting to sha256.
        :return: Normalized digest of the canonical Composite manifest.
        """
        ...

    @abc.abstractmethod
    def find_composite_digital_asset_records_by_digest(
        self,
        digest: Digest,
    ) -> tuple[CompositeDigitalAssetRecord, ...]:
        """
        Return every registered Composite whose canonical manifest matches a digest.

        Equivalent declarations are permitted and can therefore produce several records. Results
        follow Composite catalogue iteration order. Lookup recalculates metadata digests and does
        not maintain an index, read physical bytes, or treat no match as an exception.

        Example:
            >>> matches = manager.find_composite_digital_asset_records_by_digest(digest)  # doctest: +SKIP

        :param digest: Expected algorithm and canonical manifest digest value.
        :return: Possibly empty tuple of matching Composite records in catalogue order.
        """
        ...

    @abc.abstractmethod
    def set_composite_digital_asset_name(
        self,
        composite_digital_asset_id: CompositeDigitalAssetID,
        name: str | None,
        *,
        if_revision: str | None = None,
    ) -> CompositeDigitalAssetRecord:
        """Replace or clear only a Composite's display name.

        :param composite_digital_asset_id: Manager-assigned Composite identity to update.
        :param name: Nonblank display name, or None to clear it.
        :param if_revision: Expected current revision, or None to omit the optimistic precondition.
        :return: Updated Composite record preserving membership and attributes.
        """
        ...

    @abc.abstractmethod
    def set_composite_digital_asset_attributes(
        self,
        composite_digital_asset_id: CompositeDigitalAssetID,
        attributes: tuple[tuple[str, str], ...],
        *,
        if_revision: str | None = None,
    ) -> CompositeDigitalAssetRecord:
        """Replace only a Composite's ordered extension attributes.

        :param composite_digital_asset_id: Manager-assigned Composite identity to update.
        :param attributes: Complete replacement attribute pairs, retained in supplied order.
        :param if_revision: Expected current revision, or None to omit the optimistic precondition.
        :return: Updated Composite record preserving membership and name.
        """
        ...

    @abc.abstractmethod
    def replace_composite_digital_asset_member(
        self,
        composite_digital_asset_id: CompositeDigitalAssetID,
        membership: CompositeDigitalAssetMembership,
        *,
        if_revision: str | None = None,
    ) -> CompositeDigitalAssetRecord:
        """Replace the relationship occupying one declared sequence number.

        The replacement can reference another registered Asset and can change relationship labels or
        required status, but its sequence number selects and retains the existing logical position.

        :param composite_digital_asset_id: Manager-assigned Composite identity to update.
        :param membership: Complete replacement relationship whose sequence number must exist.
        :param if_revision: Expected current revision, or None to omit the optimistic precondition.
        :return: Updated Composite record preserving other relationships and descriptive metadata.
        """
        ...

    @abc.abstractmethod
    def replace_composite_digital_asset(
        self,
        composite_digital_asset_id: CompositeDigitalAssetID,
        declaration: CompositeDigitalAssetDeclaration,
        *,
        if_revision: str | None = None,
    ) -> CompositeDigitalAssetRecord:
        """
        Replace the full membership and descriptive metadata while retaining the Composite identity.

        An optional revision guards stale updates. The implementation owns metadata transaction
        guarantees and validates referenced Assets; replacing membership does not move, delete, or
        concatenate member bytes.

        Example:
            >>> updated = manager.replace_composite_digital_asset(composite_id, declaration, if_revision=composite.revision)  # doctest: +SKIP


        :param composite_digital_asset_id: Manager-assigned Composite identity to resolve.
        :param declaration: Complete replacement membership and descriptive metadata.
        :param if_revision: Expected current revision, or None to omit the optimistic precondition.
        :return: Replacement Composite record with the retained identity and resulting revision.
        """
        ...

    @abc.abstractmethod
    def iter_composite_digital_asset_records(
        self,
    ) -> Iterator[CompositeDigitalAssetRecord]:
        """
        Iterate registered Composite records without resolving their members. Ordering and snapshot
        guarantees belong to the implementation.

        Example:
            >>> composites = tuple(manager.iter_composite_digital_asset_records())  # doctest: +SKIP

        :return: Iterator of known Composite catalogue records.
        """
        ...

    @abc.abstractmethod
    def forget_composite_digital_asset(
        self,
        composite_digital_asset_id: CompositeDigitalAssetID,
        *,
        require_unlinked: bool = True,
        if_revision: str | None = None,
    ) -> bool:
        """
        Forget Composite metadata without deleting its atomic members or their bytes.

        The default requires the Composite to have no protected links or provenance references.
        Waiving that check does not imply cascading removal of referencing metadata; persistence
        constraints may still apply.

        Example:
            >>> removed = manager.forget_composite_digital_asset(composite_id, if_revision=composite.revision)  # doctest: +SKIP


        :param composite_digital_asset_id: Manager-assigned Composite identity to resolve.
        :param require_unlinked: Whether existing Item/provenance references must prevent forgetting.
        :param if_revision: Expected current revision, or None to omit the optimistic precondition.
        :return: True after removal, or False for an absent identity; revision, reference, and repository errors can propagate.
        """
        ...

    @abc.abstractmethod
    def resolve_composite_digital_asset(
        self,
        composite_digital_asset_id: CompositeDigitalAssetID,
        *,
        preferred_store_ref: StoreUUID | None = None,
        require_verified: bool = False,
    ) -> CompositeDigitalAssetResolution:
        """
        Resolve the Composite record and available members into one aggregate selection.

        Unavailable optional members may be omitted; an unavailable required member raises
        CompositeDigitalAssetIncomplete. Returned selections describe observed routing choices
        rather than open readers or a lasting availability guarantee. The aggregate remains
        iterable/indexable over its members for callers migrating from the former bare tuple.

        Example:
            >>> members = manager.resolve_composite_digital_asset(composite_id, require_verified=True)  # doctest: +SKIP


        :param composite_digital_asset_id: Manager-assigned Composite identity to resolve.
        :param preferred_store_ref: Optional Store UUID to prefer without excluding eligible copies elsewhere.
        :param require_verified: Whether selection requires a recorded VERIFIED state; this flag does not itself request fresh digest verification.
        :return: Composite record and available member resolutions in implementation delivery order.
        """
        ...

    @abc.abstractmethod
    def materialize_composite_digital_asset(
        self,
        composite_digital_asset_id: CompositeDigitalAssetID,
        *,
        preferred_store_ref: StoreUUID | None = None,
        source_modes: Iterable[ReplicaMode | str] = (ReplicaMode.ACTIVE,),
        cache_store_ref: StoreUUID | None = None,
        verify: bool = True,
    ) -> CompositeDigitalAssetResolution:
        """
        Materialize every available Composite member, optionally into one cache Store.

        Required unavailable members make the operation fail after all relationships have been
        considered; optional unavailable members are omitted. A cache destination applies to every
        member. Publication is per atomic Asset, so a later failure does not roll back earlier cache
        copies. Repeated Asset memberships can reuse the first cached Replica while retaining each
        relationship separately in the result.

        Example:
            >>> resolved = manager.materialize_composite_digital_asset(  # doctest: +SKIP
            ...     composite_id, cache_store_ref=cache_uuid,
            ... )

        :param composite_digital_asset_id: Manager-assigned Composite identity to materialize.
        :param preferred_store_ref: Optional source Store preference applied independently to each member.
        :param source_modes: Ordered source modes searched independently for each member.
        :param cache_store_ref: Exact cache destination for every member, or None to return selected sources.
        :param verify: Whether selected/reused sources require recorded verification and new cache copies are inspected.
        :return: Aggregate Composite resolution containing every materialized required and available optional member.
        """
        ...

    @abc.abstractmethod
    def assess_composite_digital_asset(
        self,
        composite_digital_asset_id: CompositeDigitalAssetID,
    ) -> CompositeDigitalAssetAvailabilityAssessment:
        """
        Assess required-member catalogue presence and Replica selection without exporting bytes.

        Optional members need not affect readability. Counts and diagnostics represent the
        observations made by the implementation; they are not an atomic snapshot across repositories
        and Stores.

        Example:
            >>> assessment = manager.assess_composite_digital_asset(composite_id)  # doctest: +SKIP


        :param composite_digital_asset_id: Manager-assigned Composite identity to resolve.
        :return: Required-member availability counts and missing/error diagnostics.
        """
        ...

__all__ = ["CompositeDigitalAssetAPI"]
