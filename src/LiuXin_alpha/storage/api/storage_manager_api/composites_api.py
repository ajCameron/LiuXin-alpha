"""
Define Composite catalogue mutation, member resolution, and availability assessment.

Operations retain the distinction between logical membership relationships and the
atomic Assets/Replicas that carry bytes. Metadata changes do not assemble or delete
member payloads.
"""

import abc

from collections.abc import Iterator

from LiuXin_alpha.storage.api.models import StoreUUID
from LiuXin_alpha.storage.api.storage_manager_api.models import (
    CompositeDigitalAssetAvailabilityAssessment,
    CompositeDigitalAssetDeclaration,
    CompositeDigitalAssetID,
    CompositeDigitalAssetRecord,
    CompositeDigitalAssetMemberResolution,
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

    # Todo: Add the ability to make a compositie digital asset from a series of digital assets - it might be in convenience

    # Todo: Again, less than elegant to have to declare and then call "declare_composite_digital_asset_from_declaration" can also exist
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

    # Todo: Also be good to get from a composite digital asset hash - which should exist
    # Todo: That's a good idea! A composite digital asset hash - stores the file names and hashes for all the files
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

    # Todo: Also be good to have methods to update individual asset properties
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

    # Todo: There should be a better container for an entire composite digit asset....
    @abc.abstractmethod
    def resolve_composite_digital_asset(
        self,
        composite_digital_asset_id: CompositeDigitalAssetID,
        *,
        preferred_store_ref: StoreUUID | None = None,
        require_verified: bool = False,
    ) -> tuple[CompositeDigitalAssetMemberResolution, ...]:
        """
        Resolve available members while preserving their complete relationship metadata.

        Unavailable optional members may be omitted; an unavailable required member raises
        CompositeDigitalAssetIncomplete. Returned selections describe observed routing choices
        rather than open readers or a lasting availability guarantee.

        Example:
            >>> members = manager.resolve_composite_digital_asset(composite_id, require_verified=True)  # doctest: +SKIP


        :param composite_digital_asset_id: Manager-assigned Composite identity to resolve.
        :param preferred_store_ref: Optional Store UUID to prefer without excluding eligible copies elsewhere.
        :param require_verified: Whether selection requires a recorded VERIFIED state; this flag does not itself request fresh digest verification.
        :return: Tuple of available member resolutions in the implementation's delivery order, provided required members resolve.
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
