"""
Composite Digital Asset domain facade.
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


# Todo: We want a similar DigitalAssetAPI
class CompositeDigitalAssetAPI(abc.ABC):
    """
    Operations over ordered logical assemblies of atomic Assets.

    Composite Assets do not directly contain bytes or own Replicas. Resolution
    preserves each membership relationship and pairs it with a selected atomic
    Asset and Replica.

    Example:
        >>> members = manager.resolve_composite_digital_asset(  # doctest: +SKIP
        ...     CompositeDigitalAssetID(3),
        ... )
    """

    @abc.abstractmethod
    def declare_composite_digital_asset(
        self,
        declaration: CompositeDigitalAssetDeclaration,
    ) -> CompositeDigitalAssetRecord:
        """
        Register a new logical assembly and its ordered memberships.

        Example:
            >>> record = manager.declare_composite_digital_asset(  # doctest: +SKIP
            ...     declaration,
            ... )


        :param declaration:
        :return:
        """
        ...

    @abc.abstractmethod
    def get_composite_digital_asset_record(
        self,
        composite_digital_asset_id: CompositeDigitalAssetID,
    ) -> CompositeDigitalAssetRecord:
        """
        Return one Composite record or raise ``CompositeDigitalAssetNotFound``.

        Example:
            >>> record = manager.get_composite_digital_asset_record(  # doctest: +SKIP
            ...     CompositeDigitalAssetID(3),
            ... )


        :param composite_digital_asset_id:
        :return:
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
        Replace Composite metadata and membership atomically in metadata.

        Example:
            >>> record = manager.replace_composite_digital_asset(  # doctest: +SKIP
            ...     CompositeDigitalAssetID(3), declaration, if_revision="v2",
            ... )


        :param composite_digital_asset_id:
        :param declaration:
        :param if_revision:
        :return:
        """
        ...

    @abc.abstractmethod
    def iter_composite_digital_asset_records(
        self,
    ) -> Iterator[CompositeDigitalAssetRecord]:
        """
        Iterate over known Composite Digital Asset records.

        Example:
            >>> records = list(  # doctest: +SKIP
            ...     manager.iter_composite_digital_asset_records(),
            ... )


        :return:
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
        Forget a Composite identity without deleting member Assets.

        Example:
            >>> forgotten = manager.forget_composite_digital_asset(  # doctest: +SKIP
            ...     CompositeDigitalAssetID(3), require_unlinked=True,
            ... )


        :param composite_digital_asset_id:
        :param require_unlinked:
        :param if_revision:
        :return:
        """
        ...

    @abc.abstractmethod
    def resolve_composite_digital_asset(
        self,
        composite_digital_asset_id: CompositeDigitalAssetID,
        *,
        preferred_store_ref: StoreUUID | None = None,
        require_verified: bool = False,
    ) -> tuple[CompositeDigitalAssetMemberResolution, ...]:
        """
        Resolve required members without discarding names, paths or roles.

        Missing required members raise ``CompositeDigitalAssetIncomplete``.

        Example:
            >>> resolved = manager.resolve_composite_digital_asset(  # doctest: +SKIP
            ...     CompositeDigitalAssetID(3), require_verified=True,
            ... )


        :param composite_digital_asset_id:
        :param preferred_store_ref:
        :param require_verified:
        :return:
        """
        ...

    @abc.abstractmethod
    def assess_composite_digital_asset(
        self,
        composite_digital_asset_id: CompositeDigitalAssetID,
    ) -> CompositeDigitalAssetAvailabilityAssessment:
        """
        Assess membership completeness and current readability.

        Example:
            >>> assessment = manager.assess_composite_digital_asset(  # doctest: +SKIP
            ...     CompositeDigitalAssetID(3),
            ... )


        :param composite_digital_asset_id:
        :return:
        """
        ...

    # Todo: We have some nice manipulation methods but retrieval still needs real work
    #       We need methods for actually getting stuff into and out of stores. Ideally simple.
    # Todo: get_composite_digital_asset_as_zip -> BinaryIO (zipped file of the contents)
    # Todo: write_composite_to_folder -> takes all the members of a composite digital asset and dumps em to some kind of folder
    # Todo: store_composite_digital_asset([BinaryIO, ]) -> something appropriate


__all__ = ["CompositeDigitalAssetAPI"]
