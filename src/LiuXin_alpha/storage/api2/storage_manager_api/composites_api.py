"""Composite Digital Asset facade."""

import abc

from collections.abc import Iterator, Sequence

from LiuXin_alpha.storage.api2.models import Location, StoreRef
from LiuXin_alpha.storage.api2.storage_manager_api.models import (
    CompositeAssetHealth, CompositeDigitalAssetID, CompositeDigitalAssetRecordAPI,
    CompositeMemberSpec, DigitalAssetID,
)


class CompositeAssetAPI(abc.ABC):
    """Contract for ordered assets assembled from atomic asset members.

    Example:
        >>> def member_locations(
        ...     manager: CompositeAssetAPI,
        ...     composite_id: CompositeDigitalAssetID,
        ... ) -> tuple[Location, ...]:
        ...     return manager.locate_composite_digital_asset(composite_id)
    """

    @abc.abstractmethod
    def create_composite_record(
        self, composite: CompositeDigitalAssetRecordAPI,
    ) -> CompositeDigitalAssetRecordAPI:
        """Persist a new Composite Digital Asset record.

        Example:
            >>> created = manager.create_composite_record(composite)  # doctest: +SKIP
        """
        ...

    @abc.abstractmethod
    def get_composite_digital_asset(
        self, composite_id: CompositeDigitalAssetID,
    ) -> CompositeDigitalAssetRecordAPI:
        """Return a Composite Digital Asset record by identifier.

        Example:
            >>> composite = manager.get_composite_digital_asset(3)  # doctest: +SKIP
        """
        ...

    @abc.abstractmethod
    def update_composite_record(
        self, composite: CompositeDigitalAssetRecordAPI,
    ) -> CompositeDigitalAssetRecordAPI:
        """Persist changes to a Composite Digital Asset record.

        Example:
            >>> updated = manager.update_composite_record(composite)  # doctest: +SKIP
        """
        ...

    @abc.abstractmethod
    def iter_composite_digital_assets(self) -> Iterator[CompositeDigitalAssetRecordAPI]:
        """Iterate over known Composite Digital Asset records.

        Example:
            >>> composites = list(manager.iter_composite_digital_assets())  # doctest: +SKIP
        """
        ...

    @abc.abstractmethod
    def delete_composite_metadata(
        self, composite_id: CompositeDigitalAssetID, *, require_unlinked: bool = True,
    ) -> bool:
        """Delete composite metadata, optionally requiring no item links.

        Example:
            >>> deleted = manager.delete_composite_metadata(3)  # doctest: +SKIP
        """
        ...

    @abc.abstractmethod
    def assemble_composite_digital_asset(
        self, members: Sequence[DigitalAssetID | CompositeMemberSpec], *,
        metadata: object | None = None,
    ) -> CompositeDigitalAssetRecordAPI:
        """Create a composite record and its ordered member relationships.

        Example:
            >>> composite = manager.assemble_composite_digital_asset(  # doctest: +SKIP
            ...     [7, CompositeMemberSpec(8, 1, role="supplement")],
            ... )
        """
        ...

    @abc.abstractmethod
    def set_composite_members(
        self, composite_id: CompositeDigitalAssetID,
        members: Sequence[DigitalAssetID | CompositeMemberSpec],
    ) -> None:
        """Replace the ordered membership of an existing composite.

        Example:
            >>> manager.set_composite_members(3, [7, 8])  # doctest: +SKIP
        """
        ...

    @abc.abstractmethod
    def iter_composite_members(
        self, composite_id: CompositeDigitalAssetID,
    ) -> Iterator[CompositeMemberSpec]:
        """Iterate over a composite's members in sequence order.

        Example:
            >>> members = list(manager.iter_composite_members(3))  # doctest: +SKIP
        """
        ...

    @abc.abstractmethod
    def locate_composite_digital_asset(
        self, composite_id: CompositeDigitalAssetID, *,
        preferred_store: StoreRef | None = None, verify: bool = False,
    ) -> tuple[Location, ...]:
        """Resolve each required composite member to a readable location.

        Example:
            >>> locations = manager.locate_composite_digital_asset(  # doctest: +SKIP
            ...     3, preferred_store="primary", verify=True,
            ... )
        """
        ...

    @abc.abstractmethod
    def assess_composite_digital_asset(
        self, composite_id: CompositeDigitalAssetID,
    ) -> CompositeAssetHealth:
        """Assess completeness and replica health across all members.

        Example:
            >>> health = manager.assess_composite_digital_asset(3)  # doctest: +SKIP
        """
        ...


__all__ = ["CompositeAssetAPI"]
