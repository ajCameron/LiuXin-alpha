"""
Readable atomic, Composite, and Item resolution values.
"""

from __future__ import annotations

import dataclasses

from LiuXin_alpha.storage.api.models import Location
from LiuXin_alpha.storage.api.storage_manager_api.models.asset_identity import (
    DigitalAssetRecord,
)
from LiuXin_alpha.storage.api.storage_manager_api.models.composites import (
    CompositeDigitalAssetMembership,
    CompositeDigitalAssetRecord,
)
from LiuXin_alpha.storage.api.storage_manager_api.models.identifiers import ItemID
from LiuXin_alpha.storage.api.storage_manager_api.models.replicas import ReplicaRecord


@dataclasses.dataclass(slots=True, frozen=True)
class DigitalAssetResolution:
    """
    Asset and Replica records selected for readable access.

    The value captures a manager selection at one point in time. Constructing
    it validates record identity; it does not claim that external storage can
    never become unavailable afterwards.

    Example:
        >>> resolution.location == resolution.replica_record.location  # doctest: +SKIP
        True
    """

    asset_record: DigitalAssetRecord
    replica_record: ReplicaRecord

    def __post_init__(self) -> None:
        """
        Require the selected Replica to belong to the paired Asset.

        Example:
            >>> DigitalAssetResolution(  # doctest: +SKIP
            ...     asset_record, wrong_replica_record,
            ... )
            Traceback (most recent call last):
            ...
            ValueError: Replica does not belong to the resolved Digital Asset.


        :return:
        """

        if (
            self.asset_record.digital_asset_id
            != self.replica_record.digital_asset_id
        ):
            raise ValueError(
                "Replica does not belong to the resolved Digital Asset."
            )

    @property
    def location(self) -> Location:
        """
        Return the selected Replica Location.

        Example:
            >>> location = resolved.location  # doctest: +SKIP


        :return:
        """

        return self.replica_record.location


@dataclasses.dataclass(slots=True, frozen=True)
class CompositeDigitalAssetMemberResolution:
    """
    One Composite membership paired with a readable Asset selection.

    Example:
        >>> member.location == member.resolution.location  # doctest: +SKIP
        True
    """

    membership: CompositeDigitalAssetMembership
    resolution: DigitalAssetResolution

    def __post_init__(self) -> None:
        """
        Require resolution of the declared member relationship.

        Example:
            >>> CompositeDigitalAssetMemberResolution(  # doctest: +SKIP
            ...     membership, wrong_resolution,
            ... )
            Traceback (most recent call last):
            ...
            ValueError: resolved Asset does not match the Composite member.


        :return:
        """

        if (
            self.membership.digital_asset_id
            != self.resolution.asset_record.digital_asset_id
        ):
            raise ValueError(
                "resolved Asset does not match the Composite member."
            )

    @property
    def location(self) -> Location:
        """
        Return the selected Location for this member.

        Example:
            >>> location = member.location  # doctest: +SKIP


        :return:
        """

        return self.resolution.location


@dataclasses.dataclass(slots=True, frozen=True)
class ItemDigitalAssetResolution:
    """
    Resolved atomic or Composite Asset selected for one Item role.

    Example:
        >>> selection = ItemDigitalAssetResolution(  # doctest: +SKIP
        ...     ItemID(9), "cover", digital_asset_resolution=resolution,
        ... )
    """

    item_id: ItemID
    role: str
    digital_asset_resolution: DigitalAssetResolution | None = None
    composite_digital_asset_record: CompositeDigitalAssetRecord | None = None
    composite_member_resolutions: tuple[
        CompositeDigitalAssetMemberResolution, ...
    ] = ()

    def __post_init__(self) -> None:
        """
        Require exactly one selected Asset and consistent resolutions.

        Example:
            >>> ItemDigitalAssetResolution(ItemID(9), "cover")
            Traceback (most recent call last):
            ...
            ValueError: exactly one atomic or Composite Asset is required.


        :return:
        """

        if (self.digital_asset_resolution is None) == (
            self.composite_digital_asset_record is None
        ):
            raise ValueError(
                "exactly one atomic or Composite Asset is required."
            )
        if not self.role.strip():
            raise ValueError("role must not be empty.")
        if self.item_id <= 0:
            raise ValueError("item_id must be positive.")
        if (
            self.digital_asset_resolution is not None
            and self.composite_member_resolutions
        ):
            raise ValueError(
                "an atomic Item selection must not contain Composite members."
            )
        if self.composite_digital_asset_record is not None:
            declared_members = set(self.composite_digital_asset_record.members)
            resolved_relationships = {
                member.membership
                for member in self.composite_member_resolutions
            }
            if not resolved_relationships <= declared_members:
                raise ValueError(
                    "resolved member does not belong to the selected Composite."
                )
            required_members = {
                member
                for member in self.composite_digital_asset_record.members
                if member.required
            }
            if not required_members <= resolved_relationships:
                raise ValueError(
                    "a required Composite member has not been resolved."
                )

    @property
    def locations(self) -> tuple[Location, ...]:
        """
        Return selected readable Locations in delivery order.

        Example:
            >>> locations = selection.locations  # doctest: +SKIP


        :return:
        """

        if self.digital_asset_resolution is not None:
            return (self.digital_asset_resolution.location,)
        return tuple(
            member.location for member in self.composite_member_resolutions
        )


__all__ = [
    "CompositeDigitalAssetMemberResolution",
    "DigitalAssetResolution",
    "ItemDigitalAssetResolution",
]
