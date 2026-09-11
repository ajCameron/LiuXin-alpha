"""
Represent atomic, Composite-member, and Item-role selections as retained domain values.

Constructors check selected identity relationships and membership coverage. Location
properties project the supplied selections without opening readers or establishing
current physical availability.
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
    Pair an expected Asset identity with one selected Replica record.

    Construction compares the records' Asset IDs only. It does not verify state, size, digests, or
    current Store availability, and it retains the original record references. The location
    projection is routing information rather than a resource-owning reader.

    Example:
        >>> resolution.location == resolution.replica_record.location  # doctest: +SKIP
        True


    :ivar asset_record: Expected Asset identity and metadata retained for the selection.
    :ivar replica_record: Selected claim whose owning Asset ID must equal asset_record.digital_asset_id.
    """

    asset_record: DigitalAssetRecord
    replica_record: ReplicaRecord

    def __post_init__(self) -> None:
        """
        Require equal owning Asset IDs on the two retained records.

        Attributes are read without record-type checks or physical verification; malformed objects
        can raise their own access errors.

        Example:
            >>> DigitalAssetResolution(  # doctest: +SKIP
            ...     asset_record, wrong_replica_record,
            ... )
            Traceback (most recent call last):
            ...
            ValueError: Replica does not belong to the resolved Digital Asset.


        :return: None when the Asset IDs agree; disagreement raises ValueError.
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
        Return the exact Location held by the selected Replica without lookup, copying, or a new
        readability check.

        Example:
            >>> location = resolved.location  # doctest: +SKIP


        :return: Retained Replica Location.
        """

        return self.replica_record.location


@dataclasses.dataclass(slots=True, frozen=True)
class CompositeDigitalAssetMemberResolution:
    """
    Pair one complete membership relationship with its selected atomic Asset and Replica.

    Role, names, path, position, and required status remain available through membership.
    Construction checks membership-to-Asset ID agreement without assessing physical readability or
    membership in a particular Composite record.

    Example:
        >>> member.location == member.resolution.location  # doctest: +SKIP
        True


    :ivar membership: Retained relationship carrying the expected atomic Asset ID and delivery metadata.
    :ivar resolution: Atomic selection whose Asset ID must agree with the membership.
    """

    membership: CompositeDigitalAssetMembership
    resolution: DigitalAssetResolution

    def __post_init__(self) -> None:
        """
        Compare the membership Asset ID with the atomic selection's Asset ID.

        The method does not revalidate the underlying records, inspect Store bytes, or determine
        whether this relationship belongs to a Composite.

        Example:
            >>> CompositeDigitalAssetMemberResolution(  # doctest: +SKIP
            ...     membership, wrong_resolution,
            ... )
            Traceback (most recent call last):
            ...
            ValueError: resolved Asset does not match the Composite member.


        :return: None for matching IDs; mismatch raises ValueError and malformed attributes can raise.
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
        Delegate the Location projection to the retained atomic selection without re-resolving it.

        Example:
            >>> location = member.location  # doctest: +SKIP


        :return: The exact Location exposed by resolution.location.
        """

        return self.resolution.location


@dataclasses.dataclass(slots=True, frozen=True)
class ItemDigitalAssetResolution:
    """
    Retain one Item-role selection containing either an atomic resolution or a Composite with
    resolved members.

    Construction enforces the exclusive target choice, selected value checks, and Composite
    membership coverage. It compares whole membership values in sets, so relationship labels and
    positions matter while duplicate resolved relationships can collapse during validation. Supplied
    member order and duplicates remain retained for locations.

    Example:
        >>> selection = ItemDigitalAssetResolution(  # doctest: +SKIP
        ...     ItemID(9), "cover", digital_asset_resolution=resolution,
        ... )


    :ivar item_id: Item identity rejected when it compares at or below zero, without catalogue lookup.
    :ivar role: Required nonblank role text retained in its original spelling.
    :ivar digital_asset_resolution: Atomic target selection, mutually exclusive with a Composite record.
    :ivar composite_digital_asset_record: Composite target whose required memberships must be represented when selected.
    :ivar composite_member_resolutions: Retained delivery sequence of member resolutions; must be empty for an atomic target.
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
        Require exactly one target, nonblank role text, and an Item ID not comparing at or below
        zero.

        Atomic targets reject nonempty Composite-member resolutions. Composite targets compare sets
        of whole membership values: every resolved relationship must be declared, and every
        truthy-required relationship must be resolved. The checks do not enforce resolution
        uniqueness or sequence order, and set construction requires hashable relationship values.

        No Item existence, current readability, or record-type validation occurs. Role spelling and
        supplied containers remain unchanged; comparison, attribute, string, and hashing errors can
        propagate.

        Example:
            >>> ItemDigitalAssetResolution(ItemID(9), "cover")
            Traceback (most recent call last):
            ...
            ValueError: exactly one atomic or Composite Asset is required.


        :return: None after target and membership coverage checks pass; violations raise ValueError or the underlying malformed-input error.
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
        Project the atomic Location as a one-item tuple, or each Composite resolution Location in
        supplied order.

        Composite results are not sorted by membership sequence or deduplicated. The projection
        performs no new selection, byte read, or physical availability check.

        Example:
            >>> locations = selection.locations  # doctest: +SKIP


        :return: New tuple of retained selected Location references, preserving Composite order and duplicates.
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
