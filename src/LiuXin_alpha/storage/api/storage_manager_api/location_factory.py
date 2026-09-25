"""
Resolve Asset and Replica identities through a retained live manager.

Factories forward current selection requirements without caching locations or
reconstructing physical addresses from catalogue identifiers.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol

from LiuXin_alpha.storage.api.models import Location, StoreUUID
from LiuXin_alpha.storage.api.storage_manager_api.models import (
    DigitalAssetID,
    ReplicaID,
)


# Todo: This could be public.
class _AssetLocator(Protocol):
    """
    Describe the manager selection methods needed by LocationFactory. This structural typing seam
    avoids importing the full facade and supplies no runtime checking, selection algorithm, or
    resource ownership.

    Example:
        >>> locator: _AssetLocator = manager  # doctest: +SKIP
    """

    def locate_digital_asset(
        self,
        digital_asset_id: DigitalAssetID,
        *,
        preferred_store_ref: StoreUUID | None = None,
        require_verified: bool = False,
    ) -> Location:
        """
        Select an eligible Location for one Digital Asset under the requested preference and
        verification requirement. Availability checks and failure categories belong to the concrete
        manager; several Replicas may represent the same Asset.

        Example:
            >>> location = locator.locate_digital_asset(asset_id, require_verified=True)  # doctest: +SKIP


        :param digital_asset_id: Catalogue Digital Asset identity whose eligible Replica is selected by the manager.
        :param preferred_store_ref: Optional configured Store UUID forwarded as a selection preference.
        :param require_verified: Whether the manager must select a Replica satisfying its verified-selection contract.
        :return: Location of the Replica selected by the manager.
        """
        ...

    def locate_replica(self, replica_id: ReplicaID) -> Location:
        """
        Resolve one registered Replica to its concrete Location. This contract selects by exact
        Replica identity rather than searching among an Asset's alternative copies; the manager owns
        lookup and availability policy.

        Example:
            >>> location = locator.locate_replica(replica_id)  # doctest: +SKIP


        :param replica_id: Exact catalogue Replica identity to resolve through the manager.
        :return: Location resolved for the requested Replica.
        """
        ...


@dataclass(slots=True, frozen=True, eq=False)
class LocationFactory:
    """
    Retain a live manager for resolving catalogue identities to concrete Locations. Asset lookup
    performs current Replica selection; it does not reconstruct a unique permanent address from an
    ID. Replica lookup forwards an exact identity. Results and errors come directly from the
    manager.

    The frozen wrapper performs no constructor validation, owns no manager lifetime, and caches no
    selection. eq=False retains identity equality; the manager is omitted from the generated
    representation.

    Example:
        >>> factory = manager.location_factory  # doctest: +SKIP
        >>> location = factory.from_id(asset_id, require_verified=True)  # doctest: +SKIP


    :ivar _manager: Live object implementing the two location-selection methods.
    """

    _manager: _AssetLocator = field(repr=False)

    def from_id(
        self,
        digital_asset_id: DigitalAssetID,
        *,
        preferred_store_ref: StoreUUID | None = None,
        require_verified: bool = False,
    ) -> Location:
        """
        Delegate current Asset-to-Replica selection with both preference arguments unchanged. The
        selected address can change as manager metadata or Replica availability changes. This
        wrapper adds no ID validation, availability probe, or verification work.

        Example:
            >>> location = factory.from_id(asset_id, preferred_store_ref=store_uuid)  # doctest: +SKIP


        :param digital_asset_id: Catalogue Digital Asset identity whose eligible Replica is selected by the manager.
        :param preferred_store_ref: Optional configured Store UUID forwarded as a selection preference.
        :param require_verified: Whether the manager must select a Replica satisfying its verified-selection contract.
        :return: Location returned by the manager for the selected Asset Replica.
        """

        return self._manager.locate_digital_asset(
            digital_asset_id,
            preferred_store_ref=preferred_store_ref,
            require_verified=require_verified,
        )

    def from_digital_asset_id(
        self,
        digital_asset_id: DigitalAssetID,
        *,
        preferred_store_ref: StoreUUID | None = None,
        require_verified: bool = False,
    ) -> Location:
        """
        Forward the explicit Asset-named entry point through from_id. Dynamic dispatch and all
        supplied selection arguments are retained, so an override of from_id also controls this
        convenience.

        Example:
            >>> location = factory.from_digital_asset_id(asset_id)  # doctest: +SKIP


        :param digital_asset_id: Catalogue Digital Asset identity whose eligible Replica is selected by the manager.
        :param preferred_store_ref: Optional configured Store UUID forwarded as a selection preference.
        :param require_verified: Whether the manager must select a Replica satisfying its verified-selection contract.
        :return: Location returned by from_id under the requested selection preferences.
        """

        return self.from_id(
            digital_asset_id,
            preferred_store_ref=preferred_store_ref,
            require_verified=require_verified,
        )

    def from_replica_id(self, replica_id: ReplicaID) -> Location:
        """
        Delegate resolution of one exact Replica identity without choosing among alternative IDs.
        The manager owns lookup, eligibility, and errors; this wrapper neither validates the
        identifier nor probes bytes.

        Example:
            >>> location = factory.from_replica_id(replica_id)  # doctest: +SKIP


        :param replica_id: Exact catalogue Replica identity to resolve through the manager.
        :return: Location returned by the manager for that Replica.
        """

        return self._manager.locate_replica(replica_id)


__all__ = ["LocationFactory"]
