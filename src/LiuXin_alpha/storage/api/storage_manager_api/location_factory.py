"""Manager-bound factories for resolving catalogue identities to Locations."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol

from LiuXin_alpha.storage.api.models import Location, StoreRef
from LiuXin_alpha.storage.api.storage_manager_api.models import (
    DigitalAssetID,
    ReplicaID,
)


class _AssetLocator(Protocol):
    """Location-resolution subset required by ``LocationFactory``.

    Example:
        >>> def accepts_locator(locator: _AssetLocator) -> None:
        ...     pass
    """

    def locate_digital_asset(
        self,
        digital_asset_id: DigitalAssetID,
        *,
        preferred_store: StoreRef | None = None,
        require_verified: bool = False,
    ) -> Location:
        """Select a readable Location for one Digital Asset.

        Example:
            >>> location = locator.locate_digital_asset(  # doctest: +SKIP
            ...     DigitalAssetID(7),
            ... )
        """
        ...

    def locate_replica(self, replica_id: ReplicaID) -> Location:
        """Resolve one exact Replica Location.

        Example:
            >>> location = locator.locate_replica(ReplicaID(12))  # doctest: +SKIP
        """
        ...


@dataclass(slots=True, frozen=True, eq=False)
class LocationFactory:
    """Resolve database identities through one storage manager.

    A Digital Asset may have several Replicas, so ``from_id`` performs a
    current manager selection rather than reconstructing a unique address.
    Selection policy, availability errors, and verification failures remain
    the manager's responsibility and propagate unchanged.

    Example:
        >>> factory = manager.location_factory  # doctest: +SKIP
        >>> location = factory.from_id(  # doctest: +SKIP
        ...     DigitalAssetID(7), require_verified=True,
        ... )
    """

    _manager: _AssetLocator = field(repr=False)

    def from_id(
        self,
        digital_asset_id: DigitalAssetID,
        *,
        preferred_store: StoreRef | None = None,
        require_verified: bool = False,
    ) -> Location:
        """Select one readable Location for a Digital Asset identity.

        ``preferred_store`` is a Store UUID, not its row ID or display name.
        The returned Location may change as Replica health or placement
        changes; persist it when the concrete address itself is significant.

        Example:
            >>> location = factory.from_id(  # doctest: +SKIP
            ...     DigitalAssetID(7), preferred_store=UUID(int=1),
            ...     require_verified=True,
            ... )
        """

        return self._manager.locate_digital_asset(
            digital_asset_id,
            preferred_store=preferred_store,
            require_verified=require_verified,
        )

    def from_digital_asset_id(
        self,
        digital_asset_id: DigitalAssetID,
        *,
        preferred_store: StoreRef | None = None,
        require_verified: bool = False,
    ) -> Location:
        """Explicitly named alias for :meth:`from_id`.

        Example:
            >>> location = factory.from_digital_asset_id(  # doctest: +SKIP
            ...     DigitalAssetID(7),
            ... )
        """

        return self.from_id(
            digital_asset_id,
            preferred_store=preferred_store,
            require_verified=require_verified,
        )

    def from_replica_id(self, replica_id: ReplicaID) -> Location:
        """Resolve one exact Replica identity to its Location.

        Unlike ``from_id``, this performs no choice among a Digital Asset's
        Replicas because the Replica ID already identifies one concrete copy.

        Example:
            >>> location = factory.from_replica_id(ReplicaID(12))  # doctest: +SKIP
        """

        return self._manager.locate_replica(replica_id)


__all__ = ["LocationFactory"]
