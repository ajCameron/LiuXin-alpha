"""Asset-oriented retrieval facade."""

import abc

from LiuXin_alpha.storage.api2.models import Location, StoreRef
from LiuXin_alpha.storage.api2.storage_manager_api.models import (
    AssetReplicaID, AssetReplicaRecordAPI, DigitalAssetID, ItemAssetSelection,
    ItemID, ReplicaMode,
)


class AssetRetrievalAPI(abc.ABC):
    """Resolve logical assets and item roles to readable locations.

    Example:
        >>> def locate(
        ...     manager: AssetRetrievalAPI, asset_id: DigitalAssetID,
        ... ) -> Location:
        ...     return manager.locate_digital_asset(asset_id, verify=True)
    """

    @abc.abstractmethod
    def select_asset_replica(
        self, digital_asset_id: DigitalAssetID, *, preferred_store: StoreRef | None = None,
        replica_mode: ReplicaMode = ReplicaMode.ACTIVE, require_verified: bool = False,
    ) -> AssetReplicaRecordAPI:
        """Choose the best readable replica record for a Digital Asset.

        Example:
            >>> replica = manager.select_asset_replica(  # doctest: +SKIP
            ...     7, preferred_store="primary", require_verified=True,
            ... )
        """
        ...

    @abc.abstractmethod
    def locate_digital_asset(
        self, digital_asset_id: DigitalAssetID, *, preferred_store: StoreRef | None = None,
        verify: bool = False,
    ) -> Location:
        """Resolve a Digital Asset to one readable replica location.

        Example:
            >>> location = manager.locate_digital_asset(7, verify=True)  # doctest: +SKIP
        """
        ...

    @abc.abstractmethod
    def locate_asset_replica(self, asset_replica_id: AssetReplicaID) -> Location:
        """Resolve one replica record to its concrete backend location.

        Example:
            >>> location = manager.locate_asset_replica(12)  # doctest: +SKIP
        """
        ...

    @abc.abstractmethod
    def materialize_digital_asset(
        self, digital_asset_id: DigitalAssetID, *, preferred_store: StoreRef | None = None,
        cache_store: StoreRef | None = None, verify: bool = True,
    ) -> Location:
        """Ensure an asset is locally readable, optionally through a cache store.

        Example:
            >>> location = manager.materialize_digital_asset(  # doctest: +SKIP
            ...     7, cache_store="local-cache",
            ... )
        """
        ...

    @abc.abstractmethod
    def locate_item_asset(
        self, item_id: ItemID, *, role: str = "primary_payload",
        preferred_store: StoreRef | None = None, verify: bool = False,
    ) -> ItemAssetSelection:
        """Resolve an Item role to an atomic or composite asset selection.

        Example:
            >>> selection = manager.locate_item_asset(  # doctest: +SKIP
            ...     9, role="cover", verify=True,
            ... )
        """
        ...


__all__ = ["AssetRetrievalAPI"]
