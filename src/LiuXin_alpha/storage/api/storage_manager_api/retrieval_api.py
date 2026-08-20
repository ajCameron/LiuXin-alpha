"""
Asset-oriented retrieval facade.
"""

import abc

from LiuXin_alpha.storage.api.models import Location, StoreUUID
from LiuXin_alpha.storage.api.storage_manager_api.location_factory import LocationFactory
from LiuXin_alpha.storage.api.storage_manager_api.models import (
    DigitalAssetID,
    DigitalAssetResolution,
    ItemDigitalAssetResolution,
    ItemID,
    ReplicaRecord,
    ReplicaID,
    ReplicaMode,
)


class DigitalAssetRetrievalAPI(abc.ABC):
    """
    Resolve logical Assets and Item roles to readable Replicas.

    Selection returns enough domain context to identify both the expected
    bytes and the chosen concrete copy. ``Location``-only helpers remain
    available for callers that need only routing.

    Example:
        >>> resolved = manager.resolve_digital_asset(  # doctest: +SKIP
        ...     DigitalAssetID(7), require_verified=True,
        ... )
        >>> location = resolved.location  # doctest: +SKIP
    """

    @property
    def location_factory(self) -> LocationFactory:
        """
        Return catalogue-aware Location factories bound to this manager.

        Example:
            >>> location = manager.location_factory.from_id(  # doctest: +SKIP
            ...     DigitalAssetID(7),
            ... )


        :return:
        """

        return LocationFactory(self)

    @abc.abstractmethod
    def select_replica(
        self,
        digital_asset_id: DigitalAssetID,
        *,
        preferred_store_ref: StoreUUID | None = None,
        mode: ReplicaMode = ReplicaMode.ACTIVE,
        require_verified: bool = False,
    ) -> ReplicaRecord:
        """
        Choose the best readable Replica for one Digital Asset.

        A known Asset without a suitable copy raises ``NoReadableReplica``.

        Example:
            >>> replica_record = manager.select_replica(  # doctest: +SKIP
            ...     DigitalAssetID(7), require_verified=True,
            ... )


        :param digital_asset_id:
        :param preferred_store_ref:
        :param mode:
        :param require_verified:
        :return:
        """
        ...

    @abc.abstractmethod
    def resolve_digital_asset(
        self,
        digital_asset_id: DigitalAssetID,
        *,
        preferred_store_ref: StoreUUID | None = None,
        mode: ReplicaMode = ReplicaMode.ACTIVE,
        require_verified: bool = False,
    ) -> DigitalAssetResolution:
        """
        Return the Asset identity paired with one selected Replica.

        Example:
            >>> resolved = manager.resolve_digital_asset(  # doctest: +SKIP
            ...     DigitalAssetID(7), require_verified=True,
            ... )


        :param digital_asset_id:
        :param preferred_store_ref:
        :param mode:
        :param require_verified:
        :return:
        """
        ...

    def locate_digital_asset(
        self,
        digital_asset_id: DigitalAssetID,
        *,
        preferred_store_ref: StoreUUID | None = None,
        mode: ReplicaMode = ReplicaMode.ACTIVE,
        require_verified: bool = False,
    ) -> "Location":
        """
        Resolve one Digital Asset to the chosen Replica Location.

        Example:
            >>> location = manager.locate_digital_asset(  # doctest: +SKIP
            ...     DigitalAssetID(7), require_verified=True,
            ... )


        :param digital_asset_id:
        :param preferred_store_ref:
        :param mode:
        :param require_verified:
        :return:
        """

        return self.resolve_digital_asset(
            digital_asset_id,
            preferred_store_ref=preferred_store_ref,
            mode=mode,
            require_verified=require_verified,
        ).location

    @abc.abstractmethod
    def locate_replica(self, replica_id: ReplicaID) -> "Location":
        """
        Resolve one exact Replica identity to its concrete Location.

        Example:
            >>> location = manager.locate_replica(ReplicaID(12))  # doctest: +SKIP


        :param replica_id:
        :return:
        """
        ...

    @abc.abstractmethod
    def materialize_digital_asset(
        self,
        digital_asset_id: DigitalAssetID,
        *,
        preferred_store_ref: StoreUUID | None = None,
        cache_store_ref: StoreUUID | None = None,
        verify: bool = True,
    ) -> DigitalAssetResolution:
        """
        Ensure an Asset is locally readable and return the resulting copy.

        Example:
            >>> resolved = manager.materialize_digital_asset(  # doctest: +SKIP
            ...     DigitalAssetID(7), cache_store_ref=cache_uuid,
            ... )


        :param digital_asset_id:
        :param preferred_store_ref:
        :param cache_store_ref:
        :param verify:
        :return:
        """
        ...

    @abc.abstractmethod
    def resolve_item_digital_asset(
        self,
        item_id: ItemID,
        *,
        role: str = "primary_payload",
        preferred_store_ref: StoreUUID | None = None,
        require_verified: bool = False,
    ) -> ItemDigitalAssetResolution:
        """
        Resolve one Item role to an atomic or Composite Asset selection.

        Example:
            >>> selection = manager.resolve_item_digital_asset(  # doctest: +SKIP
            ...     ItemID(9), role="cover", require_verified=True,
            ... )


        :param item_id:
        :param role:
        :param preferred_store_ref:
        :param require_verified:
        :return:
        """
        ...


__all__ = ["DigitalAssetRetrievalAPI"]
