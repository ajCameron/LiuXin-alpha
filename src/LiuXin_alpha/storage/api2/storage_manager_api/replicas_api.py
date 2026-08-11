"""Asset Replica lifecycle facade."""

import abc

from collections.abc import Iterator

from LiuXin_alpha.storage.api2.models import StoreRef
from LiuXin_alpha.storage.api2.storage_manager_api.models import (
    AssetReplicaID, AssetReplicaRecordAPI, AssetVerificationResult, DigitalAssetID,
    ReplicaMode, ReplicaRemovalResult, ReplicaVerificationResult,
)


class ReplicaLifecycleAPI(abc.ABC):
    """Lifecycle operations for physical copies of Digital Assets.

    Example:
        >>> def verify(
        ...     manager: ReplicaLifecycleAPI, replica_id: AssetReplicaID,
        ... ) -> ReplicaVerificationResult:
        ...     return manager.verify_asset_replica(replica_id)
    """

    @abc.abstractmethod
    def create_asset_replica_record(self, replica: AssetReplicaRecordAPI) -> AssetReplicaRecordAPI:
        """Persist metadata for a newly created physical replica.

        Example:
            >>> created = manager.create_asset_replica_record(replica)  # doctest: +SKIP
        """
        ...

    @abc.abstractmethod
    def get_asset_replica(self, asset_replica_id: AssetReplicaID) -> AssetReplicaRecordAPI:
        """Return one Asset Replica record by identifier.

        Example:
            >>> replica = manager.get_asset_replica(12)  # doctest: +SKIP
        """
        ...

    @abc.abstractmethod
    def update_asset_replica_record(self, replica: AssetReplicaRecordAPI) -> AssetReplicaRecordAPI:
        """Persist changes to an Asset Replica record.

        Example:
            >>> updated = manager.update_asset_replica_record(replica)  # doctest: +SKIP
        """
        ...

    @abc.abstractmethod
    def iter_asset_replicas(
        self, *, digital_asset_id: DigitalAssetID | None = None,
        store_ref: StoreRef | None = None, replica_mode: ReplicaMode | None = None,
    ) -> Iterator[AssetReplicaRecordAPI]:
        """Iterate over replica records matching optional filters.

        Example:
            >>> replicas = list(manager.iter_asset_replicas(  # doctest: +SKIP
            ...     digital_asset_id=7, replica_mode=ReplicaMode.ACTIVE,
            ... ))
        """
        ...

    @abc.abstractmethod
    def replicate_digital_asset(
        self, digital_asset_id: DigitalAssetID, *, destination_store: StoreRef | None = None,
        source_replica_id: AssetReplicaID | None = None,
        replica_mode: ReplicaMode = ReplicaMode.ACTIVE, verify: bool = True,
    ) -> AssetReplicaRecordAPI:
        """Create and catalogue a new physical copy of an existing asset.

        Example:
            >>> replica = manager.replicate_digital_asset(  # doctest: +SKIP
            ...     7, destination_store="mirror", verify=True,
            ... )
        """
        ...

    @abc.abstractmethod
    def verify_asset_replica(
        self, asset_replica_id: AssetReplicaID, *, calculate_digest: bool = True,
    ) -> ReplicaVerificationResult:
        """Verify one replica's presence, size, and optionally digest.

        Example:
            >>> result = manager.verify_asset_replica(12)  # doctest: +SKIP
        """
        ...

    @abc.abstractmethod
    def verify_digital_asset(
        self, digital_asset_id: DigitalAssetID, *, all_replicas: bool = False,
    ) -> AssetVerificationResult:
        """Verify enough replicas, or every replica, for one Digital Asset.

        Example:
            >>> result = manager.verify_digital_asset(  # doctest: +SKIP
            ...     7, all_replicas=True,
            ... )
        """
        ...

    @abc.abstractmethod
    def remove_asset_replica(
        self, asset_replica_id: AssetReplicaID, *, delete_bytes: bool = True,
        retain_tombstone: bool = True,
    ) -> ReplicaRemovalResult:
        """Coordinate physical deletion with replica metadata removal.

        Example:
            >>> result = manager.remove_asset_replica(  # doctest: +SKIP
            ...     12, delete_bytes=True, retain_tombstone=True,
            ... )
        """
        ...

    @abc.abstractmethod
    def forget_asset_replica(
        self, asset_replica_id: AssetReplicaID, *, require_bytes_absent: bool = True,
    ) -> bool:
        """Remove replica metadata without deleting physical bytes.

        Example:
            >>> forgotten = manager.forget_asset_replica(  # doctest: +SKIP
            ...     12, require_bytes_absent=True,
            ... )
        """
        ...


__all__ = ["ReplicaLifecycleAPI"]
