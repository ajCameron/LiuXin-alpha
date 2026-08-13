"""Replica lifecycle operations above Store byte mechanics."""

import abc

from collections.abc import Iterator

from LiuXin_alpha.storage.api.models import StoreRef
from LiuXin_alpha.storage.api.storage_manager_api.models import (
    AssetVerificationResult,
    DigitalAssetID,
    Replica,
    ReplicaID,
    ReplicaMode,
    ReplicaRemovalResult,
    ReplicaVerificationResult,
)


class ReplicaLifecycleAPI(abc.ABC):
    """Lifecycle operations for concrete copies of Digital Assets.

    Replica persistence is an implementation detail behind these operations;
    callers deal in domain snapshots rather than row-shaped protocols.

    Example:
        >>> def verify(
        ...     manager: ReplicaLifecycleAPI, replica_id: ReplicaID,
        ... ) -> ReplicaVerificationResult:
        ...     return manager.verify_replica(replica_id)
    """

    @abc.abstractmethod
    def get_replica(self, replica_id: ReplicaID) -> Replica:
        """Return one Replica snapshot or raise ``ReplicaNotFound``.

        Example:
            >>> replica = manager.get_replica(ReplicaID(12))  # doctest: +SKIP
        """
        ...

    @abc.abstractmethod
    def iter_replicas(
        self,
        *,
        digital_asset_id: DigitalAssetID | None = None,
        store_ref: StoreRef | None = None,
        mode: ReplicaMode | None = None,
    ) -> Iterator[Replica]:
        """Iterate over Replica snapshots matching optional filters.

        Example:
            >>> replicas = list(manager.iter_replicas(  # doctest: +SKIP
            ...     digital_asset_id=DigitalAssetID(7),
            ...     mode=ReplicaMode.ACTIVE,
            ... ))
        """
        ...

    @abc.abstractmethod
    def replicate_digital_asset(
        self,
        digital_asset_id: DigitalAssetID,
        *,
        destination_store: StoreRef | None = None,
        source_replica_id: ReplicaID | None = None,
        mode: ReplicaMode = ReplicaMode.ACTIVE,
        verify: bool = True,
    ) -> Replica:
        """Create, publish, verify, and register another concrete copy.

        Example:
            >>> replica = manager.replicate_digital_asset(  # doctest: +SKIP
            ...     DigitalAssetID(7), destination_store=destination_uuid,
            ... )
        """
        ...

    @abc.abstractmethod
    def verify_replica(
        self,
        replica_id: ReplicaID,
        *,
        calculate_digests: bool = True,
    ) -> ReplicaVerificationResult:
        """Compare one concrete copy with its Digital Asset identity.

        Example:
            >>> result = manager.verify_replica(ReplicaID(12))  # doctest: +SKIP
        """
        ...

    @abc.abstractmethod
    def verify_digital_asset(
        self,
        digital_asset_id: DigitalAssetID,
        *,
        all_replicas: bool = False,
    ) -> AssetVerificationResult:
        """Verify enough Replicas, or every Replica, for one Asset.

        Example:
            >>> result = manager.verify_digital_asset(  # doctest: +SKIP
            ...     DigitalAssetID(7), all_replicas=True,
            ... )
        """
        ...

    @abc.abstractmethod
    def remove_replica(
        self,
        replica_id: ReplicaID,
        *,
        delete_bytes: bool = True,
        retain_tombstone: bool = True,
    ) -> ReplicaRemovalResult:
        """Coordinate physical deletion with Replica-domain state mutation.

        Example:
            >>> result = manager.remove_replica(  # doctest: +SKIP
            ...     ReplicaID(12), retain_tombstone=True,
            ... )
        """
        ...

    @abc.abstractmethod
    def forget_replica(
        self,
        replica_id: ReplicaID,
        *,
        require_bytes_absent: bool = True,
        if_revision: str | None = None,
    ) -> bool:
        """Forget a Replica claim without deleting physical bytes.

        The safe default first requires evidence that the bytes are absent.

        Example:
            >>> forgotten = manager.forget_replica(  # doctest: +SKIP
            ...     ReplicaID(12), require_bytes_absent=True,
            ... )
        """
        ...


__all__ = ["ReplicaLifecycleAPI"]
