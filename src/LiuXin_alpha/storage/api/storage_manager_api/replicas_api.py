"""
Replica lifecycle operations above Store byte mechanics.
"""

import abc

from collections.abc import Iterator

from LiuXin_alpha.storage.api.models import StoreUUID
from LiuXin_alpha.storage.api.storage_manager_api.models import (
    DigitalAssetVerificationReport,
    DigitalAssetID,
    ReplicaRecord,
    ReplicaID,
    ReplicaMode,
    ReplicaRemovalReport,
    ReplicaVerificationReport,
)


class ReplicaLifecycleAPI(abc.ABC):
    """
    Lifecycle operations for concrete copies of Digital Assets.

    Replica persistence is an implementation detail behind these operations;
    callers deal in public records rather than row-shaped protocols.

    Example:
        >>> def verify(
        ...     manager: ReplicaLifecycleAPI, replica_id: ReplicaID,
        ... ) -> ReplicaVerificationReport:
        ...     return manager.verify_replica(replica_id)
    """

    @abc.abstractmethod
    def get_replica_record(self, replica_id: ReplicaID) -> ReplicaRecord:
        """
        Return one Replica record or raise ``ReplicaNotFound``.

        Example:
            >>> record = manager.get_replica_record(  # doctest: +SKIP
            ...     ReplicaID(12),
            ... )


        :param replica_id:
        :return:
        """
        ...

    @abc.abstractmethod
    def iter_replica_records(
        self,
        *,
        digital_asset_id: DigitalAssetID | None = None,
        store_ref: StoreUUID | None = None,
        mode: ReplicaMode | None = None,
    ) -> Iterator[ReplicaRecord]:
        """
        Iterate over Replica records matching optional filters.

        Example:
            >>> records = list(manager.iter_replica_records(  # doctest: +SKIP
            ...     digital_asset_id=DigitalAssetID(7),
            ...     mode=ReplicaMode.ACTIVE,
            ... ))


        :param digital_asset_id:
        :param store_ref:
        :param mode:
        :return:
        """
        ...

    # Todo: Ideally we should have the metadata which was used to place the asset in the first place...
    # Todo: Possibly store it as a json blob with the DigitalAssetRecord
    @abc.abstractmethod
    def replicate_digital_asset(
        self,
        digital_asset_id: DigitalAssetID,
        *,
        destination_store_ref: StoreUUID | None = None,
        source_replica_id: ReplicaID | None = None,
        mode: ReplicaMode = ReplicaMode.ACTIVE,
        verify: bool = True,
    ) -> ReplicaRecord:
        """
        Create, publish, verify, and register another concrete copy.

        Example:
            >>> replica_record = manager.replicate_digital_asset(  # doctest: +SKIP
            ...     DigitalAssetID(7), destination_store_ref=destination_uuid,
            ... )


        :param digital_asset_id:
        :param destination_store_ref:
        :param source_replica_id:
        :param mode:
        :param verify:
        :return:
        """
        ...

    @abc.abstractmethod
    def verify_replica(
        self,
        replica_id: ReplicaID,
        *,
        calculate_digests: bool = True,
    ) -> ReplicaVerificationReport:
        """
        Compare one concrete copy with its Digital Asset identity.

        Example:
            >>> report = manager.verify_replica(ReplicaID(12))  # doctest: +SKIP


        :param replica_id:
        :param calculate_digests:
        :return:
        """
        ...

    # Todo: We should be able to specify a subset of replicas. It's not clear what behavior passing false produces
    @abc.abstractmethod
    def verify_digital_asset(
        self,
        digital_asset_id: DigitalAssetID,
        *,
        all_replicas: bool = False,
    ) -> DigitalAssetVerificationReport:
        """
        Verify enough Replicas, or every Replica, for one Asset.

        Example:
            >>> report = manager.verify_digital_asset(  # doctest: +SKIP
            ...     DigitalAssetID(7), all_replicas=True,
            ... )

        :param digital_asset_id:
        :param all_replicas:
        :return:
        """
        ...

    @abc.abstractmethod
    def remove_replica(
        self,
        replica_id: ReplicaID,
        *,
        delete_bytes: bool = True,
        retain_tombstone: bool = True,
    ) -> ReplicaRemovalReport:
        """
        Coordinate physical deletion with Replica-domain state mutation.

        Example:
            >>> report = manager.remove_replica(  # doctest: +SKIP
            ...     ReplicaID(12), retain_tombstone=True,
            ... )


        :param replica_id:
        :param delete_bytes:
        :param retain_tombstone:
        :return:
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
        """
        Forget a Replica claim without deleting physical bytes.

        The safe default first requires evidence that the bytes are absent.

        Example:
            >>> forgotten = manager.forget_replica(  # doctest: +SKIP
            ...     ReplicaID(12), require_bytes_absent=True,
            ... )


        :param replica_id:
        :param require_bytes_absent:
        :param if_revision:
        :return:
        """
        ...


__all__ = ["ReplicaLifecycleAPI"]
