"""
Replica lifecycle operations above Store byte mechanics.
"""

import abc

from collections.abc import Iterable, Iterator

from LiuXin_alpha.storage.api.models import StoreUUID
from LiuXin_alpha.storage.api.placement_hints_api import StoragePlacementHints
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

    @abc.abstractmethod
    def replicate_digital_asset(
        self,
        digital_asset_id: DigitalAssetID,
        *,
        destination_store_ref: StoreUUID | None = None,
        source_replica_id: ReplicaID | None = None,
        placement_hints: StoragePlacementHints | None = None,
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
        :param placement_hints: Optional destination-placement override. When
            omitted, implementations should reuse the source Replica's
            recorded placement snapshot when available.
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

    @abc.abstractmethod
    def verify_digital_asset(
        self,
        digital_asset_id: DigitalAssetID,
        *,
        replica_ids: Iterable[ReplicaID] | None = None,
        stop_after_first_healthy: bool | None = None,
        all_replicas: bool | None = None,
    ) -> DigitalAssetVerificationReport:
        """
        Verify selected Replicas, enough Replicas, or every Replica.

        With no explicit selection, verification stops after the first healthy
        Replica. Supplying ``replica_ids`` checks that exact subset in caller
        order by default. ``stop_after_first_healthy`` makes either behavior
        explicit. ``all_replicas`` is the compatibility spelling and cannot
        be combined with an explicit stop policy.

        Example:
            >>> report = manager.verify_digital_asset(  # doctest: +SKIP
            ...     DigitalAssetID(7), all_replicas=True,
            ... )

        :param digital_asset_id:
        :param replica_ids: Exact Replica identities to check, or every live
            Replica belonging to the Asset when omitted.
        :param stop_after_first_healthy: Whether a healthy result ends the
            scan. Defaults to true for an implicit scan and false for an exact
            subset.
        :param all_replicas: Compatibility alias for the inverse stop policy.
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
