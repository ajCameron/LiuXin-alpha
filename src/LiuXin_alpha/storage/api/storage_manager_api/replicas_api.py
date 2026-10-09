"""
Define public Replica lifecycle operations above configured Store byte mechanics.

Lookup, replication, verification, and removal exchange domain records and reports.
Each operation exposes its selection and safety controls without promising an
atomic transaction across backend bytes and manager metadata.
"""

from __future__ import annotations

import abc

from collections.abc import Iterable, Iterator

from LiuXin_alpha.storage.api.models import StoreUUID
from LiuXin_alpha.storage.api.placement_hints_api import StoragePlacementHints
from LiuXin_alpha.storage.api.storage_manager_api.models import (
    CompositeDigitalAssetID,
    CompositeDigitalAssetVerificationReport,
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
    Define record lookup, replication, verification, and removal for concrete Asset copies.

    Callers exchange public domain records while implementations own persistence and Store
    operations. Reports describe observed outcomes; successful method return does not universally
    imply healthy bytes or an atomic change across storage and metadata.

    Example:
        >>> report = manager.verify_replica(replica_id)  # doctest: +SKIP
    """

    @abc.abstractmethod
    def get_replica_record(self, replica_id: ReplicaID) -> ReplicaRecord:
        """
        Resolve a recorded Replica identity or raise ReplicaNotFound without implying current byte
        availability.

        Example:
            >>> replica = manager.get_replica_record(ReplicaID(12))  # doctest: +SKIP

        :param replica_id: Manager-assigned Replica ID to resolve.

        :return: Replica domain record for the requested identity.
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
        Iterate recorded claims matching all supplied filters.

        No lifecycle-state filter is requested by this signature; callers must distinguish deleted
        or unavailable claims when selecting readable copies. Ordering and snapshot behavior belong
        to the implementation.

        Example:
            >>> replicas = tuple(manager.iter_replica_records(digital_asset_id=asset_id, mode=ReplicaMode.ACTIVE))  # doctest: +SKIP


        :param digital_asset_id: Optional owning Asset ID; None leaves ownership unconstrained.
        :param store_ref: Optional Store UUID; None leaves destination unconstrained.
        :param mode: Optional operational mode; None includes every mode.
        :return: Iterator of Replica records satisfying the selected filters.
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
        Publish another concrete copy and register its Replica claim, optionally recording
        verification.

        An omitted source requests manager selection and an omitted destination requests the default
        Store. Implementations should inherit recorded source placement hints unless explicitly
        overridden. Verification may record an unhealthy result; callers should inspect the returned
        record. Backend publication and metadata mutation need not form one transaction.

        Example:
            >>> replica = manager.replicate_digital_asset(asset_id, destination_store_ref=archive_uuid)  # doctest: +SKIP


        :param digital_asset_id: Manager-assigned atomic Asset ID to resolve.
        :param destination_store_ref: Destination Store UUID, or None to use the configured default.
        :param source_replica_id: Exact source Replica ID, or None to let the manager select a readable copy.
        :param placement_hints: Optional destination-placement override; None requests reuse of recorded source hints.
        :param mode: Operational purpose assigned to the resulting Replica.
        :param verify: Whether to inspect and record the published Replica after registration.
        :return: Registered Replica record carrying the resulting observation; errors may occur after byte publication.
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
        Compare a concrete copy with its Asset identity and record the observation.

        Missing, corrupt, or unavailable storage can produce a diagnostic report rather than an
        exception. Other backend or metadata failures can propagate. Digest inspection may use
        authoritative backend evidence instead of rereading bytes.

        Example:
            >>> report = manager.verify_replica(replica_id, calculate_digests=False)  # doctest: +SKIP


        :param replica_id: Manager-assigned Replica ID to resolve.
        :param calculate_digests: Whether to request digest evidence in addition to existence and size checks.
        :return: Verification report for the inspected copy, whose healthy flag must be checked separately.
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
        Verify an explicit Replica subset or scan the Asset's nondeleted claims.

        An implicit scan stops after the first healthy report by default. An explicit nonempty,
        duplicate-free subset preserves caller order and checks all selected IDs by default.
        stop_after_first_healthy overrides either default; all_replicas is its inverse compatibility
        spelling and cannot be supplied alongside it. Earlier observations can persist if a later
        verification fails.

        Example:
            >>> report = manager.verify_digital_asset(asset_id, all_replicas=True)  # doctest: +SKIP


        :param digital_asset_id: Manager-assigned atomic Asset ID to resolve.
        :param replica_ids: Exact ordered nonempty subset, or None to select all nondeleted claims before applying the stop policy.
        :param stop_after_first_healthy: Explicit early-stop policy; None selects the implicit-scan or explicit-subset default.
        :param all_replicas: Compatibility inverse of the stop policy, mutually exclusive with an explicit stop_after_first_healthy value.
        :return: Aggregate of reports actually produced, potentially a prefix of the selected records.
        """
        ...

    @abc.abstractmethod
    def verify_composite_digital_asset(
        self,
        composite_digital_asset_id: CompositeDigitalAssetID,
        *,
        all_replicas: bool = False,
    ) -> CompositeDigitalAssetVerificationReport:
        """
        Verify every distinct member Asset and retain evidence for each membership occurrence.

        By default each atomic verification stops after its first healthy Replica; ``all_replicas``
        requests inspection of every nondeleted claim. Repeated memberships reuse one atomic
        verification result during this call, avoiding duplicate Store reads while preserving
        relationship-specific report entries. Optional members are verified and reported but do not
        determine the aggregate readable predicate.

        Verification proceeds member by member without a cross-Asset transaction or Store snapshot.
        Earlier Replica observations can remain persisted if a later member fails unexpectedly.

        Example:
            >>> report = manager.verify_composite_digital_asset(  # doctest: +SKIP
            ...     composite_id, all_replicas=True,
            ... )

        :param composite_digital_asset_id: Registered Composite identity whose members are inspected.
        :param all_replicas: Whether each distinct Asset verifies every nondeleted Replica instead of stopping after the first healthy result.
        :return: Composite record and ordered verification evidence for every declared relationship.
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
        Coordinate optional physical deletion with a deleted-state tombstone or record removal.

        Preserving bytes and retaining a tombstone is permitted. Backend deletion and metadata
        mutation can fail separately; a removal report is produced only when the implementation
        completes its sequence.

        Example:
            >>> result = manager.remove_replica(replica_id, delete_bytes=False, retain_tombstone=True)  # doctest: +SKIP


        :param replica_id: Manager-assigned Replica ID to resolve.
        :param delete_bytes: Whether to request deletion of bytes at the claimed Location.
        :param retain_tombstone: Whether to retain a DELETED observation instead of removing the Replica record.
        :return: Removal outcome flags and warnings, with semantics defined by the producing workflow.
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
        Remove a Replica claim without deleting physical bytes.

        The default requires evidence that the claimed bytes are absent. Unknown availability must
        remain an error rather than being interpreted as absence. A supplied revision guards
        metadata mutation; callers can explicitly waive the byte-absence requirement.

        Example:
            >>> removed = manager.forget_replica(replica_id, if_revision=replica.revision)  # doctest: +SKIP


        :param replica_id: Manager-assigned Replica ID to resolve.
        :param require_bytes_absent: Whether storage must report the claimed object missing before metadata can be removed.
        :param if_revision: Expected current revision, or None to omit the optimistic precondition.
        :return: True when the claim is forgotten, False if already absent; precondition and storage failures can propagate.
        """
        ...

__all__ = ["ReplicaLifecycleAPI"]
