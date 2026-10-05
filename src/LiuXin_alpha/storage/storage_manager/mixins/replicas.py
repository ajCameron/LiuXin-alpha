"""
Implement Replica lookup, byte replication, observation updates, and claim removal.

Store operations precede or follow separate manager metadata transactions. The
methods expose recorded outcomes and typed failures without providing rollback of
completed physical changes when a later phase fails.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Iterable, Iterator
from datetime import UTC, datetime
from typing import override

import LiuXin_alpha.storage.api as api
from LiuXin_alpha.storage.storage_manager.mixins._state import _StorageManagerState


class ReplicaLifecycleMixin(_StorageManagerState):
    """
    Manage Replica claims and coordinate their recorded observations with Store operations.

    The mixin uses shared allocation, selection, inspection, and repository hooks. Byte
    publication/deletion occurs separately from metadata transactions, so errors can leave completed
    byte changes or earlier recorded observations in place. A retained claim is not proof that
    physical bytes remain readable.

    Example:
        >>> replica = manager.replicate_digital_asset(asset_id, destination_store_ref=archive_uuid)  # doctest: +SKIP
    """

    @override
    def get_replica_record(
        self,
        replica_id: api.ReplicaID,
    ) -> api.ReplicaRecord:
        """
        Look up the exact Replica key under the manager lock.

        A mapping KeyError becomes ReplicaNotFound with the original error chained. Other repository
        failures propagate; no physical Store operation occurs.

        Example:
            >>> replica = manager.get_replica_record(replica_id)  # doctest: +SKIP


        :param replica_id: Manager-assigned Replica ID to resolve.
        :return: Retained Replica record for the requested identity.
        """

        with self._lock:
            try:
                return self._replicas[replica_id]
            except KeyError as error:
                raise api.ReplicaNotFound(
                    f"Replica {replica_id} is not registered."
                ) from error

    @override
    def iter_replica_records(
        self,
        *,
        digital_asset_id: api.DigitalAssetID | None = None,
        store_ref: api.StoreUUID | None = None,
        mode: api.ReplicaMode | None = None,
    ) -> Iterator[api.ReplicaRecord]:
        """
        Capture Replica records under the lock in ascending ID-key order, applying every supplied
        filter.

        Asset and Store filters compare by equality; mode compares by enum identity and is not
        coerced from strings. State is not filtered, so deleted or unavailable claims remain
        eligible. The returned tuple iterator retains record references rather than deep copies.

        Example:
            >>> replicas = tuple(manager.iter_replica_records(store_ref=store_uuid))  # doctest: +SKIP


        :param digital_asset_id: Optional exact owning Asset ID; None omits this comparison.
        :param store_ref: Optional exact Store UUID compared with each record Location.
        :param mode: Optional mode singleton compared with is; None includes every mode.
        :return: Iterator over the captured matching records, including any matching tombstones.
        """

        with self._lock:
            records = tuple(
                record
                for _, record in sorted(self._replicas.items())
                if (
                    digital_asset_id is None
                    or record.digital_asset_id == digital_asset_id
                )
                and (store_ref is None or record.location.store_ref == store_ref)
                and (mode is None or record.mode is mode)
            )
        return iter(records)

    @override
    def replicate_digital_asset(
        self,
        digital_asset_id: api.DigitalAssetID,
        *,
        destination_store_ref: api.StoreUUID | None = None,
        source_replica_id: api.ReplicaID | None = None,
        placement_hints: api.StoragePlacementHints | None = None,
        mode: api.ReplicaMode = api.ReplicaMode.ACTIVE,
        verify: bool = True,
    ) -> api.ReplicaRecord:
        """
        Select or resolve a source, publish its bytes to a destination, then register and optionally
        verify the copy.

        An explicit source must belong to the Asset but is not otherwise selected by health or mode
        here. None placement hints inherit the source record's value; an explicit value replaces it.
        Destination checks cover configuration, mode, availability, writability, creation
        capability, and declared size through shared hooks.

        After allocation, the source reader context feeds Store.put with expected size and the
        preferred digest (SHA-256 when present). The read adds no version precondition. Source
        cleanup finishes before the PRESENT Replica claim is registered, so close or registration
        failures can leave published bytes without the new claim; no rollback is added.

        With verify enabled, verify_replica records its report and the latest record is fetched
        again. An unhealthy report does not itself raise or trigger removal: the returned record can
        be CORRUPT or UNAVAILABLE. Later errors can leave the published bytes and earlier metadata
        changes in place.

        Example:
            >>> replica = manager.replicate_digital_asset(asset_id, destination_store_ref=archive_uuid, verify=True)  # doctest: +SKIP


        :param digital_asset_id: Manager-assigned atomic Asset ID to resolve.
        :param destination_store_ref: Destination Store UUID, or None to resolve the current default.
        :param source_replica_id: Explicit source claim, or None to use select_replica.
        :param placement_hints: Destination override, or None to retain the source claim's advisory hints.
        :param mode: Requested destination mode checked against Store configuration.
        :param verify: Whether to inspect the registered copy and return its refreshed record.
        :return: Newly registered Replica record, refreshed after optional verification; healthy state is not guaranteed.
        """

        asset_record = self.get_digital_asset_record(digital_asset_id)
        source_record = (
            self.select_replica(digital_asset_id)
            if source_replica_id is None
            else self.get_replica_record(source_replica_id)
        )
        if source_record.digital_asset_id != digital_asset_id:
            raise api.StoragePreconditionFailed(
                "source Replica belongs to another Digital Asset."
            )
        effective_placement_hints = (
            source_record.placement_hints
            if placement_hints is None
            else placement_hints
        )
        destination_store_ref = (
            self.get_default_store_ref()
            if destination_store_ref is None
            else destination_store_ref
        )
        store = self._require_writable_destination(
            destination_store_ref,
            mode,
            expected_size=asset_record.size_bytes,
        )
        location = self._allocate_asset_location(
            store,
            asset_record,
            placement_hints=effective_placement_hints,
        )
        with self.get(source_record.location) as source:
            store.put(
                location,
                source,
                expected_size=asset_record.size_bytes,
                expected_digest=self._preferred_digest(asset_record),
                placement_hints=effective_placement_hints,
            )
        replica_record = self._add_replica(
            api.ReplicaDeclaration(
                digital_asset_id,
                location,
                mode,
                api.ReplicaObservation(api.ReplicaState.PRESENT),
                placement_hints=effective_placement_hints,
            )
        )
        if verify:
            self.verify_replica(replica_record.replica_id)
            replica_record = self.get_replica_record(replica_record.replica_id)
        return replica_record

    @override
    def verify_replica(
        self,
        replica_id: api.ReplicaID,
        *,
        calculate_digests: bool = True,
    ) -> api.ReplicaVerificationReport:
        """
        Inspect a Replica against its Asset and persist the resulting observation before returning
        the report.

        The shared inspector translates selected storage failures into MISSING or UNAVAILABLE
        reports and identifies size/digest disagreement as CORRUPT. Digest inspection can trust an
        authoritative SHA-256 stat value or calculate expected algorithms; disabling it leaves
        matching-size evidence PRESENT rather than VERIFIED. Unexpected lookup or inspection errors
        can propagate.

        The report becomes an observation with error messages joined by semicolons, then the update
        hook replaces metadata and advances revision/generation. Observation validation or
        persistence can fail after inspection. This method does not require the report to be
        healthy, and it adds no revision precondition tying the update to the initially read record.

        Example:
            >>> report = manager.verify_replica(replica_id, calculate_digests=True)  # doctest: +SKIP


        :param replica_id: Manager-assigned Replica ID to resolve.
        :param calculate_digests: Whether the inspector obtains digest evidence in addition to stat size/existence.
        :return: Inspection report after its observation update succeeds, including unhealthy outcomes.
        """

        record = self.get_replica_record(replica_id)
        asset_record = self.get_digital_asset_record(record.digital_asset_id)
        report = self._inspect_replica(
            record,
            asset_record,
            calculate_digests=calculate_digests,
        )
        observation = api.ReplicaObservation(
            report.state,
            observed_size_bytes=report.observed_size_bytes,
            observed_digests=report.observed_digests,
            checked_at=report.checked_at,
            failure_reason="; ".join(report.errors) if report.errors else None,
        )
        self._update_replica_observation(replica_id, observation)
        return report

    @override
    def verify_digital_asset(
        self,
        digital_asset_id: api.DigitalAssetID,
        *,
        replica_ids: Iterable[api.ReplicaID] | None = None,
        stop_after_first_healthy: bool | None = None,
        all_replicas: bool | None = None,
    ) -> api.DigitalAssetVerificationReport:
        """
        Resolve the Asset and selected claims before sequentially verifying records in the chosen
        order.

        The Asset lookup precedes argument-conflict checks. Explicit IDs are materialized, required
        to be nonempty and unique, then all resolved and checked for ownership and nondeleted state
        before any verification. An implicit selection captures the manager's ID-ordered records and
        excludes DELETED enum instances.

        The default stops after the first healthy report for an implicit scan and checks all
        explicit selections. all_replicas supplies the inverse stop policy and conflicts with any
        explicit stop_after_first_healthy value. Each verification can persist an observation; later
        failures do not undo earlier results. An implicit scan with no eligible claims returns an
        empty, unreadable aggregate.

        Example:
            >>> report = manager.verify_digital_asset(asset_id, replica_ids=(second_id, first_id))  # doctest: +SKIP


        :param digital_asset_id: Manager-assigned atomic Asset ID to resolve.
        :param replica_ids: Exact ordered subset to resolve before inspecting, or None to capture nondeleted claims.
        :param stop_after_first_healthy: Optional early-stop flag; defaults to true only for an implicit scan.
        :param all_replicas: Optional inverse stop flag; rejected whenever stop_after_first_healthy is also supplied.
        :return: Aggregate containing only reports produced before completion or a successful early stop; errors propagate instead of returning a partial aggregate.
        """

        self.get_digital_asset_record(digital_asset_id)
        if all_replicas is not None and stop_after_first_healthy is not None:
            raise ValueError(
                "all_replicas and stop_after_first_healthy are mutually exclusive."
            )
        if all_replicas is not None:
            stop_after_first_healthy = not all_replicas
        selected_ids = None if replica_ids is None else tuple(replica_ids)
        if selected_ids is not None:
            if not selected_ids:
                raise ValueError("replica_ids must not be empty when supplied.")
            if len(selected_ids) != len(set(selected_ids)):
                raise ValueError("replica_ids must not contain duplicates.")
            records = tuple(
                self.get_replica_record(replica_id) for replica_id in selected_ids
            )
            for record in records:
                if record.digital_asset_id != digital_asset_id:
                    raise api.StoragePreconditionFailed(
                        "selected Replica belongs to another Digital Asset."
                    )
                if record.state is api.ReplicaState.DELETED:
                    raise api.StoragePreconditionFailed(
                        "selected Replica has been deleted."
                    )
        else:
            records = tuple(
                record
                for record in self.iter_replica_records(
                    digital_asset_id=digital_asset_id
                )
                if record.state is not api.ReplicaState.DELETED
            )
        should_stop = (
            selected_ids is None
            if stop_after_first_healthy is None
            else stop_after_first_healthy
        )
        reports: list[api.ReplicaVerificationReport] = []
        for record in records:
            report = self.verify_replica(record.replica_id)
            reports.append(report)
            if report.healthy and should_stop:
                break
        return api.DigitalAssetVerificationReport(
            digital_asset_id,
            tuple(reports),
        )

    @override
    def remove_replica(
        self,
        replica_id: api.ReplicaID,
        *,
        delete_bytes: bool = True,
        retain_tombstone: bool = True,
    ) -> api.ReplicaRemovalReport:
        """
        Optionally delete the claimed bytes, then tombstone or remove the current Replica record.

        Only StoreNotFound from the initial stat is treated as already absent. Otherwise deletion
        requests missing_ok=True and uses the observed version only when conditional deletion is
        advertised. bytes_deleted becomes True after the delete call returns, regardless of its
        return value; this is not a separate absence check.

        Metadata is then changed under the lock and transaction hook without a revision
        precondition. Tombstoning replaces the observation with DELETED at the current UTC time and
        advances revision; either branch advances Replica generation. If bytes are deliberately
        preserved with a tombstone, the report includes a warning. A later metadata error can follow
        completed byte deletion, and no byte rollback is provided.

        Example:
            >>> report = manager.remove_replica(replica_id, retain_tombstone=True)  # doctest: +SKIP


        :param replica_id: Manager-assigned Replica ID to resolve.
        :param delete_bytes: Whether to stat and attempt deletion before metadata mutation.
        :param retain_tombstone: Whether to replace observation with DELETED instead of deleting the record.
        :return: Removal flags and warnings after both requested phases complete; unknown identities and unsuppressed failures raise.
        """

        record = self.get_replica_record(replica_id)
        bytes_deleted = False
        warnings: list[str] = []
        if delete_bytes:
            try:
                info = self.stat(record.location)
            except api.StoreNotFound:
                pass
            else:
                capabilities = self.capabilities(record.location.store_ref)
                version = info.version if capabilities.conditional_delete else None
                self.delete(
                    record.location,
                    missing_ok=True,
                    if_version=version,
                )
                bytes_deleted = True
        elif retain_tombstone:
            warnings.append("tombstone retained while physical bytes were preserved")

        with self._lock, self._metadata_transaction():
            current = self._require_replica_locked(replica_id)
            if retain_tombstone:
                self._replicas[replica_id] = dataclasses.replace(
                    current,
                    observation=api.ReplicaObservation(
                        api.ReplicaState.DELETED,
                        checked_at=datetime.now(UTC),
                    ),
                    revision=self._new_revision_locked(),
                )
                replica_forgotten = False
            else:
                del self._replicas[replica_id]
                replica_forgotten = True
            self._replica_generation += 1
        return api.ReplicaRemovalReport(
            replica_id,
            bytes_deleted,
            replica_forgotten,
            retain_tombstone,
            tuple(warnings),
        )

    @override
    def forget_replica(
        self,
        replica_id: api.ReplicaID,
        *,
        require_bytes_absent: bool = True,
        if_revision: str | None = None,
    ) -> bool:
        """
        Remove a claim after optional absence evidence and revision checks, without deleting bytes.

        Read the record under the lock and return False if absent, then check the optional revision.
        When absence is required, only StoreNotFound from stat is accepted; successful stat raises
        StoragePreconditionFailed and other errors propagate. The record is fetched again inside the
        metadata transaction, the revision is checked again, and deletion advances Replica
        generation.

        The storage observation and metadata deletion are separate operations. With no revision
        supplied, this is not an atomic proof that bytes remain absent or that the record stayed
        unchanged between checks.

        Example:
            >>> removed = manager.forget_replica(replica_id, if_revision=replica.revision)  # doctest: +SKIP


        :param replica_id: Manager-assigned Replica ID to resolve.
        :param require_bytes_absent: Whether a StoreNotFound observation is required before entering the metadata mutation.
        :param if_revision: Expected current revision, or None to omit the optimistic precondition.
        :return: True after removing the claim, or False if absent at either record lookup; precondition and storage errors propagate.
        """

        with self._lock:
            record = self._replicas.get(replica_id)
        if record is None:
            return False
        self._check_revision(record.revision, if_revision)
        if require_bytes_absent:
            try:
                self.stat(record.location)
            except api.StoreNotFound:
                pass
            else:
                raise api.StoragePreconditionFailed(
                    "Replica bytes still exist at the claimed Location."
                )
        with self._lock, self._metadata_transaction():
            current = self._replicas.get(replica_id)
            if current is None:
                return False
            self._check_revision(current.revision, if_revision)
            del self._replicas[replica_id]
            self._replica_generation += 1
            return True


__all__ = ["ReplicaLifecycleMixin"]
