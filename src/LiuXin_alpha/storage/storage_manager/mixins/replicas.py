"""
Replica lifecycle, verification, and removal workflows.
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
    Manage physical-location claims for Digital Asset bytes.

    Replica records connect one content identity to one opaque Store Location
    and track observations such as present, verified, missing, or corrupt.
    Removal coordinates Store deletion where requested, but metadata absence is
    never treated as proof that physical bytes were deleted.
    """

    @override
    def get_replica_record(
        self,
        replica_id: api.ReplicaID,
    ) -> api.ReplicaRecord:
        """
        Return one Replica record.


        :param replica_id:
        :return:
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
        Iterate over a filtered stable snapshot of Replica records.


        :param digital_asset_id:
        :param store_ref:
        :param mode:
        :return:
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
        Copy one Asset through staged publication and register its Replica.


        :param digital_asset_id:
        :param destination_store_ref:
        :param source_replica_id:
        :param placement_hints:
        :param mode:
        :param verify:
        :return:
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
        Inspect and persist one Replica's latest physical observation.


        :param replica_id:
        :param calculate_digests:
        :return:
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
        Verify an exact subset, every Replica, or one healthy copy.


        :param digital_asset_id:
        :param replica_ids:
        :param stop_after_first_healthy:
        :param all_replicas:
        :return:
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
        Coordinate Store deletion with record removal or tombstoning.


        :param replica_id:
        :param delete_bytes:
        :param retain_tombstone:
        :return:
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
        Forget one Replica claim after optional absence confirmation.


        :param replica_id:
        :param require_bytes_absent:
        :param if_revision:
        :return:
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
