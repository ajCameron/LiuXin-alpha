"""
Store observation reconciliation planning and application.
"""

from __future__ import annotations

import dataclasses
from datetime import UTC, datetime
from typing import override
from uuid import uuid4

import LiuXin_alpha.storage.api as api
from LiuXin_alpha.storage.storage_manager.mixins._state import _StorageManagerState


class StorageReconciliationMixin(_StorageManagerState):
    """
    Reconcile claimed Replicas with a Store's observed inventory.

    Planning is read-only and accounts for each Store's enumeration and digest
    guarantees.  Applying a plan changes only guarded Replica observations;
    revision and Store-generation checks prevent a stale plan from overwriting
    newer metadata or a reloaded Store instance.
    """

    @override
    def plan_reconciliation(
        self,
        store_ref: api.StoreUUID,
        *,
        verify_digests: bool = False,
    ) -> api.StoreReconciliationPlan:
        """
        Compare Replica claims with Store inventory without mutation.


        :param store_ref:
        :param verify_digests:
        :return:
        """

        store = self.get_store(store_ref)
        enumeration = store.capabilities.enumeration
        expected = tuple(
            record
            for record in self.iter_replica_records(store_ref=store_ref)
            if record.state is not api.ReplicaState.DELETED
        )
        inventory: set[api.Location] = set()
        warnings: list[str] = []
        errors: list[str] = []
        if enumeration is api.EnumerationCompleteness.UNAVAILABLE:
            warnings.append(
                "Store cannot enumerate inventory; claims were checked individually."
            )
        else:
            try:
                inventory.update(store.iter_locations())
            except api.StorageError as error:
                enumeration = api.EnumerationCompleteness.UNAVAILABLE
                errors.append(f"inventory enumeration failed: {error}")

        missing: list[api.ReplicaID] = []
        corrupt: list[api.ReplicaID] = []
        unavailable: list[api.ReplicaID] = []
        matched = 0
        for record in expected:
            if (
                enumeration is api.EnumerationCompleteness.COMPLETE
                and record.location not in inventory
            ):
                missing.append(record.replica_id)
                continue
            report = self._inspect_replica(
                record,
                self.get_digital_asset_record(record.digital_asset_id),
                calculate_digests=verify_digests,
            )
            if report.exists is False:
                missing.append(record.replica_id)
            elif report.exists is None:
                unavailable.append(record.replica_id)
                errors.extend(report.errors)
            elif report.state is api.ReplicaState.CORRUPT:
                corrupt.append(record.replica_id)
                inventory.add(record.location)
            else:
                matched += 1
                inventory.add(record.location)

        expected_locations = {record.location for record in expected}
        unexpected = tuple(
            sorted(
                inventory - expected_locations,
                key=lambda location: location.key,
            )
        )
        with self._lock:
            repository_revision = str(self._replica_generation)
        return api.StoreReconciliationPlan(
            uuid4(),
            store_ref,
            verify_digests,
            enumeration,
            expected_replicas=len(expected),
            observed_locations=len(inventory),
            matched_replicas=matched,
            missing_replica_ids=tuple(missing),
            unexpected_locations=unexpected,
            corrupt_replica_ids=tuple(corrupt),
            unavailable_replica_ids=tuple(unavailable),
            repository_revision=repository_revision,
            warnings=tuple(warnings),
            errors=tuple(errors),
        )

    @override
    def apply_reconciliation(
        self,
        plan: api.StoreReconciliationPlan,
    ) -> api.StoreReconciliationReport:
        """
        Apply one current plan to Replica observations only.


        :param plan:
        :return:
        """

        with self._lock, self._metadata_transaction():
            if plan.repository_revision != str(self._replica_generation):
                raise api.StoreReconciliationPlanStale(
                    "Replica repository changed after reconciliation planning."
                )
            classifications = (
                (plan.missing_replica_ids, api.ReplicaState.MISSING),
                (plan.corrupt_replica_ids, api.ReplicaState.CORRUPT),
                (plan.unavailable_replica_ids, api.ReplicaState.UNAVAILABLE),
            )
            updated: list[api.ReplicaID] = []
            for replica_ids, state in classifications:
                for replica_id in replica_ids:
                    record = self._require_replica_locked(replica_id)
                    if record.location.store_ref != plan.store_ref:
                        raise api.StoreReconciliationPlanStale(
                            "reconciliation plan contains a Replica from another Store."
                        )
                    self._replicas[replica_id] = dataclasses.replace(
                        record,
                        observation=api.ReplicaObservation(
                            state,
                            checked_at=datetime.now(UTC),
                            failure_reason=(
                                "reconciliation observed missing bytes"
                                if state is api.ReplicaState.MISSING
                                else "reconciliation could not confirm healthy bytes"
                            ),
                        ),
                        revision=self._new_revision_locked(),
                    )
                    updated.append(replica_id)
            if updated:
                self._replica_generation += 1
        return api.StoreReconciliationReport(
            plan,
            applied=True,
            updated_replica_ids=tuple(updated),
        )


__all__ = ["StorageReconciliationMixin"]
