"""
Compare Store inventory with Replica claims and apply selected observation changes.

Planning inspects separately captured inventory and catalogue state. Applying a plan
uses a global Replica-generation token and mutates observations through the supplied
metadata transaction, without repairing bytes or reconciling unexpected locations.
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
    Implement inventory classifications and explicit Replica-observation updates.

    Planning leaves manager observations unchanged while calling Store enumeration/stat/read
    operations. Application checks a global Replica generation and record ownership, without a
    separate Store-generation token or full planning snapshot. Transaction hooks determine whether
    an application failure rolls back earlier updates.

    Example:
        >>> plan = manager.plan_reconciliation(store_ref)  # doctest: +SKIP
        >>> report = manager.apply_reconciliation(plan)  # doctest: +SKIP
    """

    @override
    def plan_reconciliation(
        self,
        store_ref: api.StoreUUID,
        *,
        verify_digests: bool = False,
    ) -> api.StoreReconciliationPlan:
        """
        Capture nondeleted claims, enumerate when supported, and classify each expected Replica.

        With COMPLETE enum enumeration, a location absent from the inventory is missing without stat
        or digest inspection. Other claims are inspected individually for existence, size, and
        optional digests. A StorageError during enumeration retains any partial inventory, records
        an error, and switches to individual checks. Unexpected exceptions and Asset lookup failures
        propagate.

        Inspected present/corrupt locations augment the inventory; originally enumerated locations
        are not removed after a later missing result. Only unavailable inspection diagnostics join
        plan errors. Unexpected locations are sorted by key, and matched claims are counted
        independently of unique locations. Plan count validation can reject inconsistent totals. The
        global Replica generation is captured after these reads, without a lock spanning the
        comparison.

        Example:
            >>> plan = manager.plan_reconciliation(store_ref, verify_digests=True)  # doctest: +SKIP


        :param store_ref: Store resolved before capturing its nondeleted Replica claims and inventory.
        :param verify_digests: Request digest comparison during individual inspection; authoritative SHA-256 stat evidence may avoid reading bytes.
        :return: New plan with a UUID, counts, classifications, late-captured Replica revision, and diagnostics; manager records are not updated.
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
        Check the global Replica generation and replace the plan's adverse observations in order.

        Under the lock and metadata transaction, process missing IDs, then corrupt IDs, then
        unavailable IDs. Each record must exist and belong to plan.store_ref. Replacements receive a
        fresh revision/timestamp and a generic failure reason, discarding prior observation
        evidence. Duplicate or overlapping IDs are processed repeatedly, with later classifications
        winning.

        Any updates advance the generation once after the loops. A later failure can follow earlier
        writes; transient transaction hooks do not roll those back. The method does not check plan
        identity, completeness, Store availability/generation, or fresh bytes. Matched claims and
        unexpected objects receive no action, and an empty current plan reports applied without
        advancing the generation.

        Example:
            >>> report = manager.apply_reconciliation(plan)  # doctest: +SKIP


        :param plan: Caller-supplied classifications whose revision must equal the current global Replica-generation string.
        :return: Applied report with processed IDs in order, including duplicates; stale tokens, wrong Stores, missing Replicas, or adapter failures raise.
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
