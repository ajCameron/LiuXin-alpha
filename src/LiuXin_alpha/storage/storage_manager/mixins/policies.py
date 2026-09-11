"""
Implement policy registration, Asset assignment, effective resolution, and planning.

Mutation uses shared metadata transactions with explicit candidate restoration around
recreation validation. Assessment observes current state/status/size; planning uses
recorded/configuration evidence and returns proposals without physical execution.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Iterator
from typing import override

import LiuXin_alpha.storage.api as api
from LiuXin_alpha.storage.storage_manager.mixins._state import _StorageManagerState


class StoragePolicyMixin(_StorageManagerState):
    """
    Manage registered policies and derive Asset assessments and maintenance proposals from shared
    hooks.

    Policy mutations use the manager lock and metadata transaction. Candidate updates are installed
    while recreation dependencies are validated, with explicit restoration limited to validation
    failures. Assessment observes Store status/size through helpers; planning primarily uses
    recorded VERIFIED claims and configuration rules, so the two can disagree about current health.

    Example:
        >>> plan = manager.plan_replication(asset_id)  # doctest: +SKIP
    """

    @override
    def create_replication_policy(
        self,
        policy: api.ReplicationPolicy,
    ) -> api.ReplicationPolicyRecord:
        """
        Allocate a new replication policy ID and revision, then store the supplied definition under
        the metadata lock/transaction.

        Equivalent definitions are not deduplicated. The record constructor adds no definition-type
        or content validation, and creation does not assign the policy to Assets or run global
        recreation validation.

        Example:
            >>> record = manager.create_replication_policy(policy)  # doctest: +SKIP


        :param policy: Definition retained by the new record without copying.
        :return: New registered policy record after allocation and mapping assignment succeed.
        """

        with self._lock, self._metadata_transaction():
            policy_id = api.ReplicationPolicyID(
                self._allocate_metadata_id_locked("replication_policy")
            )
            record = api.ReplicationPolicyRecord(
                policy_id,
                policy,
                self._new_revision_locked(),
            )
            self._replication_policies[policy_id] = record
            return record

    @override
    def get_replication_policy_record(
        self,
        replication_policy_id: api.ReplicationPolicyID,
    ) -> api.ReplicationPolicyRecord:
        """
        Look up the exact replication policy key under the lock.

        A mapping KeyError becomes StorageManagementError with the original error chained. Other
        repository failures propagate, and manager defaults are not consulted.

        Example:
            >>> record = manager.get_replication_policy_record(policy_id)  # doctest: +SKIP


        :param replication_policy_id: Exact registered policy identity to resolve.
        :return: Retained policy record for the requested key.
        """

        with self._lock:
            try:
                return self._replication_policies[replication_policy_id]
            except KeyError as error:
                raise api.StorageManagementError(
                    f"Replication policy {replication_policy_id} is not registered."
                ) from error

    @override
    def update_replication_policy(
        self,
        replication_policy_id: api.ReplicationPolicyID,
        policy: api.ReplicationPolicy,
        *,
        if_revision: str | None = None,
    ) -> api.ReplicationPolicyRecord:
        """
        Replace a registered replication definition after checking its optional revision and all
        effective recreation dependencies.

        Under the metadata lock/transaction, install a candidate with the old revision so validation
        sees the proposed definition. A BaseException from recreation validation triggers
        restoration of the old mapping value before re-raising. After validation, allocate a new
        revision and assign the final record.

        Revision allocation, final assignment, and transaction-exit failures lie outside that
        explicit restoration block; persistence behavior then belongs to the transaction
        implementation. Missing identities raise StorageManagementError before candidate
        installation.

        Example:
            >>> updated = manager.update_replication_policy(policy_id, policy, if_revision=record.revision)  # doctest: +SKIP


        :param replication_policy_id: Registered identity retained by the replacement.
        :param policy: Complete replacement definition visible during recreation validation.
        :param if_revision: Expected current revision, or None to omit the optimistic precondition.
        :return: Updated record with a new revision after validation and persistence succeed.
        """

        with self._lock, self._metadata_transaction():
            current = self._replication_policies.get(replication_policy_id)
            if current is None:
                raise api.StorageManagementError(
                    f"Replication policy {replication_policy_id} is not registered."
                )
            self._check_revision(current.revision, if_revision)
            candidate = api.ReplicationPolicyRecord(
                replication_policy_id,
                policy,
                current.revision,
            )
            self._replication_policies[replication_policy_id] = candidate
            try:
                self._validate_all_recreation_policies()
            except BaseException:
                self._replication_policies[replication_policy_id] = current
                raise
            record = dataclasses.replace(
                candidate,
                revision=self._new_revision_locked(),
            )
            self._replication_policies[replication_policy_id] = record
            return record

    @override
    def delete_replication_policy(
        self,
        replication_policy_id: api.ReplicationPolicyID,
    ) -> bool:
        """
        Remove a replication definition only when no Asset assignment or Store default references
        its ID.

        Absence returns False before reference scanning. Checks and deletion use the lock/metadata
        transaction, without a revision precondition, cascading reassignment, or byte changes.
        Reference conflicts raise StoragePreconditionFailed; other repository failures propagate.

        Example:
            >>> removed = manager.delete_replication_policy(policy_id)  # doctest: +SKIP


        :param replication_policy_id: Policy identity to remove after checking Asset and Store references.
        :return: True after deletion, False for an absent identity.
        """

        with self._lock, self._metadata_transaction():
            if replication_policy_id not in self._replication_policies:
                return False
            if any(
                record.replication_policy_id == replication_policy_id
                for record in self._assets.values()
            ) or any(
                configuration.store_default_replication_policy_id
                == replication_policy_id
                for configuration in self._store_configurations.values()
            ):
                raise api.StoragePreconditionFailed(
                    "replication policy is still assigned."
                )
            del self._replication_policies[replication_policy_id]
            return True

    @override
    def iter_replication_policy_records(
        self,
    ) -> Iterator[api.ReplicationPolicyRecord]:
        """
        Capture replication records under the lock in ascending mapping-key order.

        The returned iterator holds a tuple of record references. Later mapping updates do not
        change that sequence, but nested values are not deep copies.

        Example:
            >>> records = tuple(manager.iter_replication_policy_records())  # doctest: +SKIP


        :return: Iterator over the captured ordered policy records.
        """

        with self._lock:
            records = tuple(
                self._replication_policies[key]
                for key in sorted(self._replication_policies)
            )
        return iter(records)

    @override
    def create_backup_policy(
        self,
        policy: api.BackupPolicy,
    ) -> api.BackupPolicyRecord:
        """
        Allocate a new backup/archive policy ID and revision, then store the supplied definition
        under the metadata lock/transaction.

        Equivalent definitions are not deduplicated. The record constructor adds no definition-type
        or content validation, and creation does not assign the policy to Assets or run global
        recreation validation.

        Example:
            >>> record = manager.create_backup_policy(policy)  # doctest: +SKIP


        :param policy: Definition retained by the new record without copying.
        :return: New registered policy record after allocation and mapping assignment succeed.
        """

        with self._lock, self._metadata_transaction():
            policy_id = api.BackupPolicyID(
                self._allocate_metadata_id_locked("backup_policy")
            )
            record = api.BackupPolicyRecord(
                policy_id,
                policy,
                self._new_revision_locked(),
            )
            self._backup_policies[policy_id] = record
            return record

    @override
    def get_backup_policy_record(
        self,
        backup_policy_id: api.BackupPolicyID,
    ) -> api.BackupPolicyRecord:
        """
        Look up the exact backup/archive policy key under the lock.

        A mapping KeyError becomes StorageManagementError with the original error chained. Other
        repository failures propagate, and manager defaults are not consulted.

        Example:
            >>> record = manager.get_backup_policy_record(policy_id)  # doctest: +SKIP


        :param backup_policy_id: Exact registered policy identity to resolve.
        :return: Retained policy record for the requested key.
        """

        with self._lock:
            try:
                return self._backup_policies[backup_policy_id]
            except KeyError as error:
                raise api.StorageManagementError(
                    f"Backup policy {backup_policy_id} is not registered."
                ) from error

    @override
    def update_backup_policy(
        self,
        backup_policy_id: api.BackupPolicyID,
        policy: api.BackupPolicy,
        *,
        if_revision: str | None = None,
    ) -> api.BackupPolicyRecord:
        """
        Replace a registered backup/archive definition after checking its optional revision and all
        effective recreation dependencies.

        Under the metadata lock/transaction, install a candidate with the old revision so validation
        sees the proposed definition. A BaseException from recreation validation triggers
        restoration of the old mapping value before re-raising. After validation, allocate a new
        revision and assign the final record.

        Revision allocation, final assignment, and transaction-exit failures lie outside that
        explicit restoration block; persistence behavior then belongs to the transaction
        implementation. Missing identities raise StorageManagementError before candidate
        installation.

        Example:
            >>> updated = manager.update_backup_policy(policy_id, policy, if_revision=record.revision)  # doctest: +SKIP


        :param backup_policy_id: Registered identity retained by the replacement.
        :param policy: Complete replacement definition visible during recreation validation.
        :param if_revision: Expected current revision, or None to omit the optimistic precondition.
        :return: Updated record with a new revision after validation and persistence succeed.
        """

        with self._lock, self._metadata_transaction():
            current = self._backup_policies.get(backup_policy_id)
            if current is None:
                raise api.StorageManagementError(
                    f"Backup policy {backup_policy_id} is not registered."
                )
            self._check_revision(current.revision, if_revision)
            candidate = api.BackupPolicyRecord(
                backup_policy_id,
                policy,
                current.revision,
            )
            self._backup_policies[backup_policy_id] = candidate
            try:
                self._validate_all_recreation_policies()
            except BaseException:
                self._backup_policies[backup_policy_id] = current
                raise
            record = dataclasses.replace(
                candidate,
                revision=self._new_revision_locked(),
            )
            self._backup_policies[backup_policy_id] = record
            return record

    @override
    def delete_backup_policy(
        self,
        backup_policy_id: api.BackupPolicyID,
    ) -> bool:
        """
        Remove a backup/archive definition only when no Asset assignment or Store default references
        its ID.

        Absence returns False before reference scanning. Checks and deletion use the lock/metadata
        transaction, without a revision precondition, cascading reassignment, or byte changes.
        Reference conflicts raise StoragePreconditionFailed; other repository failures propagate.

        Example:
            >>> removed = manager.delete_backup_policy(policy_id)  # doctest: +SKIP


        :param backup_policy_id: Policy identity to remove after checking Asset and Store references.
        :return: True after deletion, False for an absent identity.
        """

        with self._lock, self._metadata_transaction():
            if backup_policy_id not in self._backup_policies:
                return False
            if any(
                record.backup_policy_id == backup_policy_id
                for record in self._assets.values()
            ) or any(
                configuration.store_default_backup_policy_id == backup_policy_id
                for configuration in self._store_configurations.values()
            ):
                raise api.StoragePreconditionFailed("backup policy is still assigned.")
            del self._backup_policies[backup_policy_id]
            return True

    @override
    def iter_backup_policy_records(self) -> Iterator[api.BackupPolicyRecord]:
        """
        Capture backup/archive records under the lock in ascending mapping-key order.

        The returned iterator holds a tuple of record references. Later mapping updates do not
        change that sequence, but nested values are not deep copies.

        Example:
            >>> records = tuple(manager.iter_backup_policy_records())  # doctest: +SKIP


        :return: Iterator over the captured ordered policy records.
        """

        with self._lock:
            records = tuple(
                self._backup_policies[key] for key in sorted(self._backup_policies)
            )
        return iter(records)

    @override
    def set_digital_asset_policies(
        self,
        digital_asset_id: api.DigitalAssetID,
        *,
        replication_policy_id: api.ReplicationPolicyID | None = None,
        backup_policy_id: api.BackupPolicyID | None = None,
        if_revision: str | None = None,
    ) -> api.DigitalAssetRecord:
        """
        Validate supplied policy IDs, then replace both Asset references after optional revision
        checking.

        None clears the corresponding explicit assignment; it does not preserve the old reference.
        Policy lookup precedes the locked Asset lookup. The candidate retains the old revision while
        global recreation validation runs, and a validation BaseException restores the original
        Asset record before re-raising.

        On successful validation a new revision is allocated and the final record stored. Later
        allocation/assignment/transaction failures are outside the explicit restoration block. Byte
        identity and descriptive metadata are retained, and no Replica mutation is performed.

        Example:
            >>> asset = manager.set_digital_asset_policies(asset_id, backup_policy_id=backup_id, if_revision=asset.revision)  # doctest: +SKIP


        :param digital_asset_id: Registered atomic Asset identity to inspect or update.
        :param replication_policy_id: Registered replication ID, or None to clear the explicit reference.
        :param backup_policy_id: Registered backup ID, or None to clear the explicit reference.
        :param if_revision: Expected current revision, or None to omit the optimistic precondition.
        :return: Updated Asset record with both requested policy references and a new revision.
        """

        self._validate_declared_policy_ids(
            replication_policy_id,
            backup_policy_id,
        )
        with self._lock, self._metadata_transaction():
            current = self._require_asset_locked(digital_asset_id)
            self._check_revision(current.revision, if_revision)
            candidate = dataclasses.replace(
                current,
                replication_policy_id=replication_policy_id,
                backup_policy_id=backup_policy_id,
                revision=current.revision,
            )
            self._assets[digital_asset_id] = candidate
            try:
                self._validate_all_recreation_policies()
            except BaseException:
                self._assets[digital_asset_id] = current
                raise
            updated = dataclasses.replace(
                candidate,
                revision=self._new_revision_locked(),
            )
            self._assets[digital_asset_id] = updated
            return updated

    @override
    def resolve_effective_policies(
        self,
        digital_asset_id: api.DigitalAssetID,
    ) -> api.ResolvedStoragePolicies:
        """
        Read the Asset and each explicit policy record, falling back independently to manager
        defaults.

        Origins are labelled digital_asset for an explicit reference and manager_default otherwise.
        No Replica or Store-default scan occurs here. Reads are separate, and unknown referenced
        policies or repository failures propagate.

        Example:
            >>> policies = manager.resolve_effective_policies(asset_id)  # doctest: +SKIP


        :param digital_asset_id: Registered atomic Asset identity to inspect or update.
        :return: New pair of effective definitions and reported origin labels.
        """

        asset_record = self.get_digital_asset_record(digital_asset_id)
        replication: api.ReplicationPolicy | None = None
        backup: api.BackupPolicy | None = None
        replication_source = "manager_default"
        backup_source = "manager_default"
        if asset_record.replication_policy_id is not None:
            replication = self.get_replication_policy_record(
                asset_record.replication_policy_id
            ).policy
            replication_source = "digital_asset"
        if asset_record.backup_policy_id is not None:
            backup = self.get_backup_policy_record(asset_record.backup_policy_id).policy
            backup_source = "digital_asset"

        return api.ResolvedStoragePolicies(
            self._default_replication_policy if replication is None else replication,
            self._default_backup_policy if backup is None else backup,
            replication_source,
            backup_source,
        )

    @override
    def assess_replication(
        self,
        digital_asset_id: api.DigitalAssetID,
    ) -> api.StoragePolicyAssessment:
        """
        Resolve effective policies and pass the replication definition to the shared assessment
        helper.

        The helper checks state, current Store availability and stat size, then applies recorded
        VERIFIED state, configuration tags/mode, and per-dimension capacity limits. This wrapper
        adds no hashing, repair, transaction, or error suppression.

        Example:
            >>> assessment = manager.assess_replication(asset_id)  # doctest: +SKIP


        :param digital_asset_id: Registered atomic Asset identity to inspect or update.
        :return: Assessment returned by _assess_policy for the selected definition.
        """

        policy = self.resolve_effective_policies(digital_asset_id).replication
        return self._assess_policy(digital_asset_id, policy)

    @override
    def assess_backup(
        self,
        digital_asset_id: api.DigitalAssetID,
    ) -> api.StoragePolicyAssessment:
        """
        Resolve effective policies and pass the backup definition to the shared assessment helper.

        The helper checks state, current Store availability and stat size, then applies recorded
        VERIFIED state, configuration tags/mode, and per-dimension capacity limits. This wrapper
        adds no hashing, repair, transaction, or error suppression.

        Example:
            >>> assessment = manager.assess_backup(asset_id)  # doctest: +SKIP


        :param digital_asset_id: Registered atomic Asset identity to inspect or update.
        :return: Assessment returned by _assess_policy for the selected definition.
        """

        policy = self.resolve_effective_policies(digital_asset_id).backup
        return self._assess_policy(digital_asset_id, policy)

    @override
    def assess_digital_asset(
        self,
        digital_asset_id: api.DigitalAssetID,
    ) -> api.DigitalAssetStorageAssessment:
        """
        Collect currently readable claims across modes, recoverable exact derivations, and both
        policy assessments.

        The Asset is required first. Shared readability checks use state/status/size, while
        recursive derivation checks evaluate current managed prerequisites or resolver-reported
        artifacts. Exact route IDs are retained in derivation iteration order; no recipe executes.

        Replication and backup assessments are collected afterwards through separate lookups. This
        is not a single physical or repository snapshot, and unsuppressed helper failures propagate.

        Example:
            >>> assessment = manager.assess_digital_asset(asset_id)  # doctest: +SKIP


        :param digital_asset_id: Registered atomic Asset identity to inspect or update.
        :return: Aggregate of reported readable claims, current exact routes, and policy threshold results.
        """

        self.get_digital_asset_record(digital_asset_id)
        readable = tuple(
            record.replica_id
            for record in self.iter_replica_records(digital_asset_id=digital_asset_id)
            if self._record_is_readable(record)
        )
        derivations = tuple(
            record.digital_asset_derivation_id
            for record in self.iter_digital_asset_derivation_records(
                result_digital_asset_id=digital_asset_id,
                exact_only=True,
            )
            if self._derivation_is_recoverable(record, {digital_asset_id})
        )
        return api.DigitalAssetStorageAssessment(
            digital_asset_id,
            self.assess_replication(digital_asset_id),
            self.assess_backup(digital_asset_id),
            readable_replica_ids=readable,
            exact_recreation_derivation_ids=derivations,
        )

    @override
    def plan_replication(
        self,
        digital_asset_id: api.DigitalAssetID,
    ) -> api.DigitalAssetReplicationPlan:
        """
        Propose replication work from recorded state, Store configuration eligibility, and declared
        spread limits.

        The planning healthy set contains policy-mode VERIFIED claims passing configuration rules,
        without checking current status, stat size, or digests. Needed copies use the shared
        separated-capacity calculation. Destination planning excludes every Store with a nondeleted
        claim for the Asset, regardless of mode or health.

        A zero target proposes every policy-mode claim for removal, including tombstones; otherwise
        the ID-ordered healthy suffix after target is proposed without solving a separate
        optimal-retention problem. PRESENT, UNVERIFIED, and STAGED claims are proposed for
        verification. With no healthy claim and RECREATE loss action, select the first currently
        recoverable exact derivation, warning if none exists.

        Short destination lists become warnings. The method does not consult auto_heal or retention
        priority, reserve capacity, verify claims, or execute any proposal.

        Example:
            >>> plan = manager.plan_replication(asset_id)  # doctest: +SKIP


        :param digital_asset_id: Registered atomic Asset identity to inspect or update.
        :return: Replication destinations, verification/removal IDs, optional exact route, and shortage diagnostics.
        """

        asset = self.get_digital_asset_record(digital_asset_id)
        policies = self.resolve_effective_policies(digital_asset_id)
        policy = policies.replication
        records = tuple(
            self.iter_replica_records(
                digital_asset_id=digital_asset_id,
                mode=policy.mode,
            )
        )
        healthy = tuple(
            record
            for record in records
            if record.state is api.ReplicaState.VERIFIED
            and self._store_satisfies_policy(record.location.store_ref, policy)
        )
        target = policy.effective_target_copies
        needed = max(0, target - self._separated_copy_capacity(healthy, policy))
        destinations = self._plan_destination_stores(
            policy,
            healthy,
            needed,
            expected_size=asset.size_bytes,
            excluded_store_refs={
                record.location.store_ref
                for record in self.iter_replica_records(
                    digital_asset_id=digital_asset_id
                )
                if record.state is not api.ReplicaState.DELETED
            },
        )
        remove = (
            tuple(record.replica_id for record in records)
            if target == 0
            else tuple(record.replica_id for record in healthy[target:])
        )
        verify_ids = tuple(
            record.replica_id
            for record in records
            if record.state
            in {
                api.ReplicaState.PRESENT,
                api.ReplicaState.UNVERIFIED,
                api.ReplicaState.STAGED,
            }
        )
        recreation_id: api.DigitalAssetDerivationID | None = None
        if not healthy and policy.loss_action is api.DigitalAssetLossAction.RECREATE:
            recreation_id = next(
                (
                    record.digital_asset_derivation_id
                    for record in self.iter_digital_asset_derivation_records(
                        result_digital_asset_id=digital_asset_id,
                        exact_only=True,
                    )
                    if self._derivation_is_recoverable(
                        record,
                        {digital_asset_id},
                    )
                ),
                None,
            )
        warnings: list[str] = []
        if len(destinations) < needed:
            warnings.append(
                f"only {len(destinations)} of {needed} required destinations are available"
            )
        if (
            policy.loss_action is api.DigitalAssetLossAction.RECREATE
            and not healthy
            and recreation_id is None
        ):
            warnings.append("no currently recoverable exact derivation is available")
        return api.DigitalAssetReplicationPlan(
            digital_asset_id,
            destination_store_refs=destinations,
            replica_ids_to_verify=verify_ids,
            replica_ids_to_remove=remove,
            exact_recreation_derivation_id=recreation_id,
            warnings=tuple(warnings),
        )

    @override
    def plan_backup(
        self,
        digital_asset_id: api.DigitalAssetID,
    ) -> api.DigitalAssetBackupPlan:
        """
        Propose backup destinations, sources, verification, and removals using recorded policy-mode
        claims.

        As in replication planning, healthy counts require recorded VERIFIED state and configuration
        eligibility without a current physical check. Destination selection excludes all Stores with
        any nondeleted claim for the Asset. Source IDs come from other modes and must pass the
        current state/status/size readability helper.

        A zero target proposes all policy-mode claims for removal; otherwise the healthy suffix
        after target is proposed. Every policy-mode claim except VERIFIED and DELETED is proposed
        for verification, including unavailable or corrupt observations. Only destination shortages
        produce warnings here; an empty source list adds no warning.

        This planner does not enforce retention_locked, auto_heal, periodic verification, or
        retention priority, and performs no copy, verification, or deletion. Execution must recheck
        its own constraints.

        Example:
            >>> plan = manager.plan_backup(asset_id)  # doctest: +SKIP


        :param digital_asset_id: Registered atomic Asset identity to inspect or update.
        :return: Backup proposal containing destination/source IDs, verification/removal IDs, and destination warnings.
        """

        asset = self.get_digital_asset_record(digital_asset_id)
        policy = self.resolve_effective_policies(digital_asset_id).backup
        records = tuple(
            self.iter_replica_records(
                digital_asset_id=digital_asset_id,
                mode=policy.mode,
            )
        )
        healthy = tuple(
            record
            for record in records
            if record.state is api.ReplicaState.VERIFIED
            and self._store_satisfies_policy(record.location.store_ref, policy)
        )
        target = policy.effective_target_copies
        needed = max(0, target - self._separated_copy_capacity(healthy, policy))
        destinations = self._plan_destination_stores(
            policy,
            healthy,
            needed,
            expected_size=asset.size_bytes,
            excluded_store_refs={
                record.location.store_ref
                for record in self.iter_replica_records(
                    digital_asset_id=digital_asset_id
                )
                if record.state is not api.ReplicaState.DELETED
            },
        )
        sources = tuple(
            record.replica_id
            for record in self.iter_replica_records(digital_asset_id=digital_asset_id)
            if record.mode is not policy.mode and self._record_is_readable(record)
        )
        remove = (
            tuple(record.replica_id for record in records)
            if target == 0
            else tuple(record.replica_id for record in healthy[target:])
        )
        warnings = (
            (
                f"only {len(destinations)} of {needed} required destinations are available",
            )
            if len(destinations) < needed
            else ()
        )
        return api.DigitalAssetBackupPlan(
            digital_asset_id,
            destination_store_refs=destinations,
            source_replica_ids=sources,
            replica_ids_to_verify=tuple(
                record.replica_id
                for record in records
                if record.state is not api.ReplicaState.VERIFIED
                and record.state is not api.ReplicaState.DELETED
            ),
            replica_ids_to_remove=remove,
            warnings=warnings,
        )


__all__ = ["StoragePolicyMixin"]
