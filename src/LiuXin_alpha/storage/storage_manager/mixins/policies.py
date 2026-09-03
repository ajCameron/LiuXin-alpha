"""
Replication and backup policy management and planning.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Iterator
from typing import override

import LiuXin_alpha.storage.api as api
from LiuXin_alpha.storage.storage_manager.mixins._state import _StorageManagerState


class StoragePolicyMixin(_StorageManagerState):
    """
    Register, resolve, assess, and plan storage policies.

    Replication policies describe live-copy placement; backup policies describe
    recoverable archive placement.  Assessment observes current metadata and
    Store capabilities, while planning returns proposed actions without
    publishing bytes or mutating Replica records.
    """

    @override
    def create_replication_policy(
        self,
        policy: api.ReplicationPolicy,
    ) -> api.ReplicationPolicyRecord:
        """
        Register one replication policy with stable manager identity.


        :param policy:
        :return:
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
        Return one registered replication policy.


        :param replication_policy_id:
        :return:
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
        Replace a policy without invalidating recreation guarantees.


        :param replication_policy_id:
        :param policy:
        :param if_revision:
        :return:
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
        Delete an unreferenced replication policy.


        :param replication_policy_id:
        :return:
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
        Iterate over a stable snapshot of replication policies.


        :return:
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
        Register one backup policy with stable manager identity.


        :param policy:
        :return:
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
        Return one registered backup policy.


        :param backup_policy_id:
        :return:
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
        Replace a policy without invalidating recreation guarantees.


        :param backup_policy_id:
        :param policy:
        :param if_revision:
        :return:
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
        Delete an unreferenced backup policy.


        :param backup_policy_id:
        :return:
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
        Iterate over a stable snapshot of backup policies.


        :return:
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
        Assign explicit policies after validating references and recreation.


        :param digital_asset_id:
        :param replication_policy_id:
        :param backup_policy_id:
        :param if_revision:
        :return:
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
        Resolve captured Asset policy, then manager-default policy.


        :param digital_asset_id:
        :return:
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
        Assess live Replicas against the effective replication policy.


        :param digital_asset_id:
        :return:
        """

        policy = self.resolve_effective_policies(digital_asset_id).replication
        return self._assess_policy(digital_asset_id, policy)

    @override
    def assess_backup(
        self,
        digital_asset_id: api.DigitalAssetID,
    ) -> api.StoragePolicyAssessment:
        """
        Assess backup Replicas against the effective backup policy.


        :param digital_asset_id:
        :return:
        """

        policy = self.resolve_effective_policies(digital_asset_id).backup
        return self._assess_policy(digital_asset_id, policy)

    @override
    def assess_digital_asset(
        self,
        digital_asset_id: api.DigitalAssetID,
    ) -> api.DigitalAssetStorageAssessment:
        """
        Combine readability, policy satisfaction, and exact recreation.


        :param digital_asset_id:
        :return:
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
        Plan verification, placement, removal, or exact recreation.


        :param digital_asset_id:
        :return:
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
        Plan backup placement, verification, and surplus removal.


        :param digital_asset_id:
        :return:
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
