"""
Storage policy persistence, resolution, assessment, and planning facade.
"""

import abc

from collections.abc import Iterator

from LiuXin_alpha.storage.api.storage_manager_api.models import (
    DigitalAssetBackupPlan, BackupPolicy, BackupPolicyID, BackupPolicyRecord,
    DigitalAssetID, DigitalAssetRecord, DigitalAssetStorageAssessment,
    DigitalAssetReplicationPlan, ReplicationPolicy, ReplicationPolicyID,
    ReplicationPolicyRecord, ResolvedStoragePolicies,
    StoragePolicyAssessment,
)


class StoragePolicyAPI(abc.ABC):
    """
    Persistence, resolution, assessment, and planning for storage policy.

    The planning methods report intended work without executing replication or
    backup mutations.

    Example:
        >>> def needs_replication(
        ...     manager: StoragePolicyAPI, asset_id: DigitalAssetID,
        ... ) -> bool:
        ...     return not manager.assess_replication(asset_id).meets_target
    """

    @abc.abstractmethod
    def create_replication_policy(
        self,
        policy: ReplicationPolicy,
    ) -> ReplicationPolicyRecord:
        """
        Persist a new live-replication policy.

        Example:
            >>> record = manager.create_replication_policy(  # doctest: +SKIP
            ...     ReplicationPolicy(name="durable", min_copies=2),
            ... )


        :param policy:
        :return:
        """
        ...

    @abc.abstractmethod
    def get_replication_policy_record(
        self,
        replication_policy_id: ReplicationPolicyID,
    ) -> ReplicationPolicyRecord:
        """
        Return a persisted replication policy by identifier.

        Example:
            >>> record = manager.get_replication_policy_record(4)  # doctest: +SKIP


        :param replication_policy_id:
        :return:
        """
        ...

    @abc.abstractmethod
    def update_replication_policy(
        self,
        replication_policy_id: ReplicationPolicyID,
        policy: ReplicationPolicy,
        *,
        if_revision: str | None = None,
    ) -> ReplicationPolicyRecord:
        """
        Replace the definition of a persisted replication policy.

        Example:
            >>> record = manager.update_replication_policy(  # doctest: +SKIP
            ...     4, ReplicationPolicy(min_copies=2),
            ... )


        :param replication_policy_id:
        :param policy:
        :param if_revision:
        :return:
        """
        ...

    @abc.abstractmethod
    def delete_replication_policy(
        self,
        replication_policy_id: ReplicationPolicyID,
    ) -> bool:
        """
        Delete a replication policy and report whether it existed.

        Example:
            >>> deleted = manager.delete_replication_policy(4)  # doctest: +SKIP


        :param replication_policy_id:
        :return:
        """
        ...

    @abc.abstractmethod
    def iter_replication_policy_records(
        self,
    ) -> Iterator[ReplicationPolicyRecord]:
        """
        Iterate over persisted replication policies.

        Example:
            >>> records = list(  # doctest: +SKIP
            ...     manager.iter_replication_policy_records(),
            ... )


        :return:
        """
        ...

    @abc.abstractmethod
    def create_backup_policy(self, policy: BackupPolicy) -> BackupPolicyRecord:
        """
        Persist a new backup or archival policy.

        Example:
            >>> record = manager.create_backup_policy(  # doctest: +SKIP
            ...     BackupPolicy(name="offsite"),
            ... )


        :param policy:
        :return:
        """
        ...

    @abc.abstractmethod
    def get_backup_policy_record(
        self,
        backup_policy_id: BackupPolicyID,
    ) -> BackupPolicyRecord:
        """
        Return a persisted backup policy by identifier.

        Example:
            >>> record = manager.get_backup_policy_record(5)  # doctest: +SKIP


        :param backup_policy_id:
        :return:
        """
        ...

    @abc.abstractmethod
    def update_backup_policy(
        self,
        backup_policy_id: BackupPolicyID,
        policy: BackupPolicy,
        *,
        if_revision: str | None = None,
    ) -> BackupPolicyRecord:
        """
        Replace the definition of a persisted backup policy.

        Example:
            >>> record = manager.update_backup_policy(  # doctest: +SKIP
            ...     5, BackupPolicy(target_copies=2),
            ... )


        :param backup_policy_id:
        :param policy:
        :param if_revision:
        :return:
        """
        ...

    @abc.abstractmethod
    def delete_backup_policy(
        self,
        backup_policy_id: BackupPolicyID,
    ) -> bool:
        """
        Delete a backup policy and report whether it existed.

        Example:
            >>> deleted = manager.delete_backup_policy(5)  # doctest: +SKIP


        :param backup_policy_id:
        :return:
        """
        ...

    @abc.abstractmethod
    def iter_backup_policy_records(self) -> Iterator[BackupPolicyRecord]:
        """
        Iterate over persisted backup policies.

        Example:
            >>> records = list(  # doctest: +SKIP
            ...     manager.iter_backup_policy_records(),
            ... )


        :return:
        """
        ...

    @abc.abstractmethod
    def set_digital_asset_policies(
        self, digital_asset_id: DigitalAssetID, *,
        replication_policy_id: ReplicationPolicyID | None = None,
        backup_policy_id: BackupPolicyID | None = None,
        if_revision: str | None = None,
    ) -> DigitalAssetRecord:
        """
        Assign explicit replication and backup policies to an asset.

        A replication policy whose loss action is ``RECREATE`` may only be
        assigned when the Asset has at least one complete exact recipe and all
        of that recipe's sources have effective policy that keeps them
        recoverable. This validation is transitive; a recreation chain must
        terminate in retained bytes rather than a cycle of disposable Assets.

        Example:
            >>> asset_record = manager.set_digital_asset_policies(  # doctest: +SKIP
            ...     7, replication_policy_id=4, backup_policy_id=5,
            ... )


        :param digital_asset_id:
        :param replication_policy_id:
        :param backup_policy_id:
        :param if_revision:
        :return:
        """
        ...

    @abc.abstractmethod
    def resolve_effective_policies(
        self, digital_asset_id: DigitalAssetID,
    ) -> ResolvedStoragePolicies:
        """
        Resolve the policies captured on the Asset, then manager defaults.

        Store defaults are placement-time defaults. Implementations copy their
        identifiers onto a newly declared Asset when placing its first Replica;
        policy resolution therefore never depends on Replica iteration order.

        Example:
            >>> policies = manager.resolve_effective_policies(7)  # doctest: +SKIP


        :param digital_asset_id:
        :return:
        """
        ...

    @abc.abstractmethod
    def assess_replication(
        self,
        digital_asset_id: DigitalAssetID,
    ) -> StoragePolicyAssessment:
        """
        Assess live replicas against the effective replication policy.

        Example:
            >>> assessment = manager.assess_replication(7)  # doctest: +SKIP


        :param digital_asset_id:
        :return:
        """
        ...

    @abc.abstractmethod
    def assess_backup(
        self,
        digital_asset_id: DigitalAssetID,
    ) -> StoragePolicyAssessment:
        """
        Assess backup replicas against the effective backup policy.

        Example:
            >>> assessment = manager.assess_backup(7)  # doctest: +SKIP


        :param digital_asset_id:
        :return:
        """
        ...

    @abc.abstractmethod
    def assess_digital_asset(
        self,
        digital_asset_id: DigitalAssetID,
    ) -> DigitalAssetStorageAssessment:
        """
        Return readability, replayability, and live/backup policy state.

        Exact recreation is reported only when the recipe's pinned inputs and
        executor artefacts are themselves currently recoverable.

        Example:
            >>> assessment = manager.assess_digital_asset(7)  # doctest: +SKIP


        :param digital_asset_id:
        :return:
        """

        ...

    @abc.abstractmethod
    def plan_replication(
        self,
        digital_asset_id: DigitalAssetID,
    ) -> DigitalAssetReplicationPlan:
        """
        Plan work required to satisfy the effective replication policy.

        The plan may select an exact derivation when ``loss_action`` is
        ``RECREATE`` or remove surplus low-priority replicas when the target is
        zero. Planning never executes the transformation or deletion.

        Example:
            >>> plan = manager.plan_replication(7)  # doctest: +SKIP


        :param digital_asset_id:
        :return:
        """
        ...

    @abc.abstractmethod
    def plan_backup(
        self,
        digital_asset_id: DigitalAssetID,
    ) -> DigitalAssetBackupPlan:
        """
        Plan work required to satisfy the effective backup policy.

        A zero-copy policy may plan removal of existing backup Replicas. The
        manager must still honour retention locks and repository race checks.

        Example:
            >>> plan = manager.plan_backup(7)  # doctest: +SKIP


        :param digital_asset_id:
        :return:
        """
        ...


__all__ = ["StoragePolicyAPI"]
