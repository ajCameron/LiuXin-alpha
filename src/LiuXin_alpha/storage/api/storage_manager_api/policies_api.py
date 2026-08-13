"""Storage policy persistence, resolution, assessment, and planning facade."""

import abc

from collections.abc import Iterator

from LiuXin_alpha.storage.api.storage_manager_api.models import (
    BackupPlan, BackupPolicy, BackupPolicyID, BackupStatus,
    DigitalAsset, DigitalAssetID, DigitalAssetStorageHealth,
    EffectiveStoragePolicies, ReplicationPlan, ReplicationPolicy,
    ReplicationPolicyID, ReplicationStatus, StoredBackupPolicy,
    StoredReplicationPolicy,
)


class StoragePolicyAPI(abc.ABC):
    """Persistence, resolution, assessment, and planning for storage policy.

    The planning methods report intended work without executing replication or
    backup mutations.

    Example:
        >>> def needs_replication(
        ...     manager: StoragePolicyAPI, asset_id: DigitalAssetID,
        ... ) -> bool:
        ...     return not manager.assess_replication(asset_id).meets_target
    """

    @abc.abstractmethod
    def create_replication_policy(self, policy: ReplicationPolicy) -> StoredReplicationPolicy:
        """Persist a new live-replication policy.

        Example:
            >>> stored = manager.create_replication_policy(  # doctest: +SKIP
            ...     ReplicationPolicy(name="durable", min_copies=2),
            ... )
        """
        ...

    @abc.abstractmethod
    def get_replication_policy(self, policy_id: ReplicationPolicyID) -> StoredReplicationPolicy:
        """Return a persisted replication policy by identifier.

        Example:
            >>> stored = manager.get_replication_policy(4)  # doctest: +SKIP
        """
        ...

    @abc.abstractmethod
    def update_replication_policy(
        self, policy_id: ReplicationPolicyID, policy: ReplicationPolicy,
    ) -> StoredReplicationPolicy:
        """Replace the definition of a persisted replication policy.

        Example:
            >>> stored = manager.update_replication_policy(  # doctest: +SKIP
            ...     4, ReplicationPolicy(min_copies=2),
            ... )
        """
        ...

    @abc.abstractmethod
    def delete_replication_policy(self, policy_id: ReplicationPolicyID) -> bool:
        """Delete a replication policy and report whether it existed.

        Example:
            >>> deleted = manager.delete_replication_policy(4)  # doctest: +SKIP
        """
        ...

    @abc.abstractmethod
    def iter_replication_policies(self) -> Iterator[StoredReplicationPolicy]:
        """Iterate over persisted replication policies.

        Example:
            >>> policies = list(manager.iter_replication_policies())  # doctest: +SKIP
        """
        ...

    @abc.abstractmethod
    def create_backup_policy(self, policy: BackupPolicy) -> StoredBackupPolicy:
        """Persist a new backup or archival policy.

        Example:
            >>> stored = manager.create_backup_policy(  # doctest: +SKIP
            ...     BackupPolicy(name="offsite"),
            ... )
        """
        ...

    @abc.abstractmethod
    def get_backup_policy(self, policy_id: BackupPolicyID) -> StoredBackupPolicy:
        """Return a persisted backup policy by identifier.

        Example:
            >>> stored = manager.get_backup_policy(5)  # doctest: +SKIP
        """
        ...

    @abc.abstractmethod
    def update_backup_policy(
        self, policy_id: BackupPolicyID, policy: BackupPolicy,
    ) -> StoredBackupPolicy:
        """Replace the definition of a persisted backup policy.

        Example:
            >>> stored = manager.update_backup_policy(  # doctest: +SKIP
            ...     5, BackupPolicy(target_copies=2),
            ... )
        """
        ...

    @abc.abstractmethod
    def delete_backup_policy(self, policy_id: BackupPolicyID) -> bool:
        """Delete a backup policy and report whether it existed.

        Example:
            >>> deleted = manager.delete_backup_policy(5)  # doctest: +SKIP
        """
        ...

    @abc.abstractmethod
    def iter_backup_policies(self) -> Iterator[StoredBackupPolicy]:
        """Iterate over persisted backup policies.

        Example:
            >>> policies = list(manager.iter_backup_policies())  # doctest: +SKIP
        """
        ...

    @abc.abstractmethod
    def set_digital_asset_policies(
        self, digital_asset_id: DigitalAssetID, *,
        replication_policy_id: ReplicationPolicyID | None = None,
        backup_policy_id: BackupPolicyID | None = None,
    ) -> DigitalAsset:
        """Assign explicit replication and backup policies to an asset.

        Example:
            >>> asset = manager.set_digital_asset_policies(  # doctest: +SKIP
            ...     7, replication_policy_id=4, backup_policy_id=5,
            ... )
        """
        ...

    @abc.abstractmethod
    def resolve_effective_policies(
        self, digital_asset_id: DigitalAssetID,
    ) -> EffectiveStoragePolicies:
        """Resolve explicit and inherited policy for one Digital Asset.

        Example:
            >>> policies = manager.resolve_effective_policies(7)  # doctest: +SKIP
        """
        ...

    @abc.abstractmethod
    def assess_replication(self, digital_asset_id: DigitalAssetID) -> ReplicationStatus:
        """Assess live replicas against the effective replication policy.

        Example:
            >>> status = manager.assess_replication(7)  # doctest: +SKIP
        """
        ...

    @abc.abstractmethod
    def assess_backup(self, digital_asset_id: DigitalAssetID) -> BackupStatus:
        """Assess backup replicas against the effective backup policy.

        Example:
            >>> status = manager.assess_backup(7)  # doctest: +SKIP
        """
        ...

    @abc.abstractmethod
    def assess_digital_asset(self, digital_asset_id: DigitalAssetID) -> DigitalAssetStorageHealth:
        """Return readability and live/backup policy state for one Asset.

        Example:
            >>> health = manager.assess_digital_asset(7)  # doctest: +SKIP
        """

        ...

    @abc.abstractmethod
    def plan_replication(self, digital_asset_id: DigitalAssetID) -> ReplicationPlan:
        """Plan work required to satisfy the effective replication policy.

        Example:
            >>> plan = manager.plan_replication(7)  # doctest: +SKIP
        """
        ...

    @abc.abstractmethod
    def plan_backup(self, digital_asset_id: DigitalAssetID) -> BackupPlan:
        """Plan work required to satisfy the effective backup policy.

        Example:
            >>> plan = manager.plan_backup(7)  # doctest: +SKIP
        """
        ...


__all__ = ["StoragePolicyAPI"]
