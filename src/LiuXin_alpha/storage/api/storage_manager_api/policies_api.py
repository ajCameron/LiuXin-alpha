"""
Define policy registration, assignment, resolution, observation, and maintenance proposals.

Definitions express intent, assessments report selected evidence, and plans describe
work without executing it. Persistence and physical execution remain implementation
responsibilities with separate failure boundaries.
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


# Todo: Need a method to get the currently active replication and backup policy
# Todo: This is not feeling complete - it can't answer questions such as "currently set policy" e.t.c

class StoragePolicyAPI(abc.ABC):
    """
    Define policy registration, Asset assignment, effective resolution, assessment, and planning.

    Implementations own persistence and observation details. Assessment returns evidence and
    threshold results; planning proposes work without publishing, verifying, recreating, or deleting
    bytes. Execution must apply its own current retention and concurrency checks.

    Example:
        >>> assessment = manager.assess_replication(asset_id)  # doctest: +SKIP
    """

    # Todo: Again, we need a convenience method for setting this replication policy
    # Todo: How do we get what replication policy is currently live?
    @abc.abstractmethod
    def create_replication_policy(
        self,
        policy: ReplicationPolicy,
    ) -> ReplicationPolicyRecord:
        """
        Register a new replication policy definition without changing existing Asset assignments or
        creating copies.

        Example:
            >>> record = manager.create_replication_policy(policy)  # doctest: +SKIP


        :param policy: Complete replication definition to register.
        :return: New registered definition with its assigned policy identity and revision.
        """
        ...

    @abc.abstractmethod
    def get_replication_policy_record(
        self,
        replication_policy_id: ReplicationPolicyID,
    ) -> ReplicationPolicyRecord:
        """
        Resolve one registered replication definition by identity. Unknown identities and repository
        failures remain errors rather than default-policy selection.

        Example:
            >>> record = manager.get_replication_policy_record(policy_id)  # doctest: +SKIP


        :param replication_policy_id: Registered policy identity to resolve.
        :return: Policy record retained for the requested identity.
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
        Replace a registered replication definition under an optional revision precondition.

        The implementation validates recreation-policy dependencies before accepting the change.
        Replacing a definition affects Assets that refer to it without copying new policy IDs onto
        them, and does not execute resulting maintenance work.

        Example:
            >>> updated = manager.update_replication_policy(policy_id, policy, if_revision=record.revision)  # doctest: +SKIP


        :param replication_policy_id: Existing policy identity to retain.
        :param policy: Complete replacement definition.
        :param if_revision: Expected current revision, or None to omit the optimistic precondition.
        :return: Updated policy record with its resulting revision; stale or invalid dependent state can raise.
        """
        ...

    @abc.abstractmethod
    def delete_replication_policy(
        self,
        replication_policy_id: ReplicationPolicyID,
    ) -> bool:
        """
        Delete an unassigned replication definition without changing stored bytes.

        Asset assignments and Store default references can prevent deletion. This signature provides
        no optimistic revision argument or cascading reassignment.

        Example:
            >>> removed = manager.delete_replication_policy(policy_id)  # doctest: +SKIP


        :param replication_policy_id: Policy identity to remove if it is unreferenced.
        :return: True when removed, False if absent; protected references and repository errors can raise.
        """
        ...

    @abc.abstractmethod
    def iter_replication_policy_records(
        self,
    ) -> Iterator[ReplicationPolicyRecord]:
        """
        Iterate registered replication definitions without assessing Assets or discovering Store
        bytes. Ordering and snapshot details belong to the implementation.

        Example:
            >>> records = tuple(manager.iter_replication_policy_records())  # doctest: +SKIP


        :return: Iterator of registered policy records.
        """
        ...

    @abc.abstractmethod
    def create_backup_policy(self, policy: BackupPolicy) -> BackupPolicyRecord:
        """
        Register a new backup/archive policy definition without changing existing Asset assignments
        or creating copies.

        Example:
            >>> record = manager.create_backup_policy(policy)  # doctest: +SKIP


        :param policy: Complete backup/archive definition to register.
        :return: New registered definition with its assigned policy identity and revision.
        """
        ...

    @abc.abstractmethod
    def get_backup_policy_record(
        self,
        backup_policy_id: BackupPolicyID,
    ) -> BackupPolicyRecord:
        """
        Resolve one registered backup/archive definition by identity. Unknown identities and
        repository failures remain errors rather than default-policy selection.

        Example:
            >>> record = manager.get_backup_policy_record(policy_id)  # doctest: +SKIP


        :param backup_policy_id: Registered policy identity to resolve.
        :return: Policy record retained for the requested identity.
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
        Replace a registered backup/archive definition under an optional revision precondition.

        The implementation validates recreation-policy dependencies before accepting the change.
        Replacing a definition affects Assets that refer to it without copying new policy IDs onto
        them, and does not execute resulting maintenance work.

        Example:
            >>> updated = manager.update_backup_policy(policy_id, policy, if_revision=record.revision)  # doctest: +SKIP


        :param backup_policy_id: Existing policy identity to retain.
        :param policy: Complete replacement definition.
        :param if_revision: Expected current revision, or None to omit the optimistic precondition.
        :return: Updated policy record with its resulting revision; stale or invalid dependent state can raise.
        """
        ...

    @abc.abstractmethod
    def delete_backup_policy(
        self,
        backup_policy_id: BackupPolicyID,
    ) -> bool:
        """
        Delete an unassigned backup/archive definition without changing stored bytes.

        Asset assignments and Store default references can prevent deletion. This signature provides
        no optimistic revision argument or cascading reassignment.

        Example:
            >>> removed = manager.delete_backup_policy(policy_id)  # doctest: +SKIP


        :param backup_policy_id: Policy identity to remove if it is unreferenced.
        :return: True when removed, False if absent; protected references and repository errors can raise.
        """
        ...

    @abc.abstractmethod
    def iter_backup_policy_records(self) -> Iterator[BackupPolicyRecord]:
        """
        Iterate registered backup/archive definitions without assessing Assets or discovering Store
        bytes. Ordering and snapshot details belong to the implementation.

        Example:
            >>> records = tuple(manager.iter_backup_policy_records())  # doctest: +SKIP


        :return: Iterator of registered policy records.
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
        Replace both explicit policy references on an Asset, using None to clear a reference.

        Omitting a parameter therefore requests manager-default resolution for that policy, rather
        than preserving the old assignment. Referenced policies must exist, and RECREATE assignments
        require an exact recipe whose pinned prerequisites have retaining or recursively recreating
        policies. This policy feasibility check is distinct from proof that every prerequisite is
        readable now.

        Example:
            >>> asset = manager.set_digital_asset_policies(asset_id, replication_policy_id=policy_id, if_revision=asset.revision)  # doctest: +SKIP


        :param digital_asset_id: Registered atomic Asset identity to inspect or update.
        :param replication_policy_id: Explicit replication definition to assign, or None to clear it.
        :param backup_policy_id: Explicit backup definition to assign, or None to clear it.
        :param if_revision: Expected current revision, or None to omit the optimistic precondition.
        :return: Asset record with the replacement policy references and resulting revision.
        """
        ...

    @abc.abstractmethod
    def resolve_effective_policies(
        self, digital_asset_id: DigitalAssetID,
    ) -> ResolvedStoragePolicies:
        """
        Resolve each explicit Asset policy reference, otherwise use the manager's default
        definition.

        Store defaults are captured during first placement by the owning workflow; this lookup does
        not choose a policy from current Replica order or a Store's current defaults.

        Example:
            >>> policies = manager.resolve_effective_policies(asset_id)  # doctest: +SKIP


        :param digital_asset_id: Registered atomic Asset identity to inspect or update.
        :return: Effective replication/backup definitions with their reported sources.
        """
        ...

    @abc.abstractmethod
    def assess_replication(
        self,
        digital_asset_id: DigitalAssetID,
    ) -> StoragePolicyAssessment:
        """
        Assess the Asset's effective replication policy using readable claims, recorded
        verification, and Store constraints.

        Results distinguish present/eligible claims and minimum/target satisfaction. Observation
        does not itself require fresh hashing or repair the recorded state.

        Example:
            >>> assessment = manager.assess_replication(asset_id)  # doctest: +SKIP


        :param digital_asset_id: Registered atomic Asset identity to inspect or update.
        :return: Policy assessment containing observed claim IDs, threshold flags, and diagnostics.
        """
        ...

    @abc.abstractmethod
    def assess_backup(
        self,
        digital_asset_id: DigitalAssetID,
    ) -> StoragePolicyAssessment:
        """
        Assess the Asset's effective backup policy using readable claims, recorded verification, and
        Store constraints.

        Results distinguish present/eligible claims and minimum/target satisfaction. Observation
        does not itself require fresh hashing or repair the recorded state.

        Example:
            >>> assessment = manager.assess_backup(asset_id)  # doctest: +SKIP


        :param digital_asset_id: Registered atomic Asset identity to inspect or update.
        :return: Policy assessment containing observed claim IDs, threshold flags, and diagnostics.
        """
        ...

    # Todo: asses_composite_digital_asset - to asses every member of a digital asset
    @abc.abstractmethod
    def assess_digital_asset(
        self,
        digital_asset_id: DigitalAssetID,
    ) -> DigitalAssetStorageAssessment:
        """
        Combine current readability, policy satisfaction, and exact-recreation reachability
        evidence.

        Recreation assessment considers recipe prerequisites and resolver-reported artifacts without
        executing the transformation. Independent observations need not form an atomic snapshot
        across repositories and Stores.

        Example:
            >>> assessment = manager.assess_digital_asset(asset_id)  # doctest: +SKIP


        :param digital_asset_id: Registered atomic Asset identity to inspect or update.
        :return: Aggregate reported readability, policy results, and currently recoverable exact derivations.
        """

        ...

    @abc.abstractmethod
    def plan_replication(
        self,
        digital_asset_id: DigitalAssetID,
    ) -> DigitalAssetReplicationPlan:
        """
        Propose destinations, verification, removal, or an exact recreation route under the
        effective policy.

        Plans may be based on recorded verification and configuration evidence rather than fresh
        physical checks. They do not reserve destinations, execute transformations, or authorize
        unconditional deletion; execution must revalidate current conditions.

        Example:
            >>> plan = manager.plan_replication(asset_id)  # doctest: +SKIP


        :param digital_asset_id: Registered atomic Asset identity to inspect or update.
        :return: Replication proposal and warnings about unavailable destinations or recreation routes.
        """
        ...

    @abc.abstractmethod
    def plan_backup(
        self,
        digital_asset_id: DigitalAssetID,
    ) -> DigitalAssetBackupPlan:
        """
        Propose backup destinations, candidate source claims, verification, and surplus removal.

        A zero target can propose removing existing backup claims. The plan itself does not enforce
        every execution-time retention or repository-race requirement and does not publish or remove
        bytes.

        Example:
            >>> plan = manager.plan_backup(asset_id)  # doctest: +SKIP


        :param digital_asset_id: Registered atomic Asset identity to inspect or update.
        :return: Backup proposal with candidate sources and any destination-shortage diagnostics.
        """
        ...


__all__ = ["StoragePolicyAPI"]
