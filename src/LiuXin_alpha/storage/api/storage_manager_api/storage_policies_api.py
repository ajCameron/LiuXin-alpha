"""Storage policy access/update methods for the storage manager.

Examples:
    Assess one asset against its effective policy::

        status = manager.assess_replication(digital_asset_id=42)
"""

from __future__ import annotations

import abc
from collections.abc import Iterator
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from LiuXin_alpha.storage.api.info_containers_api import DigitalAssetRow
    from LiuXin_alpha.storage.api.policy_apis import (
        BackupPolicy,
        BackupPolicyRecord,
        ReplicationPlan,
        ReplicationPolicy,
        ReplicationPolicyRecord,
        ReplicationStatus,
    )
    from LiuXin_alpha.storage.storage_types import BackupPolicyID, DigitalAssetID, ReplicationPolicyID


class StoragePoliciesManagerAPI(abc.ABC):
    """
    CRUD and assessment methods for storage policies.

    Two types of policy are available.
    They are deliberately different, as the split is important.

    Replication policies
    - Create live replicas of a file
    - Used to create local copies
    - Live versions

    Backup policies
    - Create cold backups of files

    Note. Replicas ARE NOT backups.
    In the same way that RAID is not a backup strategy.
    Backups are often cold and offline.
    And ideally, should be hard to destroy.

    Examples:
        Create and later assign a replication policy::

            record = manager.create_replication_policy(policy)
            asset = manager.set_digital_asset_policies(
                42, replication_policy_id=record.replication_policy_id
            )
    """

    @abc.abstractmethod
    def create_replication_policy(self, policy: "ReplicationPolicy") -> "ReplicationPolicyRecord":
        """
        We are writing an existing replication policy out to the database.

        Returning the record of the policy.
        :param policy:
        :return:

        Examples:
            Persist a value-object policy::

                record = manager.create_replication_policy(policy)
        """

    @abc.abstractmethod
    def get_replication_policy(self, replication_policy_id: "ReplicationPolicyID") -> "ReplicationPolicyRecord":
        """
        Load a replication policy from the database.

        :param replication_policy_id:
        :return:

        Examples:
            Load policy ``4``::

                record = manager.get_replication_policy(4)
        """

    @abc.abstractmethod
    def update_replication_policy(
        self,
        replication_policy_id: "ReplicationPolicyID",
        policy: "ReplicationPolicy",
    ) -> "ReplicationPolicyRecord":
        """
        Update a replication policy stored on the database.

        :param replication_policy_id:
        :param policy:
        :return:

        Examples:
            Replace the values stored for policy ``4``::

                record = manager.update_replication_policy(4, revised_policy)
        """

    @abc.abstractmethod
    def iter_replication_policies(self) -> Iterator["ReplicationPolicyRecord"]:
        """
        Iterate over all replication policies.

        :return:

        Examples:
            List every persisted replication policy::

                policies = list(manager.iter_replication_policies())
        """

    @abc.abstractmethod
    def create_backup_policy(self, policy: "BackupPolicy") -> "BackupPolicyRecord":
        """
        Create a backup policy.

        :param policy:
        :return:

        Examples:
            Persist a cold-backup policy::

                record = manager.create_backup_policy(policy)
        """

    @abc.abstractmethod
    def get_backup_policy(self, backup_policy_id: "BackupPolicyID") -> "BackupPolicyRecord":
        """
        Retrieve a backup policy.

        :param backup_policy_id:
        :return:

        Examples:
            Load backup policy ``6``::

                record = manager.get_backup_policy(6)
        """

    @abc.abstractmethod
    def update_backup_policy(
        self,
        backup_policy_id: "BackupPolicyID",
        policy: "BackupPolicy",
    ) -> "BackupPolicyRecord":
        """
        Update a backup policy.

        :param backup_policy_id:
        :param policy:
        :return:

        Examples:
            Replace the values stored for backup policy ``6``::

                record = manager.update_backup_policy(6, revised_policy)
        """

    @abc.abstractmethod
    def iter_backup_policies(self) -> Iterator["BackupPolicyRecord"]:
        """
        Iterate over all backup policies.

        :return:

        Examples:
            List every persisted backup policy::

                policies = list(manager.iter_backup_policies())
        """

    @abc.abstractmethod
    def set_digital_asset_policies(
        self,
        digital_asset_id: "DigitalAssetID",
        *,
        replication_policy_id: Optional["ReplicationPolicyID"] = None,
        backup_policy_id: Optional["BackupPolicyID"] = None,
    ) -> "DigitalAssetRow":
        """
        Write policies out to the assets.

        :param digital_asset_id:
        :param replication_policy_id:
        :param backup_policy_id:
        :return:

        Examples:
            Assign both policies in one update::

                asset = manager.set_digital_asset_policies(
                    42, replication_policy_id=4, backup_policy_id=6
                )
        """

    @abc.abstractmethod
    def assess_replication(self, digital_asset_id: "DigitalAssetID") -> "ReplicationStatus":
        """
        Check the level of replication for an asset.

        :param digital_asset_id:
        :return:

        Examples:
            Check whether asset ``42`` needs another replica::

                status = manager.assess_replication(42)
                needs_copy = not status.meets_target
        """

    @abc.abstractmethod
    def iter_badly_replicated_assets(self) -> Iterator["DigitalAssetID"]:
        """
        Iterate over assets which do not meet the replication plan.

        :return:

        Examples:
            Queue under-replicated assets for planning::

                asset_ids = list(manager.iter_badly_replicated_assets())
        """

    @abc.abstractmethod
    def iter_badly_backed_up_assets(self) -> Iterator["DigitalAssetID"]:
        """
        Iterate over assets which do not meet their backup plan.

        :return:

        Examples:
            Find assets requiring a new backup::

                asset_ids = list(manager.iter_badly_backed_up_assets())
        """

    @abc.abstractmethod
    def plan_replication(self, digital_asset_id: "DigitalAssetID") -> "ReplicationPlan":
        """
        Create a plan to get the asset to full replication status.

        :param digital_asset_id:
        :return:

        Examples:
            Ask which stores could satisfy asset ``42``::

                plan = manager.plan_replication(42)
        """

    @abc.abstractmethod
    def iter_unplanned_assets(
            self,
            un_replication: bool = False,
            un_backed_up: bool = False) -> Iterator["DigitalAssetID"]:
        """
        Iterate over assets which do not have a policy plan.

        :param un_replication: If True, iter over the un replicated assets.
        :param un_backed_up: If True, iter over the un backed up assets.
        :return:

        Examples:
            Find assets missing either kind of plan::

                asset_ids = list(manager.iter_unplanned_assets(
                    un_replication=True,
                    un_backed_up=True,
                ))
        """
