"""Storage policy access/update methods for the storage manager."""

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
    """

    @abc.abstractmethod
    def create_replication_policy(self, policy: "ReplicationPolicy") -> "ReplicationPolicyRecord":
        """
        We are writing an existing replication policy out to the database.

        Returning the record of the policy.
        :param policy:
        :return:
        """

    @abc.abstractmethod
    def get_replication_policy(self, replication_policy_id: "ReplicationPolicyID") -> "ReplicationPolicyRecord":
        """
        Load a replication policy from the database.

        :param replication_policy_id:
        :return:
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
        """

    @abc.abstractmethod
    def iter_replication_policies(self) -> Iterator["ReplicationPolicyRecord"]:
        """
        Iterate over all replication policies.

        :return:
        """

    @abc.abstractmethod
    def create_backup_policy(self, policy: "BackupPolicy") -> "BackupPolicyRecord":
        """
        Create a backup policy.

        :param policy:
        :return:
        """

    @abc.abstractmethod
    def get_backup_policy(self, backup_policy_id: "BackupPolicyID") -> "BackupPolicyRecord":
        """
        Retrieve a backup policy.

        :param backup_policy_id:
        :return:
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
        """

    @abc.abstractmethod
    def iter_backup_policies(self) -> Iterator["BackupPolicyRecord"]:
        """
        Iterate over all backup policies.

        :return:
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
        """

    @abc.abstractmethod
    def assess_replication(self, digital_asset_id: "DigitalAssetID") -> "ReplicationStatus":
        """
        Check the level of replication for an asset.

        :param digital_asset_id:
        :return:
        """

    @abc.abstractmethod
    def iter_badly_replicated_assets(self) -> Iterator["DigitalAssetID"]:
        """
        Iter assets which do not meet the replication plan.

        :return:
        """

    @abc.abstractmethod
    def iter_badly_backed_up_assets(self) -> Iterator["DigitalAssetID"]:
        """
        Iter assets which do not meet the replication plan.

        :return:
        """

    @abc.abstractmethod
    def plan_replication(self, digital_asset_id: "DigitalAssetID") -> "ReplicationPlan":
        """
        Create a plan to get the asset to full replication status.

        :param digital_asset_id:
        :return:
        """

    @abc.abstractmethod
    def iter_unplanned_assets(
            self,
            un_replication: bool = False,
            un_backed_up: bool = False) -> Iterator["DigitalAssetID"]:
        """
        Itter over assets which do not have a plan.

        :param un_replication: If True, iter over the un replicated assets.
        :param un_backed_up: If True, iter over the un backed up assets.
        :return:
        """