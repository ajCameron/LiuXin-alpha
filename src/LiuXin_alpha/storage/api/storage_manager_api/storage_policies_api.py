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
    """CRUD and assessment methods for storage policies."""

    @abc.abstractmethod
    def create_replication_policy(self, policy: "ReplicationPolicy") -> "ReplicationPolicyRecord":
        ...

    @abc.abstractmethod
    def get_replication_policy(self, replication_policy_id: "ReplicationPolicyID") -> "ReplicationPolicyRecord":
        ...

    @abc.abstractmethod
    def update_replication_policy(
        self,
        replication_policy_id: "ReplicationPolicyID",
        policy: "ReplicationPolicy",
    ) -> "ReplicationPolicyRecord":
        ...

    @abc.abstractmethod
    def iter_replication_policies(self) -> Iterator["ReplicationPolicyRecord"]:
        ...

    @abc.abstractmethod
    def create_backup_policy(self, policy: "BackupPolicy") -> "BackupPolicyRecord":
        ...

    @abc.abstractmethod
    def get_backup_policy(self, backup_policy_id: "BackupPolicyID") -> "BackupPolicyRecord":
        ...

    @abc.abstractmethod
    def update_backup_policy(
        self,
        backup_policy_id: "BackupPolicyID",
        policy: "BackupPolicy",
    ) -> "BackupPolicyRecord":
        ...

    @abc.abstractmethod
    def iter_backup_policies(self) -> Iterator["BackupPolicyRecord"]:
        ...

    @abc.abstractmethod
    def set_digital_asset_policies(
        self,
        digital_asset_id: "DigitalAssetID",
        *,
        replication_policy_id: Optional["ReplicationPolicyID"] = None,
        backup_policy_id: Optional["BackupPolicyID"] = None,
    ) -> "DigitalAssetRow":
        ...

    @abc.abstractmethod
    def assess_replication(self, digital_asset_id: "DigitalAssetID") -> "ReplicationStatus":
        ...

    @abc.abstractmethod
    def plan_replication(self, digital_asset_id: "DigitalAssetID") -> "ReplicationPlan":
        ...
