"""
Shared mutable state for composed storage-manager implementations.
"""

from __future__ import annotations

from collections.abc import Iterable
from threading import RLock
from typing import TYPE_CHECKING, Any
from uuid import UUID

import LiuXin_alpha.storage.api as api
from LiuXin_alpha.storage.api.storage_manager_api import StorageManagerAPI
from LiuXin_alpha.storage.storage_manager.mixins._types import (
    StoreFactory,
    StoreRegistration,
    _IngestOperation,
    _ItemTarget,
)


class _StorageManagerState(StorageManagerAPI):
    """
    Typed state base shared by the orthogonal implementation mixins.

    The public manager contract makes ``@override`` and cross-component API
    calls checkable in each standalone mixin. Private cross-cutting helpers are
    supplied by the final composition and remain dynamically visible only to
    static analysis; runtime attribute lookup still fails normally on typos.
    """

    if TYPE_CHECKING:

        def __getattr__(self, name: str) -> Any:
            """
            Describe private helpers supplied by sibling mixins.


            :param name:
            :return:
            """

            ...

    def __init__(
        self,
        *,
        store_registrations: Iterable[StoreRegistration] = (),
        store_factory: StoreFactory | None = None,
        default_store_ref: api.StoreUUID | None = None,
        default_replication_policy: api.ReplicationPolicy | None = None,
        default_backup_policy: api.BackupPolicy | None = None,
        artifact_resolver: (api.ReproductionRecipeArtifactResolverAPI | None) = None,
    ) -> None:
        """
        Initialize empty manager state and attach supplied Store instances.


        :param store_registrations:
        :param store_factory:
        :param default_store_ref:
        :param default_replication_policy:
        :param default_backup_policy:
        :param artifact_resolver:
        :return:
        """

        self._lock = RLock()
        self._store_factory = store_factory
        self._artifact_resolver = artifact_resolver
        self._store_configurations: dict[api.StoreUUID, api.StoreConfiguration] = {}
        self._stores: dict[api.StoreUUID, api.StoreAPI] = {}
        self._default_store_ref = default_store_ref

        self._assets: dict[api.DigitalAssetID, api.DigitalAssetRecord] = {}
        self._replicas: dict[api.ReplicaID, api.ReplicaRecord] = {}
        self._composites: dict[
            api.CompositeDigitalAssetID, api.CompositeDigitalAssetRecord
        ] = {}
        self._derivations: dict[
            api.DigitalAssetDerivationID, api.DigitalAssetDerivationRecord
        ] = {}
        self._replication_policies: dict[
            api.ReplicationPolicyID, api.ReplicationPolicyRecord
        ] = {}
        self._backup_policies: dict[api.BackupPolicyID, api.BackupPolicyRecord] = {}
        self._item_targets: dict[tuple[api.ItemID, str], _ItemTarget] = {}
        self._ingest_operations: dict[UUID, _IngestOperation] = {}
        self._ingest_identity_locks: dict[
            tuple[int, tuple[api.Digest, ...]], RLock
        ] = {}

        self._next_asset_id = 1
        self._next_replica_id = 1
        self._next_composite_id = 1
        self._next_derivation_id = 1
        self._next_replication_policy_id = 1
        self._next_backup_policy_id = 1
        self._revision_counter = 0
        self._replica_generation = 0

        self._default_replication_policy = (
            api.ReplicationPolicy()
            if default_replication_policy is None
            else default_replication_policy
        )
        self._default_backup_policy = (
            api.BackupPolicy()
            if default_backup_policy is None
            else default_backup_policy
        )

        for configuration, store in store_registrations:
            self.attach_store(configuration, store)
        if self._default_store_ref is not None:
            self.set_default_store(self._default_store_ref)


__all__ = ["_StorageManagerState"]
