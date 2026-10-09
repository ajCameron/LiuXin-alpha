"""
Initialize process-owned state shared by composed storage-manager workflow mixins.

The abstract base combines public APIs and private helper contracts for type-checked
cross-component calls. It allocates empty registries, counters, and locks; supplied
Store attachment can perform startup, while persistence adapters replace selected
state and transaction behavior elsewhere in the manager composition.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Iterable, MutableMapping
from threading import RLock
from uuid import UUID

from LiuXin_alpha.storage.api import models as storage_models
from LiuXin_alpha.storage.api import storage_manager_api as manager_api
from LiuXin_alpha.storage.api import store_api
from LiuXin_alpha.storage.api.storage_manager_api import StorageManagerAPI
from LiuXin_alpha.storage.storage_manager.mixins._contracts import (
    _StorageManagerMechanics,
    _StorageManagerPolicyHooks,
    _StorageManagerStoreHooks,
)
from LiuXin_alpha.storage.storage_manager.mixins._types import (
    StoreFactory,
    StoreRegistration,
    _IngestOperation,
    _ItemTarget,
)


class _StorageManagerState(
    StorageManagerAPI,
    _StorageManagerMechanics,
    _StorageManagerPolicyHooks,
    _StorageManagerStoreHooks,
):
    """
    Provide typed shared state and initialization for the final manager composition.

    The public API and abstract helper contracts keep incomplete compositions abstract. This base
    initializes one set of mutable registries/counters and a reentrant lock; sibling mixins supply
    operational methods. It does not make dictionary access automatically synchronized or install
    durable persistence.

    Example:
        >>> manager = TransientStorageManager(store_registrations=registrations)  # doctest: +SKIP
    """

    def __init__(
        self,
        *,
        store_registrations: Iterable[StoreRegistration] = (),
        store_factory: StoreFactory | None = None,
        default_store_ref: storage_models.StoreUUID | None = None,
        default_replication_policy: manager_api.ReplicationPolicy | None = None,
        default_backup_policy: manager_api.BackupPolicy | None = None,
        artifact_resolver: (
            manager_api.ReproductionRecipeArtifactResolverAPI | None
        ) = None,
    ) -> None:
        """
        Allocate empty state, retain configured helpers/default policies, and attach supplied Stores
        in order.

        All record/operation maps and identity-lock caches start empty; metadata IDs start at one
        and revision/Replica generations at zero. Missing policies receive new default values, while
        supplied policies, factory, and resolver are retained without added validation.

        Each registration delegates to attach_store with its default startup behavior. The first
        attachment can become the default when none was supplied; any resulting default is checked
        after all attachments. Iteration, startup, duplicate, or final-default failures propagate
        without cleanup or rollback of earlier attachments. No Store factory is invoked directly by
        this initializer.

        Example:
            >>> manager = TransientStorageManager(  # doctest: +SKIP
            ...     store_registrations=((configuration, store),), default_store_ref=store.store_ref,
            ... )


        :param store_registrations: Iterable of configuration/facade pairs consumed once and attached in order.
        :param store_factory: Optional retained callable used later by Store lifecycle operations.
        :param default_store_ref: Optional default UUID retained before attachment and checked against attached facades afterwards.
        :param default_replication_policy: Optional retained policy value; None constructs ReplicationPolicy().
        :param default_backup_policy: Optional retained policy value; None constructs BackupPolicy().
        :param artifact_resolver: Optional external-artefact availability provider retained for later recovery decisions.
        :return: None after state setup and Store attachment/default validation; failures may leave earlier Store startup and registration effects.
        """

        self._lock = RLock()
        self._store_factory = store_factory
        self._artifact_resolver = artifact_resolver
        self._store_configurations: dict[
            storage_models.StoreUUID, manager_api.StoreConfiguration
        ] = {}
        self._stores: dict[storage_models.StoreUUID, store_api.StoreAPI] = {}
        self._default_store_ref = default_store_ref

        self._assets: MutableMapping[
            manager_api.DigitalAssetID, manager_api.DigitalAssetRecord
        ] = {}
        self._replicas: MutableMapping[
            manager_api.ReplicaID, manager_api.ReplicaRecord
        ] = {}
        self._composites: MutableMapping[
            manager_api.CompositeDigitalAssetID, manager_api.CompositeDigitalAssetRecord
        ] = {}
        self._derivations: MutableMapping[
            manager_api.DigitalAssetDerivationID,
            manager_api.DigitalAssetDerivationRecord,
        ] = {}
        self._replication_policies: MutableMapping[
            manager_api.ReplicationPolicyID, manager_api.ReplicationPolicyRecord
        ] = {}
        self._backup_policies: MutableMapping[
            manager_api.BackupPolicyID, manager_api.BackupPolicyRecord
        ] = {}
        self._item_targets: MutableMapping[
            tuple[manager_api.ItemID, str], _ItemTarget
        ] = {}
        self._ingest_operations: MutableMapping[UUID, _IngestOperation] = {}
        self._ingest_identity_locks: dict[
            tuple[int, tuple[storage_models.Digest, ...]], RLock
        ] = {}
        self._operational_status_history: deque[
            manager_api.StorageOperationalStatus
        ] = deque(maxlen=100)

        self._next_asset_id = 1
        self._next_replica_id = 1
        self._next_composite_id = 1
        self._next_derivation_id = 1
        self._next_replication_policy_id = 1
        self._next_backup_policy_id = 1
        self._revision_counter = 0
        self._replica_generation = 0

        self._default_replication_policy = (
            manager_api.ReplicationPolicy()
            if default_replication_policy is None
            else default_replication_policy
        )
        self._default_backup_policy = (
            manager_api.BackupPolicy()
            if default_backup_policy is None
            else default_backup_policy
        )

        for configuration, store in store_registrations:
            self.attach_store(configuration, store)
        if self._default_store_ref is not None:
            self.set_default_store(self._default_store_ref)


__all__ = ["_StorageManagerState"]
