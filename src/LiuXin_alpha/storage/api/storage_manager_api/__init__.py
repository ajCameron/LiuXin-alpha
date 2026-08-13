"""LiuXin-aware domain, policy, routing, and persistence-port contracts."""

from __future__ import annotations

import abc

from types import TracebackType

from LiuXin_alpha.storage.api.storage_manager_api.catalog_api import AssetRegistryAPI
from LiuXin_alpha.storage.api.storage_manager_api.composites_api import CompositeAssetAPI
from LiuXin_alpha.storage.api.storage_manager_api.errors import (
    CompositeAssetNotFound,
    CompositeIncomplete,
    DigitalAssetNotFound,
    NoReadableReplica,
    PolicyUnsatisfied,
    ReconciliationPlanStale,
    ReplicaNotFound,
    StorageManagementError,
)
from LiuXin_alpha.storage.api.storage_manager_api.ingest_api import AssetIngestAPI
from LiuXin_alpha.storage.api.storage_manager_api.location_api import BoundLocation
from LiuXin_alpha.storage.api.storage_manager_api.location_factory import LocationFactory
from LiuXin_alpha.storage.api.storage_manager_api.models import (
    AssetVerificationResult,
    BackupPlan,
    BackupPolicy,
    BackupPolicyID,
    BackupStatus,
    CompositeAssetHealth,
    CompositeDigitalAsset,
    CompositeDigitalAssetID,
    CompositeDigitalAssetSpec,
    CompositeMemberSpec,
    DigitalAsset,
    DigitalAssetID,
    DigitalAssetMetadata,
    DigitalAssetSpec,
    DigitalAssetStorageHealth,
    DistinctBy,
    EffectiveStoragePolicies,
    IngestResult,
    ItemAssetSelection,
    ItemID,
    PolicyStatus,
    ReconciliationPlan,
    ReconciliationReport,
    Replica,
    ReplicaID,
    ReplicaMode,
    ReplicaObservation,
    ReplicaRemovalResult,
    ReplicaSpec,
    ReplicaState,
    ReplicaVerificationResult,
    ReplicationPlan,
    ReplicationPolicy,
    ReplicationPolicyID,
    ReplicationStatus,
    ResolvedAsset,
    ResolvedCompositeMember,
    StorageBootstrapIssue,
    StorageBootstrapReport,
    StoredBackupPolicy,
    StoredReplicationPolicy,
    StoreID,
    StoreSpec,
    TopologyRelation,
)
from LiuXin_alpha.storage.api.storage_manager_api.policies_api import StoragePolicyAPI
from LiuXin_alpha.storage.api.storage_manager_api.reconciliation_api import StorageReconciliationAPI
from LiuXin_alpha.storage.api.storage_manager_api.replicas_api import ReplicaLifecycleAPI
from LiuXin_alpha.storage.api.storage_manager_api.repositories_api import (
    CompositeAssetRepositoryAPI,
    DigitalAssetRepositoryAPI,
    ReplicaRepositoryAPI,
    StorageUnitOfWorkAPI,
    StorageUnitOfWorkFactoryAPI,
)
from LiuXin_alpha.storage.api.storage_manager_api.retrieval_api import AssetRetrievalAPI
from LiuXin_alpha.storage.api.storage_manager_api.router_api import StorageRouterAPI
from LiuXin_alpha.storage.api.storage_manager_api.stores_api import StoreAdministrationAPI


class StorageManagerAPI(
    StorageRouterAPI,
    StoreAdministrationAPI,
    AssetRegistryAPI,
    AssetIngestAPI,
    AssetRetrievalAPI,
    ReplicaLifecycleAPI,
    StoragePolicyAPI,
    CompositeAssetAPI,
    StorageReconciliationAPI,
    abc.ABC,
):
    """Complete manager facade over storage domain values and configured Stores.

    Concrete managers orchestrate byte publication and domain repositories.
    Database records, ORM models, and raw driver addresses do not cross this
    boundary. The context manager closes configured Stores on exit.

    Example:
        >>> def read_asset(
        ...     manager: StorageManagerAPI, asset_id: DigitalAssetID,
        ... ) -> bytes:
        ...     resolved = manager.resolve_digital_asset(asset_id)
        ...     return manager.read_bytes(resolved.location)
    """

    def __enter__(self) -> StorageManagerAPI:
        """Enter the manager lifetime and return this manager.

        Example:
            >>> entered = manager.__enter__()  # doctest: +SKIP
        """

        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close configured Stores when leaving the manager context.

        Example:
            >>> manager.__exit__(None, None, None)  # doctest: +SKIP
        """

        self.close()


__all__ = [
    "AssetIngestAPI",
    "AssetRegistryAPI",
    "AssetRetrievalAPI",
    "AssetVerificationResult",
    "BackupPlan",
    "BackupPolicy",
    "BackupPolicyID",
    "BackupStatus",
    "BoundLocation",
    "CompositeAssetAPI",
    "CompositeAssetHealth",
    "CompositeAssetNotFound",
    "CompositeAssetRepositoryAPI",
    "CompositeDigitalAsset",
    "CompositeDigitalAssetID",
    "CompositeDigitalAssetSpec",
    "CompositeIncomplete",
    "CompositeMemberSpec",
    "DigitalAsset",
    "DigitalAssetID",
    "DigitalAssetMetadata",
    "DigitalAssetNotFound",
    "DigitalAssetRepositoryAPI",
    "DigitalAssetSpec",
    "DigitalAssetStorageHealth",
    "DistinctBy",
    "EffectiveStoragePolicies",
    "IngestResult",
    "ItemAssetSelection",
    "ItemID",
    "LocationFactory",
    "NoReadableReplica",
    "PolicyStatus",
    "PolicyUnsatisfied",
    "ReconciliationPlan",
    "ReconciliationPlanStale",
    "ReconciliationReport",
    "Replica",
    "ReplicaID",
    "ReplicaLifecycleAPI",
    "ReplicaMode",
    "ReplicaNotFound",
    "ReplicaObservation",
    "ReplicaRemovalResult",
    "ReplicaRepositoryAPI",
    "ReplicaSpec",
    "ReplicaState",
    "ReplicaVerificationResult",
    "ReplicationPlan",
    "ReplicationPolicy",
    "ReplicationPolicyID",
    "ReplicationStatus",
    "ResolvedAsset",
    "ResolvedCompositeMember",
    "StorageBootstrapIssue",
    "StorageBootstrapReport",
    "StorageManagementError",
    "StorageManagerAPI",
    "StoragePolicyAPI",
    "StorageReconciliationAPI",
    "StorageRouterAPI",
    "StorageUnitOfWorkAPI",
    "StorageUnitOfWorkFactoryAPI",
    "StoreAdministrationAPI",
    "StoreID",
    "StoredBackupPolicy",
    "StoredReplicationPolicy",
    "StoreSpec",
    "TopologyRelation",
]
