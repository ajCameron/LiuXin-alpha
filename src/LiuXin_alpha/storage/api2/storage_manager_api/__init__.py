"""
LiuXin-aware storage manager facade package.

This package sits above the transactional ``FileStore`` contract.  Each concern
is available as a narrow ABC, while ``StorageManagerAPI`` composes the complete
surface intended to replace the legacy storage manager API.
"""

from __future__ import annotations

import abc

from types import TracebackType

from LiuXin_alpha.storage.api2.storage_manager_api.catalog_api import DigitalAssetCatalogAPI
from LiuXin_alpha.storage.api2.storage_manager_api.composites_api import CompositeAssetAPI
from LiuXin_alpha.storage.api2.storage_manager_api.ingest_api import AssetIngestAPI
from LiuXin_alpha.storage.api2.storage_manager_api.location_api import BoundLocation
from LiuXin_alpha.storage.api2.storage_manager_api.models import (
    AssetReplicaID,
    AssetReplicaRecordAPI,
    AssetVerificationResult,
    BackupPlan,
    BackupPolicy,
    BackupPolicyID,
    BackupPolicyRecord,
    BackupStatus,
    CompositeAssetHealth,
    CompositeDigitalAssetID,
    CompositeDigitalAssetRecordAPI,
    CompositeMemberSpec,
    DigitalAssetID,
    DigitalAssetRecordAPI,
    DigitalAssetStorageHealth,
    DistinctBy,
    EffectiveStoragePolicies,
    IngestResult,
    ItemAssetSelection,
    ItemID,
    PolicyStatus,
    ReconciliationReport,
    ReplicaMode,
    ReplicaRemovalResult,
    ReplicaState,
    ReplicaVerificationResult,
    ReplicationPlan,
    ReplicationPolicy,
    ReplicationPolicyID,
    ReplicationPolicyRecord,
    ReplicationStatus,
    StorageBootstrapIssue,
    StorageBootstrapReport,
    StoreID,
    StoreSpec,
)
from LiuXin_alpha.storage.api2.storage_manager_api.policies_api import StoragePolicyAPI
from LiuXin_alpha.storage.api2.storage_manager_api.reconciliation_api import StorageReconciliationAPI
from LiuXin_alpha.storage.api2.storage_manager_api.replicas_api import ReplicaLifecycleAPI
from LiuXin_alpha.storage.api2.storage_manager_api.retrieval_api import AssetRetrievalAPI
from LiuXin_alpha.storage.api2.storage_manager_api.router_api import StorageRouterAPI
from LiuXin_alpha.storage.api2.storage_manager_api.stores_api import StoreAdministrationAPI


class StorageManagerAPI(
    StorageRouterAPI,
    StoreAdministrationAPI,
    DigitalAssetCatalogAPI,
    AssetIngestAPI,
    AssetRetrievalAPI,
    ReplicaLifecycleAPI,
    StoragePolicyAPI,
    CompositeAssetAPI,
    StorageReconciliationAPI,
    abc.ABC,
):
    """Complete manager facade above the small transactional backend core.

    Concrete managers implement the narrow routing, administration, catalogue,
    ingest, retrieval, replica, policy, composite, and reconciliation facades.
    The context manager closes configured stores on exit.

    Example:
        >>> def read_asset(
        ...     manager: StorageManagerAPI, asset_id: DigitalAssetID,
        ... ) -> bytes:
        ...     location = manager.locate_digital_asset(asset_id, verify=True)
        ...     return manager.read_bytes(location)
    """

    def __enter__(self):
        """Enter the manager lifetime and return the manager itself.

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
        """Close all configured stores when leaving the manager context.

        Example:
            >>> manager.__exit__(None, None, None)  # doctest: +SKIP
        """

        self.close()


__all__ = [
    "AssetIngestAPI",
    "AssetReplicaID",
    "AssetReplicaRecordAPI",
    "AssetRetrievalAPI",
    "AssetVerificationResult",
    "BackupPlan",
    "BackupPolicy",
    "BackupPolicyID",
    "BackupPolicyRecord",
    "BackupStatus",
    "BoundLocation",
    "CompositeAssetAPI",
    "CompositeAssetHealth",
    "CompositeDigitalAssetID",
    "CompositeDigitalAssetRecordAPI",
    "CompositeMemberSpec",
    "DigitalAssetID",
    "DigitalAssetCatalogAPI",
    "DigitalAssetRecordAPI",
    "DigitalAssetStorageHealth",
    "DistinctBy",
    "EffectiveStoragePolicies",
    "IngestResult",
    "ItemAssetSelection",
    "ItemID",
    "PolicyStatus",
    "ReconciliationReport",
    "ReplicaLifecycleAPI",
    "ReplicaMode",
    "ReplicaRemovalResult",
    "ReplicaState",
    "ReplicaVerificationResult",
    "ReplicationPlan",
    "ReplicationPolicy",
    "ReplicationPolicyID",
    "ReplicationPolicyRecord",
    "ReplicationStatus",
    "StorageBootstrapIssue",
    "StorageBootstrapReport",
    "StorageManagerAPI",
    "StoragePolicyAPI",
    "StorageReconciliationAPI",
    "StorageRouterAPI",
    "StoreID",
    "StoreAdministrationAPI",
    "StoreSpec",
]
