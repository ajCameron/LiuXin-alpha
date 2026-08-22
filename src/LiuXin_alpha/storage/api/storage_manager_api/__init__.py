"""
LiuXin-aware domain, policy, routing, and persistence-port contracts.
"""

from __future__ import annotations

import abc

from types import TracebackType

from LiuXin_alpha.storage.api.storage_manager_api.catalog_api import (
    DigitalAssetRegistryAPI,
)
from LiuXin_alpha.storage.api.storage_manager_api.composites_api import (
    CompositeDigitalAssetAPI,
)
from LiuXin_alpha.storage.api.storage_manager_api.convenience_api import (
    DigitalAssetFileIdentifier,
    StorageConvenienceAPI,
)
from LiuXin_alpha.storage.api.storage_manager_api.derivations_api import (
    DigitalAssetDerivationRegistryAPI,
    ReproductionRecipeArtifactResolverAPI,
)
from LiuXin_alpha.storage.api.storage_manager_api.errors import (
    DigitalAssetDerivationNotFound,
    CompositeDigitalAssetIncomplete,
    CompositeDigitalAssetNotFound,
    DigitalAssetNotFound,
    NoReadableReplica,
    StoragePolicyUnsatisfied,
    StoreReconciliationPlanStale,
    ReplicaNotFound,
    StoreConfigurationNotFound,
    StorageManagementError,
)
from LiuXin_alpha.storage.api.storage_manager_api.ingest_api import (
    DigitalAssetIngestAPI,
)
from LiuXin_alpha.storage.api.storage_manager_api.item_links_api import (
    ItemDigitalAssetLinkAPI,
)
from LiuXin_alpha.storage.api.storage_manager_api.location_api import BoundLocation
from LiuXin_alpha.storage.api.storage_manager_api.location_factory import LocationFactory
from LiuXin_alpha.storage.api.storage_manager_api.models import (
    DigitalAssetDerivationDeclaration,
    DigitalAssetDerivationGraph,
    DigitalAssetDerivationGraphDirection,
    DigitalAssetDerivationID,
    DigitalAssetDerivationRecord,
    DigitalAssetRecreationPlan,
    DigitalAssetLossAction,
    DigitalAssetBackupPlan,
    BackupPolicy,
    BackupPolicyID,
    BackupPolicyRecord,
    CompositeDigitalAssetAvailabilityAssessment,
    CompositeDigitalAssetDeclaration,
    CompositeDigitalAssetID,
    CompositeDigitalAssetMemberResolution,
    CompositeDigitalAssetMembership,
    CompositeDigitalAssetRecord,
    DigitalAssetDeclaration,
    DigitalAssetID,
    DigitalAssetMetadata,
    DigitalAssetIngestResult,
    DigitalAssetRecord,
    DigitalAssetResolution,
    DigitalAssetStorageAssessment,
    DigitalAssetVerificationReport,
    DigitalAssetDerivationKind,
    DigitalAssetDerivationSourceReference,
    ReplicaSeparationDimension,
    ItemDigitalAssetResolution,
    ItemID,
    StoreReconciliationPlan,
    StoreReconciliationReport,
    ReproductionRecipeArtifactReference,
    ReproductionRecipeInputReference,
    ReplicaDeclaration,
    ReplicaID,
    ReplicaMode,
    ReplicaObservation,
    ReplicaRecord,
    ReplicaRemovalReport,
    ReplicaState,
    ReplicaVerificationReport,
    DigitalAssetReplicationPlan,
    ReplicationPolicy,
    ReplicationPolicyID,
    ReplicationPolicyRecord,
    Reproducibility,
    ReproductionRecipe,
    ResolvedStoragePolicies,
    StorageBootstrapIssue,
    StorageBootstrapReport,
    StoragePolicyAssessment,
    StorageOperationalIssue,
    StorageOperationalSeverity,
    StorageOperationalStatus,
    StorageRecoveryAction,
    StoreConfiguration,
    StoreStatusObservation,
    TopologyRelation,
)
from LiuXin_alpha.storage.api.storage_manager_api.policies_api import StoragePolicyAPI
from LiuXin_alpha.storage.api.storage_manager_api.operational_api import (
    StorageOperationalStatusAPI,
)
from LiuXin_alpha.storage.api.storage_manager_api.reconciliation_api import StorageReconciliationAPI
from LiuXin_alpha.storage.api.storage_manager_api.replicas_api import ReplicaLifecycleAPI
from LiuXin_alpha.storage.api.persistence_api import (
    DigitalAssetDerivationRepositoryAPI,
    CompositeDigitalAssetRepositoryAPI,
    DigitalAssetRepositoryAPI,
    ReplicaRepositoryAPI,
    StorageUnitOfWorkAPI,
    StorageUnitOfWorkFactoryAPI,
)
from LiuXin_alpha.storage.api.storage_manager_api.retrieval_api import (
    DigitalAssetRetrievalAPI,
)
from LiuXin_alpha.storage.api.storage_manager_api.router_api import StorageRouterAPI
from LiuXin_alpha.storage.api.storage_manager_api.stores_api import StoreAdministrationAPI


class StorageManagerAPI(
    StorageConvenienceAPI,
    StorageRouterAPI,
    StoreAdministrationAPI,
    DigitalAssetRegistryAPI,
    DigitalAssetIngestAPI,
    DigitalAssetRetrievalAPI,
    ItemDigitalAssetLinkAPI,
    ReplicaLifecycleAPI,
    StoragePolicyAPI,
    CompositeDigitalAssetAPI,
    DigitalAssetDerivationRegistryAPI,
    StorageReconciliationAPI,
    StorageOperationalStatusAPI,
    abc.ABC,
):
    """
    Complete manager facade over storage domain values and configured Stores.

    Concrete managers orchestrate byte publication and domain repositories.
    Database records, ORM models, and raw driver addresses do not cross this
    boundary. Concrete convenience methods accept ordinary bytes, paths, IDs,
    records, and keyword metadata, then delegate to the explicit domain
    methods. The context manager closes configured Stores on exit.

    Example:
        >>> def read_asset(manager: StorageManagerAPI, asset_id: int) -> bytes:
        ...     return manager.read_file(asset_id)
    """

    def __enter__(self) -> StorageManagerAPI:
        """
        Enter the manager lifetime and return this manager.

        Example:
            >>> entered = manager.__enter__()  # doctest: +SKIP


        :return:
        """

        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """
        Close configured Stores when leaving the manager context.

        Example:
            >>> manager.__exit__(None, None, None)  # doctest: +SKIP


        :param exc_type:
        :param exc:
        :param traceback:
        :return:
        """

        self.close()


__all__ = [
    "DigitalAssetDerivationDeclaration",
    "DigitalAssetDerivationGraph",
    "DigitalAssetDerivationGraphDirection",
    "DigitalAssetDerivationID",
    "DigitalAssetDerivationNotFound",
    "DigitalAssetDerivationRecord",
    "DigitalAssetRecreationPlan",
    "DigitalAssetDerivationRegistryAPI",
    "DigitalAssetDerivationRepositoryAPI",
    "DigitalAssetLossAction",
    "DigitalAssetBackupPlan",
    "BackupPolicy",
    "BackupPolicyID",
    "BackupPolicyRecord",
    "BoundLocation",
    "CompositeDigitalAssetAvailabilityAssessment",
    "CompositeDigitalAssetAPI",
    "CompositeDigitalAssetDeclaration",
    "CompositeDigitalAssetID",
    "CompositeDigitalAssetIncomplete",
    "CompositeDigitalAssetMemberResolution",
    "CompositeDigitalAssetMembership",
    "CompositeDigitalAssetNotFound",
    "CompositeDigitalAssetRecord",
    "CompositeDigitalAssetRepositoryAPI",
    "DigitalAssetDeclaration",
    "DigitalAssetFileIdentifier",
    "DigitalAssetID",
    "DigitalAssetMetadata",
    "DigitalAssetNotFound",
    "DigitalAssetRepositoryAPI",
    "DigitalAssetIngestAPI",
    "DigitalAssetIngestResult",
    "DigitalAssetRecord",
    "DigitalAssetRegistryAPI",
    "DigitalAssetResolution",
    "DigitalAssetRetrievalAPI",
    "DigitalAssetStorageAssessment",
    "DigitalAssetVerificationReport",
    "DigitalAssetDerivationKind",
    "DigitalAssetDerivationSourceReference",
    "ReplicaSeparationDimension",
    "ItemDigitalAssetResolution",
    "ItemDigitalAssetLinkAPI",
    "ItemID",
    "LocationFactory",
    "NoReadableReplica",
    "StoragePolicyUnsatisfied",
    "StoreReconciliationPlan",
    "StoreReconciliationPlanStale",
    "StoreReconciliationReport",
    "ReproductionRecipeArtifactReference",
    "ReproductionRecipeArtifactResolverAPI",
    "ReproductionRecipeInputReference",
    "ReplicaDeclaration",
    "ReplicaID",
    "ReplicaLifecycleAPI",
    "ReplicaMode",
    "ReplicaNotFound",
    "ReplicaObservation",
    "ReplicaRecord",
    "ReplicaRemovalReport",
    "ReplicaRepositoryAPI",
    "ReplicaState",
    "ReplicaVerificationReport",
    "DigitalAssetReplicationPlan",
    "ReplicationPolicy",
    "ReplicationPolicyID",
    "ReplicationPolicyRecord",
    "Reproducibility",
    "ReproductionRecipe",
    "ResolvedStoragePolicies",
    "StorageBootstrapIssue",
    "StorageBootstrapReport",
    "StorageConvenienceAPI",
    "StorageManagementError",
    "StorageManagerAPI",
    "StorageOperationalIssue",
    "StorageOperationalSeverity",
    "StorageOperationalStatus",
    "StorageOperationalStatusAPI",
    "StorageRecoveryAction",
    "StoragePolicyAPI",
    "StorageReconciliationAPI",
    "StorageRouterAPI",
    "StorageUnitOfWorkAPI",
    "StorageUnitOfWorkFactoryAPI",
    "StoreAdministrationAPI",
    "StoreConfigurationNotFound",
    "StoragePolicyAssessment",
    "StoreConfiguration",
    "StoreStatusObservation",
    "TopologyRelation",
]
