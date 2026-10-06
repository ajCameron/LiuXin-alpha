"""
Expose the composed storage-manager API, domain values, and compatibility ports.

Store plugins own byte mechanics; manager components coordinate Asset identity,
Replica claims, provenance, policy, and operator workflows. StorageManagerAPI
combines those contracts with convenience methods accepting ordinary caller
values. Its own methods only provide context-manager entry and delegated close.

Persistence protocols are identity-preserving re-exports from persistence_api
for existing adapters. Application callers should use manager operations;
database rows and raw driver addresses do not form this facade's value contract.
"""

from __future__ import annotations

import abc

from types import TracebackType

from LiuXin_alpha.storage.api.storage_manager_api.catalog_api import (
    DigitalAssetRecordOrder,
    DigitalAssetRegistryAPI,
)
from LiuXin_alpha.storage.api.storage_manager_api.composites_api import (
    CompositeDigitalAssetAPI,
)
from LiuXin_alpha.storage.api.storage_manager_api.convenience_api import (
    DigitalAssetFileIdentifier,
    StorageConvenienceAPI,
    StorageConvenienceBase,
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
    DigitalAssetReplacementAssessment,
    DigitalAssetReplacementStatus,
    BackupPolicy,
    BackupPolicyID,
    BackupPolicyRecord,
    CompositeDigitalAssetAvailabilityAssessment,
    CompositeDigitalAssetDeclaration,
    CompositeDigitalAssetID,
    CompositeDigitalAssetMemberResolution,
    CompositeDigitalAssetMemberStorageAssessment,
    CompositeDigitalAssetMemberVerificationReport,
    CompositeDigitalAssetResolution,
    CompositeDigitalAssetStorageAssessment,
    CompositeDigitalAssetVerificationReport,
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
    ExternalReproductionCommand,
    ReplicaSeparationDimension,
    ItemDigitalAssetResolution,
    ItemID,
    StoreReconciliationPlan,
    StoreReconciliationReport,
    ReproductionRecipeArtifactReference,
    ReproductionNormalizationDigest,
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
    StorageOperationalRecoverability,
    StorageOperationalSeverity,
    StorageOperationalStatus,
    StorageRecoveryAction,
    StoreBackingReference,
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
    StoreAdministrationAPI,
    StorageRouterAPI,
    DigitalAssetRegistryAPI,
    DigitalAssetIngestAPI,
    DigitalAssetRetrievalAPI,
    ReplicaLifecycleAPI,
    ItemDigitalAssetLinkAPI,
    CompositeDigitalAssetAPI,
    DigitalAssetDerivationRegistryAPI,
    StoragePolicyAPI,
    StorageReconciliationAPI,
    StorageOperationalStatusAPI,
    abc.ABC,
):
    """
    Combine manager workflows, domain records, and caller conveniences.

    The component order follows Store configuration, byte routing, Asset registration/ingest,
    Replica retrieval/lifecycle, logical relationships, policy, and operational reconciliation. That
    order also controls Python method resolution. Concrete managers supply the abstract operations;
    inherited conveniences delegate through those operations rather than exposing database rows or
    raw driver addresses.

    The composed abstract facade owns no constructor state, so concrete managers define
    initialization for their repositories and Stores. Context entry returns the existing manager
    without starting or probing Stores. Exit delegates to
    close regardless of the body's outcome and never suppresses its exception. Store shutdown and
    cleanup failures follow the concrete close implementation; this wrapper provides no transaction,
    publication rollback, or suppression of close errors.

    Example:
        >>> from LiuXin_alpha.storage.storage_manager import TransientStorageManager
        >>> with TransientStorageManager() as manager:
        ...     isinstance(manager, StorageManagerAPI)
        True
    """

    def __enter__(self) -> StorageManagerAPI:
        """
        Return this manager so context exit can reliably pair its concrete close operation.

        All initialization and resource setup remain with the concrete manager and its callers; this
        method does not check whether the manager was closed.

        Example:
            >>> from LiuXin_alpha.storage.storage_manager import TransientStorageManager
            >>> manager = TransientStorageManager()
            >>> manager.__enter__() is manager
            True
            >>> manager.close()


        :return: This same manager instance for use in the context body.
        """

        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """
        Close the manager on both normal and exceptional context exit.

        Ignore the supplied exception details and call self.close dynamically. Returning None leaves
        a body exception unsuppressed. A close failure propagates and can replace it as the active
        exception; no cleanup retry or transaction rollback is implemented by this wrapper.

        Example:
            >>> manager.__exit__(None, None, None)  # doctest: +SKIP


        :param exc_type: Exception type from the context body, or None on normal exit; ignored by this wrapper.
        :param exc: Exception instance from the body, or None; not inspected or forwarded to close.
        :param traceback: Body exception traceback, or None; not inspected by the wrapper.
        :return: None after close succeeds, without suppressing an exception from the context body.
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
    "ExternalReproductionCommand",
    "DigitalAssetDerivationRegistryAPI",
    "DigitalAssetDerivationRepositoryAPI",
    "DigitalAssetLossAction",
    "DigitalAssetBackupPlan",
    "DigitalAssetReplacementAssessment",
    "DigitalAssetReplacementStatus",
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
    "CompositeDigitalAssetMemberStorageAssessment",
    "CompositeDigitalAssetMemberVerificationReport",
    "CompositeDigitalAssetResolution",
    "CompositeDigitalAssetStorageAssessment",
    "CompositeDigitalAssetVerificationReport",
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
    "DigitalAssetRecordOrder",
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
    "ReproductionNormalizationDigest",
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
    "StorageConvenienceBase",
    "StorageManagementError",
    "StorageManagerAPI",
    "StorageOperationalIssue",
    "StorageOperationalRecoverability",
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
    "StoreBackingReference",
    "StoreConfigurationNotFound",
    "StoragePolicyAssessment",
    "StoreConfiguration",
    "StoreStatusObservation",
    "TopologyRelation",
]
