"""
Compatibility exports for storage-manager Asset domain values.

The values are organized by responsibility in ``asset_identity``,
``replicas``, ``composites``, and ``resolutions``. This module remains as a
stable import surface for existing callers.
"""

from LiuXin_alpha.storage.api.storage_manager_api.models.asset_identity import (
    DigitalAssetDeclaration,
    DigitalAssetMetadata,
    DigitalAssetRecord,
)
from LiuXin_alpha.storage.api.storage_manager_api.models.composites import (
    CompositeDigitalAssetAvailabilityAssessment,
    CompositeDigitalAssetDeclaration,
    CompositeDigitalAssetMembership,
    CompositeDigitalAssetRecord,
)
from LiuXin_alpha.storage.api.storage_manager_api.models.replicas import (
    DigitalAssetIngestResult,
    DigitalAssetVerificationReport,
    ReplicaDeclaration,
    ReplicaMode,
    ReplicaObservation,
    ReplicaRecord,
    ReplicaRemovalReport,
    ReplicaState,
    ReplicaVerificationReport,
)
from LiuXin_alpha.storage.api.storage_manager_api.models.resolutions import (
    CompositeDigitalAssetMemberResolution,
    DigitalAssetResolution,
    ItemDigitalAssetResolution,
)


__all__ = [
    "CompositeDigitalAssetAvailabilityAssessment",
    "CompositeDigitalAssetDeclaration",
    "CompositeDigitalAssetMemberResolution",
    "CompositeDigitalAssetMembership",
    "CompositeDigitalAssetRecord",
    "DigitalAssetDeclaration",
    "DigitalAssetIngestResult",
    "DigitalAssetMetadata",
    "DigitalAssetRecord",
    "DigitalAssetResolution",
    "DigitalAssetVerificationReport",
    "ItemDigitalAssetResolution",
    "ReplicaDeclaration",
    "ReplicaMode",
    "ReplicaObservation",
    "ReplicaRecord",
    "ReplicaRemovalReport",
    "ReplicaState",
    "ReplicaVerificationReport",
]
