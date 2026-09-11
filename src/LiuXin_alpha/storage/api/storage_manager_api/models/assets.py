"""
Preserve the compatibility import surface for storage-manager Asset domain values.

Identity/metadata, Replica, composite, and resolution values are imported eagerly
from their owning modules and re-exported as the same objects. This module adds no
wrappers, validation, repository access, or physical storage operations. New domain
behavior belongs to the responsibility-specific owners.
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
