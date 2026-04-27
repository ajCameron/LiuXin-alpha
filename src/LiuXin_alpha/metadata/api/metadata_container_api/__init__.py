"""Canonical public metadata-container API surface.

This package re-exports metadata-container contracts only. Concrete
implementations live under ``LiuXin_alpha.metadata.containers``.
"""

from __future__ import annotations

from LiuXin_alpha.metadata.api.metadata_container_api.storage_containers_api import (
    AssetReplicaIdentityAPI,
    AssetReplicaMetadataAPI,
    DigitalAssetIdentityAPI,
    DigitalAssetMetadataAPI,
)
from LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api import (
    AgentIdentityAPI,
    AgentProfileAPI,
    ExpressionIdentityAPI,
    ExpressionIdentityPropertiesAPI,
    ExpressionMetadataAPI,
    ExpressionRelationLink,
    ExpressionStorageHints,
    ItemIdentityAPI,
    ItemIdentityPropertiesAPI,
    ItemMetadataAPI,
    ItemRelationLink,
    ItemStorageHints,
    ManifestationIdentityAPI,
    ManifestationIdentityPropertiesAPI,
    ManifestationMetadataAPI,
    ManifestationRelationLink,
    ManifestationStorageHints,
    WorkIdentityAPI,
    WorkIdentityPropertiesAPI,
    WorkMetadataAPI,
    WorkRelationLink,
    WorkStorageHints,
)

__all__ = [
    "AssetReplicaIdentityAPI",
    "AssetReplicaMetadataAPI",
    "DigitalAssetIdentityAPI",
    "DigitalAssetMetadataAPI",
    "AgentIdentityAPI",
    "AgentProfileAPI",
    "ExpressionIdentityAPI",
    "ExpressionIdentityPropertiesAPI",
    "ExpressionMetadataAPI",
    "ExpressionRelationLink",
    "ExpressionStorageHints",
    "ItemIdentityAPI",
    "ItemIdentityPropertiesAPI",
    "ItemMetadataAPI",
    "ItemRelationLink",
    "ItemStorageHints",
    "ManifestationIdentityAPI",
    "ManifestationIdentityPropertiesAPI",
    "ManifestationMetadataAPI",
    "ManifestationRelationLink",
    "ManifestationStorageHints",
    "WorkIdentityAPI",
    "WorkIdentityPropertiesAPI",
    "WorkMetadataAPI",
    "WorkRelationLink",
    "WorkStorageHints",
]
