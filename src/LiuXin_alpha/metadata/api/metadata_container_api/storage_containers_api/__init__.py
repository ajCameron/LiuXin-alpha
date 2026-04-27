"""Public storage-container API surface.

Storage entities are not WEMI-attached metadata families, so they live in their
own package. This module exists to make their public export surface explicit and
consistent with the rest of the metadata API package tree.
"""

from __future__ import annotations

from LiuXin_alpha.metadata.api.metadata_container_api.storage_containers_api.asset_replica_api import (
    AssetReplicaIdentityAPI,
    AssetReplicaMetadataAPI,
)
from LiuXin_alpha.metadata.api.metadata_container_api.storage_containers_api.digital_asset_api import (
    DigitalAssetIdentityAPI,
    DigitalAssetMetadataAPI,
)

__all__ = [
    "AssetReplicaIdentityAPI",
    "AssetReplicaMetadataAPI",
    "DigitalAssetIdentityAPI",
    "DigitalAssetMetadataAPI",
]
