"""Storage API contracts for concrete asset replicas."""

from __future__ import annotations

import abc


class AssetReplicaIdentityAPI(abc.ABC):
    """Represents one concrete copy of a digital asset on storage."""


class AssetReplicaMetadataAPI(abc.ABC):
    """Represents storage-facing metadata for one asset replica."""


__all__ = ["AssetReplicaIdentityAPI", "AssetReplicaMetadataAPI"]
