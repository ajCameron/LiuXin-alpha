"""
Composed repository-neutral storage-manager implementation.

Each implementation mixin mirrors one component of ``StorageManagerAPI``.
Shared state and cross-cutting mechanics remain private implementation details;
the composed class is the stable integration seam used by transient and
database-backed managers.
"""

from __future__ import annotations

from LiuXin_alpha.storage.storage_manager.mixins import (
    CompositeDigitalAssetMixin,
    DigitalAssetDerivationRegistryMixin,
    DigitalAssetIngestMixin,
    DigitalAssetRegistryMixin,
    DigitalAssetRetrievalMixin,
    ItemDigitalAssetLinkMixin,
    ReplicaLifecycleMixin,
    StorageOperationalStatusMixin,
    StoragePolicyMixin,
    StorageReconciliationMixin,
    StorageRouterMixin,
    StoreAdministrationMixin,
)
from LiuXin_alpha.storage.storage_manager.mixins import _types as _manager_types
from LiuXin_alpha.storage.storage_manager.mixins._policy_support import (
    _StorageManagerPolicySupportMixin,
)
from LiuXin_alpha.storage.storage_manager.mixins._support import (
    _StorageManagerSupportMixin,
)

# Private compatibility exports consumed by the database-backed manager and by
# durable journal envelopes written before the implementation was decomposed.
_AdoptIngestRequest = _manager_types._AdoptIngestRequest
_IdentifiedStreamIngestRequest = _manager_types._IdentifiedStreamIngestRequest
_IngestOperation = _manager_types._IngestOperation
_StoreObjectIngestRequest = _manager_types._StoreObjectIngestRequest
_StreamIngestRequest = _manager_types._StreamIngestRequest


class _StorageManagerOrchestrator(
    StoreAdministrationMixin,
    StorageRouterMixin,
    DigitalAssetRegistryMixin,
    DigitalAssetIngestMixin,
    DigitalAssetRetrievalMixin,
    ReplicaLifecycleMixin,
    ItemDigitalAssetLinkMixin,
    CompositeDigitalAssetMixin,
    DigitalAssetDerivationRegistryMixin,
    StoragePolicyMixin,
    StorageReconciliationMixin,
    StorageOperationalStatusMixin,
    _StorageManagerSupportMixin,
    _StorageManagerPolicySupportMixin,
):
    """
    Compose the repository-neutral storage workflow implementation.

    Mixin order mirrors the public ``StorageManagerAPI`` component order so
    readers can move between contract and implementation predictably.  The
    class adds no behaviour of its own: transient and database-backed managers
    supply state and persistence boundaries through the shared support hooks.
    """


class TransientStorageManager(_StorageManagerOrchestrator):
    """
    Disposable manager state for focused tests and one-shot work.

    Store publication is real, but manager-owned records disappear with the
    process. Applications should use the database-backed ``StorageManager``;
    this implementation is not a cache and does not participate in LiuXin's
    cache lifecycle.
    """


# Compatibility for callers written before the persistence boundary was made
# explicit. New code should prefer the honest ``TransientStorageManager`` name.
InMemoryStorageManager = TransientStorageManager


__all__ = [
    "InMemoryStorageManager",
    "TransientStorageManager",
]
