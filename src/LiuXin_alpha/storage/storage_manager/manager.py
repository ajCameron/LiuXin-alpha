"""
Compose the repository-neutral storage manager and expose its transient variant.

The orchestrator combines responsibility-specific implementations over shared
state and support hooks. Its public transient class keeps manager metadata in
memory while attached Stores still perform real byte operations. Durable
application managers reuse the composition with persistence-specific hooks.

Five private request/result aliases retain their historical import locations
for existing adapters and serialized journal envelopes. InMemoryStorageManager
is the same class object as TransientStorageManager, not another implementation.
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
    Assemble Store, Asset, Replica, policy, and operational implementations.

    This class adds no methods of its own. Its base order determines Python dispatch and follows the
    manager API's responsibility order; shared support and policy helpers supply the remaining
    mechanics. State creation and Store attachment come from the shared initializer. Durable
    subclasses can replace persistence hooks without changing these public workflows.

    Example:
        >>> issubclass(TransientStorageManager, _StorageManagerOrchestrator)
        True
    """


class TransientStorageManager(_StorageManagerOrchestrator):
    """
    Manage disposable catalogue state while performing real Store operations.

    Each instance owns fresh in-memory records, revisions, and ingest retry state. None is durable
    across a new manager or process, and the transient metadata transaction and journal hooks
    provide no rollback or recovery. Attached Stores can still publish or delete persistent bytes.
    Initialization can start Stores, and leaving the manager context closes attached facades;
    closing does not erase the retained in-memory registries.

    Use the application database-backed StorageManager for durable catalogue ownership. This class
    is not a storage cache and does not join the cache lifecycle. InMemoryStorageManager remains an
    identity alias for older callers.

    Example:
        >>> with TransientStorageManager() as manager:
        ...     tuple(manager.iter_stores())
        ()
        >>> InMemoryStorageManager is TransientStorageManager
        True
    """


# Compatibility for callers written before the persistence boundary was made
# explicit. New code should prefer the honest ``TransientStorageManager`` name.
InMemoryStorageManager = TransientStorageManager


__all__ = [
    "InMemoryStorageManager",
    "TransientStorageManager",
]
