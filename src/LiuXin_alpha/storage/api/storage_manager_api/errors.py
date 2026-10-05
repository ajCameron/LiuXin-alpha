"""
Distinguish managed storage catalogue, selection, policy, and stale-plan failures.

All categories derive from the shared storage API StorageError hierarchy and use
ordinary Exception arguments. They do not add persistence or recovery behavior.
"""

from LiuXin_alpha.storage.api.errors import StorageError


class StorageManagementError(StorageError):
    """
    Group failures involving managed storage catalogue, policy, or workflow state under the shared
    StorageError hierarchy. Subclasses retain ordinary Exception arguments without automatic
    translation or recovery.

    Example:
        >>> isinstance(StorageManagementError("operation rejected"), StorageError)
        True
    """


class StoreConfigurationNotFound(StorageManagementError):
    """
    Report an unknown configured Store identity at the manager boundary. This category differs from
    a missing concrete object and from a known Store that is currently unavailable.

    Example:
        >>> isinstance(StoreConfigurationNotFound("operation rejected"), StorageError)
        True
    """


class DigitalAssetNotFound(StorageManagementError):
    """
    Report an Asset identity absent from the managed catalogue. This does not describe whether bytes
    exist at an unrelated concrete Location.

    Example:
        >>> isinstance(DigitalAssetNotFound("operation rejected"), StorageError)
        True
    """


class ReplicaNotFound(StorageManagementError):
    """
    Report a Replica identity absent from the managed catalogue. A registered Replica whose bytes
    are missing or unavailable is a separate state or operation failure.

    Example:
        >>> isinstance(ReplicaNotFound("operation rejected"), StorageError)
        True
    """


class NoReadableReplica(StorageManagementError):
    """
    Report that selection found no eligible readable Replica under the request's mode, Store, or
    verification requirements. Other copies may exist but fail those requirements; this category
    does not assert physical absence everywhere.

    Example:
        >>> isinstance(NoReadableReplica("operation rejected"), StorageError)
        True
    """


class CompositeDigitalAssetNotFound(StorageManagementError):
    """
    Report an unregistered Composite Digital Asset identity, distinct from an existing composite
    with unresolved members.

    Example:
        >>> isinstance(CompositeDigitalAssetNotFound("operation rejected"), StorageError)
        True
    """


class CompositeDigitalAssetIncomplete(StorageManagementError):
    """
    Report that resolution of a known Composite Digital Asset could not satisfy its required-member
    contract. The producing operation determines which member failures are aggregated or allowed.

    Example:
        >>> isinstance(CompositeDigitalAssetIncomplete("operation rejected"), StorageError)
        True
    """


class DigitalAssetDerivationNotFound(StorageManagementError):
    """
    Report a derivation identity absent from the managed provenance repository. The exception adds
    no attempt to reconstruct or replay provenance.

    Example:
        >>> isinstance(DigitalAssetDerivationNotFound("operation rejected"), StorageError)
        True
    """


class StoragePolicyUnsatisfied(StorageManagementError):
    """
    Report a requested storage policy that cannot be satisfied by the available placement or
    recreation evidence. The producing operation supplies the failed requirement and owns any prior
    effects.

    Example:
        >>> isinstance(StoragePolicyUnsatisfied("operation rejected"), StorageError)
        True
    """


class StoreReconciliationPlanStale(StorageManagementError):
    """
    Report that a reconciliation plan's captured repository state or revisions no longer match the
    state required for application. Callers must obtain current evidence rather than assume that the
    old plan was applied.

    Example:
        >>> isinstance(StoreReconciliationPlanStale("operation rejected"), StorageError)
        True
    """


__all__ = [
    "DigitalAssetDerivationNotFound",
    "CompositeDigitalAssetIncomplete",
    "CompositeDigitalAssetNotFound",
    "DigitalAssetNotFound",
    "NoReadableReplica",
    "StoragePolicyUnsatisfied",
    "StoreReconciliationPlanStale",
    "ReplicaNotFound",
    "StoreConfigurationNotFound",
    "StorageManagementError",
]
