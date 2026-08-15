"""
Typed failures produced by LiuXin-aware storage-manager operations.
"""

from LiuXin_alpha.storage.api.errors import StorageError


class StorageManagementError(StorageError):
    """
    Base class for failures involving managed storage domain state.

    Example:
        >>> isinstance(StorageManagementError("invalid state"), StorageError)
        True
    """


class StoreConfigurationNotFound(StorageManagementError):
    """
    The requested Store is not configured in the storage manager.

    This is distinct from ``StoreNotFound``, which means that a concrete
    object is absent from a known Store, and ``StoreUnavailable``, which means
    that a configured Store cannot currently be reached.

    Example:
        >>> isinstance(
        ...     StoreConfigurationNotFound("unknown Store"), StorageError,
        ... )
        True
    """


class DigitalAssetNotFound(StorageManagementError):
    """
    The requested Digital Asset is absent from the asset repository.

    This is distinct from ``StoreNotFound``, which means that bytes are absent
    at one concrete ``Location``.

    Example:
        >>> str(DigitalAssetNotFound("asset 7"))
        'asset 7'
    """


class ReplicaNotFound(StorageManagementError):
    """
    The requested Replica is absent from the replica repository.

    Example:
        >>> isinstance(ReplicaNotFound("replica 12"), StorageManagementError)
        True
    """


class NoReadableReplica(StorageManagementError):
    """
    A Digital Asset is known but currently has no readable Replica.

    Example:
        >>> str(NoReadableReplica("asset 7 is offline"))
        'asset 7 is offline'
    """


class CompositeDigitalAssetNotFound(StorageManagementError):
    """
    The requested Composite Digital Asset is not registered.

    Example:
        >>> isinstance(CompositeDigitalAssetNotFound("composite 3"), StorageError)
        True
    """


class CompositeDigitalAssetIncomplete(StorageManagementError):
    """
    A Composite Digital Asset cannot resolve all required members.

    Example:
        >>> str(CompositeDigitalAssetIncomplete("member 8 is unavailable"))
        'member 8 is unavailable'
    """


class DigitalAssetDerivationNotFound(StorageManagementError):
    """
    The requested Digital Asset derivation is absent from the provenance
    repository.

    Example:
        >>> isinstance(
        ...     DigitalAssetDerivationNotFound("derivation 11"), StorageError,
        ... )
        True
    """


class StoragePolicyUnsatisfied(StorageManagementError):
    """
    Required storage policy cannot be satisfied by current placement.

    Example:
        >>> isinstance(StoragePolicyUnsatisfied("no second failure domain"), StorageError)
        True
    """


class StoreReconciliationPlanStale(StorageManagementError):
    """
    A reconciliation plan no longer describes current repository state.

    Example:
        >>> str(StoreReconciliationPlanStale("replica revision changed"))
        'replica revision changed'
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
