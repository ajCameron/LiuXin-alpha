"""
Define the shared storage-contract failure hierarchy and Store-facing aliases.

StoreError and the other Store names are identical class objects, so no exception
translation is required between those names and their Storage counterparts.
The legacy LiuXin_alpha.storage.errors hierarchy remains separate.
"""


class StorageError(Exception):
    """
    Base exception for failures reported by the shared raw-driver, Store, and manager contracts.

    Subclasses use ordinary Exception arguments and behavior. This hierarchy is separate from the
    legacy LiuXinException-based storage.errors hierarchy.

    Example:
        >>> isinstance(StorageError("operation failed"), StorageError)
        True
    """


class StorageNotFound(StorageError):
    """
    Report that the requested concrete object or storage target was not found.

    Callers can distinguish reported absence from other access failures by catching this specific
    category.

    Example:
        >>> isinstance(StorageNotFound("operation failed"), StorageError)
        True
    """


class StorageAlreadyExists(StorageError):
    """
    Report a create-only publication collision with an existing target.

    The producer supplies target/context details through ordinary exception arguments.

    Example:
        >>> isinstance(StorageAlreadyExists("operation failed"), StorageError)
        True
    """


class StorageInvalidAddress(StorageError):
    """
    Report an invalid or foreign object address, Location, URI, or read range.

    The producing boundary defines the exact ownership and syntax checks.

    Example:
        >>> isinstance(StorageInvalidAddress("operation failed"), StorageError)
        True
    """


class StorageReadOnly(StorageError):
    """
    Report a refused mutation under endpoint or configured-Store read-only policy.

    The category does not independently establish that reads are currently available.

    Example:
        >>> isinstance(StorageReadOnly("operation failed"), StorageError)
        True
    """


class StorageNoSpace(StorageError):
    """
    Report that capacity or an endpoint space constraint prevents the requested operation.

    The exception carries ordinary arguments rather than a structured capacity measurement.

    Example:
        >>> isinstance(StorageNoSpace("operation failed"), StorageError)
        True
    """


class StoragePreconditionFailed(StorageError):
    """
    Report a failed version or other operation precondition, including observed changes around a
    request.

    The producing backend determines the evidence checked and any publication already performed.

    Example:
        >>> isinstance(StoragePreconditionFailed("operation failed"), StorageError)
        True
    """


class StorageIntegrityError(StorageError):
    """
    Report malformed storage evidence or a mismatch with required bytes, size, digest, or topology.

    Raising this category does not itself provide rollback or identify which validation the producer
    performed.

    Example:
        >>> isinstance(StorageIntegrityError("operation failed"), StorageError)
        True
    """


class StorageUnavailable(StorageError):
    """
    Report that the endpoint or backend process cannot currently complete the requested operation.

    Transport, filesystem, external-tool, and unusable-output failures may use this category with
    producer-specific details.

    Example:
        >>> isinstance(StorageUnavailable("operation failed"), StorageError)
        True
    """


class StoragePermissionDenied(StorageError):
    """
    Report that an access-permission check refused the storage operation.

    The producer supplies the relevant local or remote context through the message.

    Example:
        >>> isinstance(StoragePermissionDenied("operation failed"), StorageError)
        True
    """


class StorageAuthenticationFailed(StorageError):
    """
    Report missing, invalid, expired, or rejected credentials at a storage authentication boundary.

    Example:
        >>> isinstance(StorageAuthenticationFailed("operation failed"), StorageError)
        True
    """


class StorageTimeout(StorageError):
    """
    Report expiry of a backend or caller time allowance.

    The producing implementation defines what the timer covers; this category alone does not bound
    later cleanup or guarantee cancellation.

    Example:
        >>> isinstance(StorageTimeout("operation failed"), StorageError)
        True
    """


class StorageUnsupportedOperation(StorageError):
    """
    Report an operation, object feature, or configured limit that the backend will not support.

    This category also covers policy-bounded archive operations, not only missing methods.

    Example:
        >>> isinstance(StorageUnsupportedOperation("operation failed"), StorageError)
        True
    """


# Store-facing names remain aliases so policy and Store code can use its more
# specific vocabulary without forcing reusable raw drivers to call themselves
# Stores. There is intentionally no second exception hierarchy to translate.
StoreError = StorageError
StoreNotFound = StorageNotFound
StoreAlreadyExists = StorageAlreadyExists
StoreInvalidLocation = StorageInvalidAddress
StoreReadOnly = StorageReadOnly
StoreNoSpace = StorageNoSpace
StorePreconditionFailed = StoragePreconditionFailed
StoreIntegrityError = StorageIntegrityError
StoreUnavailable = StorageUnavailable
StoreUnsupportedOperation = StorageUnsupportedOperation


__all__ = [
    "StorageAlreadyExists",
    "StorageAuthenticationFailed",
    "StorageError",
    "StorageIntegrityError",
    "StorageInvalidAddress",
    "StorageNoSpace",
    "StorageNotFound",
    "StoragePermissionDenied",
    "StoragePreconditionFailed",
    "StorageReadOnly",
    "StorageTimeout",
    "StorageUnavailable",
    "StorageUnsupportedOperation",
    "StoreAlreadyExists",
    "StoreError",
    "StoreIntegrityError",
    "StoreInvalidLocation",
    "StoreNoSpace",
    "StoreNotFound",
    "StorePreconditionFailed",
    "StoreReadOnly",
    "StoreUnavailable",
    "StoreUnsupportedOperation",
]
