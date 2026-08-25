"""
Typed failures shared by raw storage drivers, Stores, and managers.
"""


class StorageError(Exception):
    """
    Base class for failures produced by the storage contracts.

    Example:
        >>> str(StorageError("backend failure"))
        'backend failure'
    """


class StorageNotFound(StorageError):
    """
    The requested concrete object genuinely does not exist.

    Example:
        >>> isinstance(StorageNotFound("missing.bin"), StorageError)
        True
    """


class StorageAlreadyExists(StorageError):
    """
    A create-only publication collided with an existing object.

    Example:
        >>> str(StorageAlreadyExists("objects/42"))
        'objects/42'
    """


class StorageInvalidAddress(StorageError):
    """
    An object address, Location, URI, or read range is invalid.

    Example:
        >>> isinstance(StorageInvalidAddress("wrong address space"), StorageError)
        True
    """


class StorageReadOnly(StorageError):
    """
    The endpoint is available for reads but refuses mutation.

    Example:
        >>> str(StorageReadOnly("archive is sealed"))
        'archive is sealed'
    """


class StorageNoSpace(StorageError):
    """
    The endpoint cannot accept the write due to capacity.

    Example:
        >>> isinstance(StorageNoSpace("12 bytes required"), StorageError)
        True
    """


class StoragePreconditionFailed(StorageError):
    """
    A version or other race-protection precondition did not hold.

    Example:
        >>> str(StoragePreconditionFailed("expected version v2"))
        'expected version v2'
    """


class StorageIntegrityError(StorageError):
    """
    Observed bytes do not match their required size or digest.

    Example:
        >>> isinstance(StorageIntegrityError("sha256 mismatch"), StorageError)
        True
    """


class StorageUnavailable(StorageError):
    """
    The storage endpoint cannot currently be contacted or accessed.

    Example:
        >>> str(StorageUnavailable("FTP server is offline"))
        'FTP server is offline'
    """


class StoragePermissionDenied(StorageError):
    """
    The authenticated principal lacks permission for the operation.

    Example:
        >>> isinstance(StoragePermissionDenied("read denied"), StorageError)
        True
    """


class StorageAuthenticationFailed(StorageError):
    """
    Credentials were absent, invalid, or no longer accepted.

    Example:
        >>> str(StorageAuthenticationFailed("token expired"))
        'token expired'
    """


class StorageTimeout(StorageError):
    """
    A storage operation exceeded its backend or caller time limit.

    Example:
        >>> isinstance(StorageTimeout("read timed out"), StorageError)
        True
    """


class StorageUnsupportedOperation(StorageError):
    """
    The backend fundamentally cannot provide an operation.

    Example:
        >>> isinstance(StorageUnsupportedOperation("no replacement"), StorageError)
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
