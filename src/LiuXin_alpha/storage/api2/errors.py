"""Typed failures shared by transactional storage backends and managers."""


class StoreError(Exception):
    """Base class for errors produced by the storage contract.

    Example:
        >>> str(StoreError("backend failure"))
        'backend failure'
    """


class StoreNotFound(StoreError):
    """The requested concrete object genuinely does not exist.

    Example:
        >>> isinstance(StoreNotFound("missing.bin"), StoreError)
        True
    """


class StoreAlreadyExists(StoreError):
    """A create-only publication collided with an existing object.

    Example:
        >>> str(StoreAlreadyExists("objects/42"))
        'objects/42'
    """


class StoreInvalidLocation(StoreError):
    """A Location is malformed or does not belong to this store.

    Example:
        >>> error = StoreInvalidLocation("wrong store reference")
        >>> isinstance(error, StoreError)
        True
    """


class StoreReadOnly(StoreError):
    """The store is available for reads but refuses mutation.

    Example:
        >>> str(StoreReadOnly("archive is sealed"))
        'archive is sealed'
    """


class StoreNoSpace(StoreError):
    """The store cannot accept the requested write due to capacity.

    Example:
        >>> isinstance(StoreNoSpace("12 bytes required"), StoreError)
        True
    """


class StorePreconditionFailed(StoreError):
    """A version or other race-protection precondition did not hold.

    Example:
        >>> str(StorePreconditionFailed("expected version v2"))
        'expected version v2'
    """


class StoreIntegrityError(StoreError):
    """Observed bytes do not match their required size or digest.

    Example:
        >>> error = StoreIntegrityError("sha256 mismatch")
        >>> isinstance(error, StoreError)
        True
    """


class StoreUnavailable(StoreError):
    """The store cannot currently be contacted or accessed.

    Example:
        >>> str(StoreUnavailable("FTP server is offline"))
        'FTP server is offline'
    """


class StoreUnsupportedOperation(StoreError):
    """The backend fundamentally cannot provide the requested operation.

    Example:
        >>> error = StoreUnsupportedOperation("immutable store cannot replace")
        >>> isinstance(error, StoreError)
        True
    """


__all__ = [
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
