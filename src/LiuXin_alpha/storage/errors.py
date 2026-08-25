"""Storage-layer exception declarations.

Storage-specific exceptions should hang off a small, explicit hierarchy so the
caller can either catch one concrete plugin error or one shared storage base.
"""

from __future__ import annotations

from LiuXin_alpha.errors import LiuXinException


class StorageError(LiuXinException):
    """Base class for storage-subsystem exceptions."""


class StorageWriteError(StorageError):
    """Base class for storage write-path failures."""


class StorageImplicitOverwriteError(StorageWriteError):
    """Raised when an implicit write would overwrite an existing target."""


class ManagedDriveImplicitOverwriteError(StorageImplicitOverwriteError):
    """Implicit managed-drive write collided with an incompatible existing path."""


class CalibreLikeImplicitOverwriteError(StorageImplicitOverwriteError):
    """Implicit calibre-like write collided with an incompatible existing path."""


class FlatStoreImplicitOverwriteError(StorageImplicitOverwriteError):
    """Implicit flat-store write collided with an incompatible existing path."""


class SqliteBlobImplicitOverwriteError(StorageImplicitOverwriteError):
    """Implicit SQLite blob write found incompatible bytes at the canonical hash."""


class SquashfsBuildImplicitOverwriteError(StorageImplicitOverwriteError):
    """Implicit SquashFS build write collided with an incompatible staged path."""


class RarBuildImplicitOverwriteError(StorageImplicitOverwriteError):
    """Implicit RAR build write collided with an incompatible staged path."""


__all__ = [
    'StorageError',
    'StorageWriteError',
    'StorageImplicitOverwriteError',
    'ManagedDriveImplicitOverwriteError',
    'CalibreLikeImplicitOverwriteError',
    'FlatStoreImplicitOverwriteError',
    'SqliteBlobImplicitOverwriteError',
    'SquashfsBuildImplicitOverwriteError',
    'RarBuildImplicitOverwriteError',
]
