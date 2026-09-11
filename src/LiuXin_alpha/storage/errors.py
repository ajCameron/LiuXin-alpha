"""
Retain the legacy LiuXinException-based storage write-error hierarchy.

These classes preserve plugin-specific implicit-overwrite categories. They are
separate from the shared storage.api.errors hierarchy and are not aliases of its
StorageError or StoreError. No constructors or translation behavior are added.
"""

from __future__ import annotations

from LiuXin_alpha.errors import LiuXinException


class StorageError(LiuXinException):
    """
    Base for legacy storage-subsystem failures under LiuXinException.

    This class is distinct from storage.api.StorageError; catching the shared API base does not
    catch this separate legacy hierarchy.

    Example:
        >>> isinstance(StorageError("implicit destination collision"), StorageError)
        True
    """


class StorageWriteError(StorageError):
    """
    Group legacy storage write failures under the legacy StorageError base.

    Example:
        >>> isinstance(StorageWriteError("implicit destination collision"), StorageError)
        True
    """


class StorageImplicitOverwriteError(StorageWriteError):
    """
    Classify a legacy implicit destination collision or incompatible existing content.

    Individual callers decide when this category applies; it adds no automatic collision handling or
    recovery.

    Example:
        >>> isinstance(StorageImplicitOverwriteError("implicit destination collision"), StorageError)
        True
    """


class ManagedDriveImplicitOverwriteError(StorageImplicitOverwriteError):
    """
    Retain the legacy category for an incompatible implicit managed-drive destination.

    Current generic Store collision policy may instead report the shared API collision type.

    Example:
        >>> isinstance(ManagedDriveImplicitOverwriteError("implicit destination collision"), StorageError)
        True
    """


class CalibreLikeImplicitOverwriteError(StorageImplicitOverwriteError):
    """
    Retain the legacy category for an incompatible implicit Calibre-like destination.

    The class does not override current Store collision policy or adapt shared API errors.

    Example:
        >>> isinstance(CalibreLikeImplicitOverwriteError("implicit destination collision"), StorageError)
        True
    """


class FlatStoreImplicitOverwriteError(StorageImplicitOverwriteError):
    """
    Retain the legacy category for an incompatible implicit flat-store destination.

    Constructing or catching this class does not imply all current flat-store collisions use it.

    Example:
        >>> isinstance(FlatStoreImplicitOverwriteError("implicit destination collision"), StorageError)
        True
    """


class SqliteBlobImplicitOverwriteError(StorageImplicitOverwriteError):
    """
    Retain the legacy category for incompatible bytes at an implicit SQLite blob destination.

    Example:
        >>> isinstance(SqliteBlobImplicitOverwriteError("implicit destination collision"), StorageError)
        True
    """


class SquashfsBuildImplicitOverwriteError(StorageImplicitOverwriteError):
    """
    Report an incompatible implicit SquashFS staging destination through the legacy write hierarchy.

    The builder raises this for selected deduplication/collision paths; other explicitly targeted
    writes can use shared API errors.

    Example:
        >>> isinstance(SquashfsBuildImplicitOverwriteError("implicit destination collision"), StorageError)
        True
    """


class RarBuildImplicitOverwriteError(StorageImplicitOverwriteError):
    """
    Report an incompatible implicit RAR staging destination through the legacy write hierarchy.

    Example:
        >>> isinstance(RarBuildImplicitOverwriteError("implicit destination collision"), StorageError)
        True
    """


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
