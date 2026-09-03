"""Writable staging plugin for building SquashFS archives.

This plugin is intentionally *not* a normal mutable store. It is a staging
surface for collecting a finite pack of files, then sealing them into one
SquashFS archive. After a build, callers should generally switch to the paired
`SquashfsReadOnlyStorageBackend` for archive access.
"""

from .squashfs_build_location import SquashfsBuildStoreLocation
from .squashfs_build_storage_backend import SquashfsBuildStorageBackend

__all__ = [
    "SquashfsBuildStoreLocation",
    "SquashfsBuildStorageBackend",
]
