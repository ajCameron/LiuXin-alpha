"""Writable staging plugin for building SquashFS archives.

This plugin is intentionally *not* a normal mutable store. It is a staging
surface for collecting a finite pack of files, then sealing them into one
SquashFS archive. After a build, callers should generally switch to the paired
`SquashfsReadOnlyStorageBackend` for archive access.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = [
    "SquashfsBuildStoreLocation",
    "SquashfsBuildStorageBackend",
]


def __getattr__(name: str) -> Any:
    if name == "SquashfsBuildStoreLocation":
        return import_module(
            "LiuXin_alpha.storage.store_backend_plugins.squashfs_build.squashfs_build_location"
        ).SquashfsBuildStoreLocation
    if name == "SquashfsBuildStorageBackend":
        return import_module(
            "LiuXin_alpha.storage.store_backend_plugins.squashfs_build.squashfs_build_storage_backend"
        ).SquashfsBuildStorageBackend
    raise AttributeError(name)
