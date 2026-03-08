"""
Read-only SquashFS store backend.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = [
    "SquashfsManifestEntry",
    "SquashfsBuildReport",
    "build_squashfs_from_manifest",
    "load_manifest_entries",
    "SquashfsReadOnlySingleFile",
    "SquashfsReadOnlyStorageBackend",
]


def __getattr__(name: str) -> Any:
    if name in {"SquashfsManifestEntry", "SquashfsBuildReport", "build_squashfs_from_manifest", "load_manifest_entries"}:
        module = import_module(
            "LiuXin_alpha.storage.store_backend_plugins.squashfs_readonly.squashfs_manifest_builder"
        )
        return getattr(module, name)
    if name == "SquashfsReadOnlySingleFile":
        return import_module(
            "LiuXin_alpha.storage.store_backend_plugins.squashfs_readonly.squashfs_readonly_single_file"
        ).SquashfsReadOnlySingleFile
    if name == "SquashfsReadOnlyStorageBackend":
        return import_module(
            "LiuXin_alpha.storage.store_backend_plugins.squashfs_readonly.squashfs_readonly_storage_backend"
        ).SquashfsReadOnlyStorageBackend
    raise AttributeError(name)
