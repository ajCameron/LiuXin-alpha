"""
Lazily expose the SquashFS read-only Store, Location alias, and manifest builder.

The standalone manifest builder writes a destination archive directly; the
separate squashfs_build plugin provides staged candidate validation and sealing.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = [
    "SquashfsManifestEntry",
    "SquashfsBuildReport",
    "build_squashfs_from_manifest",
    "load_manifest_entries",
    "SquashfsReadOnlyStoreLocation",
    "SquashfsReadOnlyStorageBackend",
]


def __getattr__(name: str) -> Any:
    """
    Resolve supported exports from their owning modules on demand.

    The hook does not bind resolved objects into this package's globals; ordinary import caching
    applies to the imported modules. Unknown names raise AttributeError, and import failures
    propagate.

    Example:
        >>> __getattr__("SquashfsReadOnlyStoreLocation").__name__
        'Location'


    :param name: Requested package attribute name.
    :return: The requested builder value/function, Location alias, or Store class.
    """
    if name in {"SquashfsManifestEntry", "SquashfsBuildReport", "build_squashfs_from_manifest", "load_manifest_entries"}:
        module = import_module("LiuXin_alpha.storage.store_backend_plugins.squashfs_readonly.squashfs_manifest_builder")
        return getattr(module, name)
    if name == "SquashfsReadOnlyStoreLocation":
        return import_module("LiuXin_alpha.storage.store_backend_plugins.squashfs_readonly.squashfs_readonly_location").SquashfsReadOnlyStoreLocation
    if name == "SquashfsReadOnlyStorageBackend":
        return import_module("LiuXin_alpha.storage.store_backend_plugins.squashfs_readonly.squashfs_readonly_storage_backend").SquashfsReadOnlyStorageBackend
    raise AttributeError(name)
