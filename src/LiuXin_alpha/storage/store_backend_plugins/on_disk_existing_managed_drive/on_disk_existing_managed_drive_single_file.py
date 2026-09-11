"""
Retain the managed single-file class name over the read-only unmanaged facade.

This historical subclass adds no overrides, managed allocation, or write methods;
its inherited constructor still creates an OnDiskUnmanagedStorageBackend.
"""

from __future__ import annotations

from LiuXin_alpha.storage.store_backend_plugins.on_disk_existing_unmanaged_drive.on_disk_existing_unmanaged_drive_single_file import (
    OnDiskUnmanagedSingleFile,
)


class OnDiskExistingManagedSingleFile(OnDiskUnmanagedSingleFile):
    """
    Preserve the legacy managed single-file type name with inherited read-only behavior.

    All construction, stable URI-derived identity, stat, range reads, and closure come from
    OnDiskUnmanagedSingleFile. The class does not wrap a managed writable Store.

    Example:
        >>> file = OnDiskExistingManagedSingleFile("book.txt")  # doctest: +SKIP
        >>> file.store.configuration.read_only  # doctest: +SKIP
        True
    """
