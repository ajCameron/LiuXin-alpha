"""
Re-export the unmanaged single-file facade through its shorter legacy module path.

The exported class is the same canonical Store/Location wrapper, including its
read-only behavior and file-URI-derived identity.
"""

from __future__ import annotations

from LiuXin_alpha.storage.store_backend_plugins.on_disk_existing_unmanaged_drive.on_disk_existing_unmanaged_drive_single_file import (
    OnDiskUnmanagedSingleFile,
)

__all__ = ["OnDiskUnmanagedSingleFile"]
