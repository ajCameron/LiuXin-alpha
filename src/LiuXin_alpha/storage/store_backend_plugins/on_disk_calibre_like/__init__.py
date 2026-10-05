"""
Expose the local Store that allocates Calibre-like paths.

Locations and observed file metadata use the shared storage API values.
The imported implementation owns backend construction and physical operations.
"""

from __future__ import annotations

from LiuXin_alpha.storage.store_backend_plugins.on_disk_calibre_like.on_disk_calibre_like_storage_backend import (
    OnDiskCalibreLikeStorageBackend,
)

__all__ = [
    "OnDiskCalibreLikeStorageBackend",
]
