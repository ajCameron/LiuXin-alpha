"""
Expose the Calibre-like placement Store and its common Location alias.

Construction, filesystem publication, and optional database updates remain with
the backend class; importing this package performs no Store creation.
"""

from __future__ import annotations

from LiuXin_alpha.storage.store_backend_plugins.on_disk_calibre_like.on_disk_calibre_like_location import (
    OnDiskCalibreLikeStoreLocation,
)
from LiuXin_alpha.storage.store_backend_plugins.on_disk_calibre_like.on_disk_calibre_like_storage_backend import (
    OnDiskCalibreLikeStorageBackend,
)

__all__ = [
    "OnDiskCalibreLikeStoreLocation",
    "OnDiskCalibreLikeStorageBackend",
]
