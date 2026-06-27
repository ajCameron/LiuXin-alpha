"""On-disk calibre-like store plugin."""

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
