
"""
On-disk calibre-like store plugin.

Layout strategy:
- top-level folder by author or author-combo
- second-level folder by title with id suffix
- files inside as format variants
"""

from __future__ import annotations

from LiuXin_alpha.storage.store_backend_plugins.on_disk_calibre_like.on_disk_calibre_like_location import (
    OnDiskCalibreLikeStoreLocation,
)
from LiuXin_alpha.storage.store_backend_plugins.on_disk_calibre_like.on_disk_calibre_like_single_file import (
    OnDiskCalibreLikeSingleFile,
)
from LiuXin_alpha.storage.store_backend_plugins.on_disk_calibre_like.on_disk_calibre_like_storage_backend import (
    OnDiskCalibreLikeStorageBackend,
)

__all__ = [
    "OnDiskCalibreLikeSingleFile",
    "OnDiskCalibreLikeStoreLocation",
    "OnDiskCalibreLikeStorageBackend",
]
