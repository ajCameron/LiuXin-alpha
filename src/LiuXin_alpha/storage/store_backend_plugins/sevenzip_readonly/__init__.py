"""
Read-only 7z archive Store plugin.
"""

from LiuXin_alpha.storage.api import Location
from LiuXin_alpha.storage.store_backend_plugins.archive_backends import (
    SevenZipReadOnlyStorageBackend,
)


SevenZipReadOnlyStoreLocation = Location


__all__ = ["SevenZipReadOnlyStorageBackend", "SevenZipReadOnlyStoreLocation"]
