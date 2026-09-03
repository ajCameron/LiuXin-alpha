"""Writable ZIP archive Store plugin."""

from LiuXin_alpha.storage.api import Location
from LiuXin_alpha.storage.store_backend_plugins.archive_backends import (
    ZipWritableStorageBackend,
)


ZipWritableStoreLocation = Location


__all__ = ["ZipWritableStorageBackend", "ZipWritableStoreLocation"]
