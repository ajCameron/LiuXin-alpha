"""Writable TAR archive Store plugin."""

from LiuXin_alpha.storage.api import Location
from LiuXin_alpha.storage.store_backend_plugins.archive_backends import (
    TarWritableStorageBackend,
)


TarWritableStoreLocation = Location


__all__ = ["TarWritableStorageBackend", "TarWritableStoreLocation"]
