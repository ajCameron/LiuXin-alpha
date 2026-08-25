"""Read-only RAR archive Store plugin."""

from LiuXin_alpha.storage.api import Location
from LiuXin_alpha.storage.store_backend_plugins.archive_backends import (
    RarReadOnlyStorageBackend,
)


RarReadOnlyStoreLocation = Location


__all__ = ["RarReadOnlyStorageBackend", "RarReadOnlyStoreLocation"]
