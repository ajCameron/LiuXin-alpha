"""Read-only TAR archive Store plugin."""

from LiuXin_alpha.storage.api import Location
from LiuXin_alpha.storage.store_backend_plugins.archive_backends import (
    TarReadOnlyStorageBackend,
)


TarReadOnlyStoreLocation = Location


__all__ = ["TarReadOnlyStorageBackend", "TarReadOnlyStoreLocation"]
