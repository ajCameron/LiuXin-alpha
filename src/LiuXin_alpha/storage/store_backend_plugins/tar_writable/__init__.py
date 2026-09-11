"""
Export the configured TAR whole-container rebuild Store through its plugin package.

TarWritableStoreLocation is the shared Location class, retained as a compatibility
alias rather than a format-specific subclass. Driver construction and member
validation belong to TarWritableStorageBackend and its raw archive driver.
"""

from LiuXin_alpha.storage.api import Location
from LiuXin_alpha.storage.store_backend_plugins.archive_backends import (
    TarWritableStorageBackend,
)


TarWritableStoreLocation = Location


__all__ = ["TarWritableStorageBackend", "TarWritableStoreLocation"]
