"""
Export the configured ZIP whole-container rebuild Store through its plugin package.

ZipWritableStoreLocation is the shared Location class, retained as a compatibility
alias rather than a format-specific subclass. Driver construction and member
validation belong to ZipWritableStorageBackend and its raw archive driver.
"""

from LiuXin_alpha.storage.api import Location
from LiuXin_alpha.storage.store_backend_plugins.archive_backends import (
    ZipWritableStorageBackend,
)


ZipWritableStoreLocation = Location


__all__ = ["ZipWritableStorageBackend", "ZipWritableStoreLocation"]
