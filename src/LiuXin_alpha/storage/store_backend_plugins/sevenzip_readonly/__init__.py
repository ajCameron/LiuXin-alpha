"""
Export the configured 7z read-only member Store through its plugin package.

SevenZipReadOnlyStoreLocation is the shared Location class, retained as a compatibility
alias rather than a format-specific subclass. Driver construction and member
validation belong to SevenZipReadOnlyStorageBackend and its raw archive driver.
"""

from LiuXin_alpha.storage.api import Location
from LiuXin_alpha.storage.store_backend_plugins.archive_backends import (
    SevenZipReadOnlyStorageBackend,
)


SevenZipReadOnlyStoreLocation = Location


__all__ = ["SevenZipReadOnlyStorageBackend", "SevenZipReadOnlyStoreLocation"]
