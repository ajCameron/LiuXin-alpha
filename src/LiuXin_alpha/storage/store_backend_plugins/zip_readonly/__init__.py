"""
Export the configured ZIP read-only member Store through its plugin package.

ZipReadOnlyStoreLocation is the shared Location class, retained as a compatibility
alias rather than a format-specific subclass. Driver construction and member
validation belong to ZipReadOnlyStorageBackend and its raw archive driver.
"""

from LiuXin_alpha.storage.api import Location
from LiuXin_alpha.storage.store_backend_plugins.archive_backends import (
    ZipReadOnlyStorageBackend,
)


ZipReadOnlyStoreLocation = Location


__all__ = ["ZipReadOnlyStorageBackend", "ZipReadOnlyStoreLocation"]
