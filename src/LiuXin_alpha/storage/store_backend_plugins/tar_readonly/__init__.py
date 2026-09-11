"""
Export the configured TAR read-only member Store through its plugin package.

TarReadOnlyStoreLocation is the shared Location class, retained as a compatibility
alias rather than a format-specific subclass. Driver construction and member
validation belong to TarReadOnlyStorageBackend and its raw archive driver.
"""

from LiuXin_alpha.storage.api import Location
from LiuXin_alpha.storage.store_backend_plugins.archive_backends import (
    TarReadOnlyStorageBackend,
)


TarReadOnlyStoreLocation = Location


__all__ = ["TarReadOnlyStorageBackend", "TarReadOnlyStoreLocation"]
