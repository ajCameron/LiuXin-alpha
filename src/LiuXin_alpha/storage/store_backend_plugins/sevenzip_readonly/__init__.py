"""
Expose the read-only 7z member Store.

Locations and observed file metadata use the shared storage API values.
The imported implementation owns backend construction and physical operations.
"""

from LiuXin_alpha.storage.store_backend_plugins.archive_backends import (
    SevenZipReadOnlyStorageBackend,
)

__all__ = [
    "SevenZipReadOnlyStorageBackend",
]
