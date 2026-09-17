"""
Expose the ZIP Store that publishes writes by rebuilding its container.

Locations and observed file metadata use the shared storage API values.
The imported implementation owns backend construction and physical operations.
"""

from LiuXin_alpha.storage.store_backend_plugins.archive_backends import (
    ZipWritableStorageBackend,
)

__all__ = [
    "ZipWritableStorageBackend",
]
