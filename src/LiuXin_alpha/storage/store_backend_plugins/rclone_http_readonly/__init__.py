"""
Expose the read-only rclone Store, runtime options, and request-rate lookup.

Locations and observed file metadata use the shared storage API values.
The imported implementation owns backend construction and physical operations.
"""

from .rclone_http_storage_backend import (
    RcloneBackendOptions,
    RcloneHttpReadOnlyStorageBackend,
    get_default_rclone_http_requests_per_hour,
)

__all__ = [
    "RcloneBackendOptions",
    "RcloneHttpReadOnlyStorageBackend",
    "get_default_rclone_http_requests_per_hour",
]
