"""
Export the configured read-only rclone Store, runtime options, and Location compatibility name.

    The historical HTTP package also accepts named and other rclone filesystem roots.
    Exported objects are defined in the adapter modules; importing this package does
    not start rclone or probe a remote.
"""

from .rclone_http_storage_backend import (
    RcloneBackendOptions,
    RcloneHttpReadOnlyStorageBackend,
    get_default_rclone_http_requests_per_hour,
)
from .rclone_http_location import RcloneHttpReadOnlyStoreLocation

__all__ = [
    "RcloneBackendOptions",
    "RcloneHttpReadOnlyStorageBackend",
    "RcloneHttpReadOnlyStoreLocation",
    "get_default_rclone_http_requests_per_hour",
]
