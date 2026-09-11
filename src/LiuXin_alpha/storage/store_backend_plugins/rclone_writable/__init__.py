"""
Export the configured writable rclone Store adapter.

    The implementation composes staged raw-driver writes with the shared rclone
    invocation options and read-only adapter infrastructure. Importing it performs
    no remote command or local write-session allocation.
"""

from .rclone_writable_storage_backend import RcloneWritableStorageBackend

__all__ = ["RcloneWritableStorageBackend"]
