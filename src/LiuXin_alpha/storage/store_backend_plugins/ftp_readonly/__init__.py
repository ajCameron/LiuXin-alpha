"""
Expose the configured FTP/FTPS Store and its runtime options.

Locations and observed file metadata use the shared storage API values.
The imported implementation owns backend construction and physical operations.
"""

from .ftp_storage_backend import FtpReadOnlyStorageBackend

__all__ = [
    "FtpReadOnlyStorageBackend",
]
