"""
Expose the SquashFS staging Store and its common Location compatibility alias.

Committed filesystem staging becomes a validated archive through explicit seal.
Use the returned read-only Store for archive access after successful sealing;
the builder retains its staging view and refuses new leased mutations.
"""

from .squashfs_build_location import SquashfsBuildStoreLocation
from .squashfs_build_storage_backend import SquashfsBuildStorageBackend

__all__ = [
    "SquashfsBuildStoreLocation",
    "SquashfsBuildStorageBackend",
]
