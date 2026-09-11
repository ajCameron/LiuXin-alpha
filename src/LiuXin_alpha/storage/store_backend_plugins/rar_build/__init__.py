"""
Export the build-once RAR Store and its creator timeout/compression defaults.

RarBuildStoreLocation remains an alias of the shared Location class, not a separate
address type. The backend owns filesystem staging and explicit sealing; its returned
read-only Store addresses the published archive.
"""

from LiuXin_alpha.storage.api import Location
from LiuXin_alpha.storage.store_backend_plugins.rar_build.rar_build_storage_backend import (
    DEFAULT_RAR_BUILD_TIMEOUT_S,
    DEFAULT_RAR_COMPRESSION_LEVEL,
    RarBuildStorageBackend,
)


RarBuildStoreLocation = Location


__all__ = [
    "DEFAULT_RAR_BUILD_TIMEOUT_S",
    "DEFAULT_RAR_COMPRESSION_LEVEL",
    "RarBuildStorageBackend",
    "RarBuildStoreLocation",
]
