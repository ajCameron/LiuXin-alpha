"""
Build-once RAR archive Store plugin.
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
