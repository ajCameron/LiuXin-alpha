"""
Public storage API surface.

Import contracts from this package root to avoid deep import paths.
"""

from __future__ import annotations

from LiuXin_alpha.storage.api.file_api import FileOpenerTypeMixin, FileStatus, SingleFileAPI
from LiuXin_alpha.storage.api.location_api import (
    AsyncNativePretendSyncLocation,
    FileDescriptorOrPath,
    StoreLocationMixinAPI,
    StrOrBytesPath,
    SyncNativePretendAsyncLocation,
)
from LiuXin_alpha.storage.api.modes_api import (
    AsyncBinaryFile,
    AsyncTextFile,
    OpenBinaryMode,
    OpenBinaryModeReading,
    OpenBinaryModeUpdating,
    OpenBinaryModeWriting,
    OpenTextMode,
    OpenTextModeReading,
    OpenTextModeUpdating,
    OpenTextModeWriting,
)
from LiuXin_alpha.storage.api.storage_api import (
    StoreAPI,
    StoreCheckStatus,
    StoreStatus,
    StorageManagerAPI,
)
from LiuXin_alpha.storage.api.storage_manager_api import StorageManagerAPI

__all__ = [
    "AsyncBinaryFile",
    "AsyncNativePretendSyncLocation",
    "AsyncTextFile",
    "FileDescriptorOrPath",
    "FileOpenerTypeMixin",
    "FileStatus",
    "OpenBinaryMode",
    "OpenBinaryModeReading",
    "OpenBinaryModeUpdating",
    "OpenBinaryModeWriting",
    "OpenTextMode",
    "OpenTextModeReading",
    "OpenTextModeUpdating",
    "OpenTextModeWriting",
    "SingleFileAPI",
    "StorageManagerAPI",
    "StoreAPI",
    "StoreCheckStatus",
    "StoreStatus",
    "StorageManagerAPI",
    "StoreLocationMixinAPI",
    "StrOrBytesPath",
    "SyncNativePretendAsyncLocation",
]
