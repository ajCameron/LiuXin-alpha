"""
Public storage API surface.

Import contracts from this package root to avoid deep import paths.
"""

from __future__ import annotations

from .file_api import FileOpenerTypeMixin, FileStatus, SingleFileAPI
from .location_api import (
    AsyncNativePretendSyncLocation,
    FileDescriptorOrPath,
    StoreLocationMixinAPI,
    StrOrBytesPath,
    SyncNativePretendAsyncLocation,
)
from .modes_api import (
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
from .storage_api import (
    StoreAPI,
    StoreCheckStatus,
    StoreStatus,
    StorageAPI,
    StorageManagerAPI,
)

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
    "StorageAPI",
    "StoreAPI",
    "StoreCheckStatus",
    "StoreStatus",
    "StorageManagerAPI",
    "StoreLocationMixinAPI",
    "StrOrBytesPath",
    "SyncNativePretendAsyncLocation",
]
