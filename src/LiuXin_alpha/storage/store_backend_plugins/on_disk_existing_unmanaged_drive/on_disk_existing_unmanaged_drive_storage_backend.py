"""
On-disk unmanaged drive store backend (read-only).

This backend indexes and reads files from a local directory while refusing
mutating store-level operations.
"""

from __future__ import annotations

from typing import Optional

from LiuXin_alpha.storage.api.file_api import SingleFileAPI
from LiuXin_alpha.storage.api import StoreStatus
from LiuXin_alpha.storage.store_backend_plugins.on_disk_existing_managed_drive.on_disk_existing_managed_drive_storage_backend import (
    OnDiskExistingManagedStorageBackend,
)
from LiuXin_alpha.storage.store_backend_plugins.on_disk_existing_unmanaged_drive.on_disk_existing_unmanaged_drive_location import (
    OnDiskUnmanagedStoreLocation,
)
from LiuXin_alpha.storage.store_backend_plugins.on_disk_existing_unmanaged_drive.on_disk_existing_unmanaged_drive_single_file import (
    OnDiskUnmanagedSingleFile,
)


class OnDiskUnmanagedStorageBackend(OnDiskExistingManagedStorageBackend):
    """
    Read-only view of files already present on a local drive.
    """

    location_cls = OnDiskUnmanagedStoreLocation
    single_file_cls = OnDiskUnmanagedSingleFile

    def __init__(self, url: str, name: Optional[str] = None, uuid: Optional[str] = None) -> None:
        super().__init__(url=url, name=name, uuid=uuid)

    def self_test(self) -> StoreStatus:
        status = super().self_test()
        status.check_status.write = False
        status.checked = bool(
            status.check_status.store_marker_file and status.check_status.read and status.check_status.sundry
        )
        status.good = status.checked
        status.details["mode"] = "read_only"
        self._cached_status = status
        return status

    def add_file(self, file_bytes: bytes, *, metadata=None) -> SingleFileAPI:
        raise PermissionError("OnDiskUnmanagedStorageBackend is read-only.")

    def delete_file(self, file_url: str) -> bool:
        raise PermissionError("OnDiskUnmanagedStorageBackend is read-only.")
