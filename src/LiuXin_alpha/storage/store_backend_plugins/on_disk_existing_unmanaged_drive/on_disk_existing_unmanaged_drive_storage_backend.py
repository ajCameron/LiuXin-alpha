"""Read-only local-disk store plugin.

This plugin exposes one existing directory tree for reads only. It shares the
managed directory resolution logic with the writable variant but refuses all
mutating operations.
"""

from __future__ import annotations

from typing import Optional

from LiuXin_alpha.storage.api import StoreStatus, StoreLocationMixinAPI
from LiuXin_alpha.storage.store_backend_plugins.on_disk_existing_managed_drive.on_disk_existing_managed_drive_storage_backend import (
    OnDiskExistingManagedStorageBackend,
)
from LiuXin_alpha.storage.store_backend_plugins.on_disk_existing_unmanaged_drive.on_disk_existing_unmanaged_drive_location import (
    OnDiskUnmanagedStoreLocation,
)


class OnDiskUnmanagedStorageBackend(OnDiskExistingManagedStorageBackend):
    """Read-only view of files already present on a local drive."""

    location_cls = OnDiskUnmanagedStoreLocation

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

    def write_bytes(self, file_bytes: bytes, *, metadata=None, location: str | None = None) -> OnDiskUnmanagedStoreLocation:
        raise PermissionError("OnDiskUnmanagedStorageBackend is read-only.")

    def copy_within_plugin(
        self,
        src_location: str | StoreLocationMixinAPI,
        dst_location: str | StoreLocationMixinAPI,
    ) -> OnDiskUnmanagedStoreLocation:
        raise PermissionError("OnDiskUnmanagedStorageBackend is read-only.")

    def delete(self, file_identifier: str | StoreLocationMixinAPI) -> bool:
        raise PermissionError("OnDiskUnmanagedStorageBackend is read-only.")

    def update_bytes(
        self,
        file_identifier: str | StoreLocationMixinAPI,
        file_bytes: bytes,
        *,
        append: bool = False,
    ) -> bool:
        raise PermissionError("OnDiskUnmanagedStorageBackend is read-only.")
