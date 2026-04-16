"""Location type for the flat hash-named on-disk store plugin.

This plugin intentionally models a *single directory* containing files whose
names are their content hashes. Locations therefore represent either the root of
that directory or one direct child file inside it. Nested directories are not
part of the contract.
"""

from __future__ import annotations

from LiuXin_alpha.storage.store_backend_plugins.on_disk_existing_unmanaged_drive.on_disk_existing_unmanaged_drive_location import (
    OnDiskUnmanagedStoreLocation,
)


class OnDiskFlatStoreLocation(OnDiskUnmanagedStoreLocation):
    """Location for the flat hash-named local disk plugin.

    A flat-store location may point to the store root (no path parts) or to one
    direct child filename. Any nested path would violate the plugin contract and
    is refused eagerly.
    """

    def __init__(self, *args: str, store) -> None:
        super().__init__(*args, store=store)
        if len(self.parts) > 1:
            raise ValueError(
                "OnDiskFlatStoreLocation only supports the store root or one direct child file."
            )
