from __future__ import annotations

from LiuXin_alpha.storage.store_backend_plugins.on_disk_existing_managed_drive.on_disk_existing_managed_drive_location import (
    OnDiskExistingManagedStoreLocation,
)


class OnDiskCalibreLikeStoreLocation(OnDiskExistingManagedStoreLocation):
    """
    Calibre-like store location.

    This backend keeps the same local-filesystem semantics as the existing
    managed on-disk store and only changes file placement strategy.
    """

