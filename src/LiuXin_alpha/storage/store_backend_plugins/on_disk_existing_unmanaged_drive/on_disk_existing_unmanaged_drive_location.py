"""Location compatibility name for the opaque new Store location value."""

from LiuXin_alpha.storage.api import Location


OnDiskUnmanagedStoreLocation = Location
OnDiskLocalStoreLocation = Location
OnDiskReadOnlyStoreLocation = Location


__all__ = [
    "OnDiskLocalStoreLocation",
    "OnDiskReadOnlyStoreLocation",
    "OnDiskUnmanagedStoreLocation",
]
