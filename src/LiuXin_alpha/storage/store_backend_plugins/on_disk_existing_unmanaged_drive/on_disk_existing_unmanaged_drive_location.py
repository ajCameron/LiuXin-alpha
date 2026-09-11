"""
Expose unmanaged, local, and read-only location names as the same Location class.

All three compatibility names reference the common opaque value. Read-only
policy, text parsing, and identity validation remain with the owning Store.
"""

from LiuXin_alpha.storage.api import Location


OnDiskUnmanagedStoreLocation = Location
OnDiskLocalStoreLocation = Location
OnDiskReadOnlyStoreLocation = Location


__all__ = [
    "OnDiskLocalStoreLocation",
    "OnDiskReadOnlyStoreLocation",
    "OnDiskUnmanagedStoreLocation",
]
