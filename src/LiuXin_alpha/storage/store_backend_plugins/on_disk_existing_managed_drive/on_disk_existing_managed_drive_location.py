"""
Expose OnDiskExistingManagedStoreLocation as the common opaque Location class.

The alias does not enforce the managed allocation prefix or perform filesystem
lookup; those operations belong to the owning Store.
"""

from LiuXin_alpha.storage.api import Location


OnDiskExistingManagedStoreLocation = Location


__all__ = ["OnDiskExistingManagedStoreLocation"]
