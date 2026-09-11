"""
Export the shared Location class under the legacy writable-ISO name.

IsoWritableStoreLocation is an identity alias, not a subclass or parser. Store
ownership and writable member-key checks belong to the adapter and its driver.
"""

from LiuXin_alpha.storage.api import Location


IsoWritableStoreLocation = Location


__all__ = ["IsoWritableStoreLocation"]
