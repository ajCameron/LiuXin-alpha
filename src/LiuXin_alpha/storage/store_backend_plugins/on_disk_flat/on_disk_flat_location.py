"""
Expose OnDiskFlatStoreLocation as the common opaque Location class.

The alias adds no digest naming, path parsing, ownership checks, or methods;
the owning Store interprets and validates identifiers.
"""

from LiuXin_alpha.storage.api import Location


OnDiskFlatStoreLocation = Location


__all__ = ["OnDiskFlatStoreLocation"]
