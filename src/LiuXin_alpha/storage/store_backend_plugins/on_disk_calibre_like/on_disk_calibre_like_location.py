"""
Expose OnDiskCalibreLikeStoreLocation as the common opaque Location class.

Placement hints and filesystem interpretation belong to the Store. This alias
adds no Calibre-specific fields, parsing, or methods.
"""

from LiuXin_alpha.storage.api import Location


OnDiskCalibreLikeStoreLocation = Location


__all__ = ["OnDiskCalibreLikeStoreLocation"]
