"""
Expose OnDiskCalibreLikeSingleFile as FileInfo for legacy imports.

The alias is a metadata value containing a Location and observed file facts; it
does not retain a Store, open a file, or implement a backend-specific reader.
"""

from LiuXin_alpha.storage.api import FileInfo


OnDiskCalibreLikeSingleFile = FileInfo


__all__ = ["OnDiskCalibreLikeSingleFile"]
