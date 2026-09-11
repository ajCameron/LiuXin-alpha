"""
Preserve the historical wget-HTML location name as an alias of Location.

This is the storage API class itself, without legacy URL constructor translation,
additional methods, or a separate identity domain.
"""

from LiuXin_alpha.storage.api import Location


WgetHtmlReadOnlyStoreLocation = Location


__all__ = ["WgetHtmlReadOnlyStoreLocation"]
