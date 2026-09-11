"""
Preserve the historical native-HTML location name as an alias of Location.

The alias is the same class object, not a URL-parsing adapter or subclass. Its
constructor and Store-scoped identity rules are those of the storage API value.
"""

from LiuXin_alpha.storage.api import Location


NativeHtmlReadOnlyStoreLocation = Location


__all__ = ["NativeHtmlReadOnlyStoreLocation"]
