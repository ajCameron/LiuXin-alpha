"""
Export the shared opaque Location under the legacy ISO-specific name.

IsoReadOnlyStoreLocation is the same Location class, not a subclass or parser.
Store ownership and member-key validation remain with the configured adapter.
"""

from LiuXin_alpha.storage.api import Location


IsoReadOnlyStoreLocation = Location


__all__ = ["IsoReadOnlyStoreLocation"]
