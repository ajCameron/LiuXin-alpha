"""
Expose SquashfsBuildStoreLocation as the common opaque Store Location class.

This compatibility alias adds no archive parsing, identity checks, or methods.
The owning Store resolves text and validates location ownership.
"""

from LiuXin_alpha.storage.api import Location


SquashfsBuildStoreLocation = Location


__all__ = ["SquashfsBuildStoreLocation"]
