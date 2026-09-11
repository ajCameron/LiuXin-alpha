"""
Retain the historical rclone Location name as an exact alias of the shared opaque Location.

    No subclass, path parser, or additional remote state is introduced. Store methods
    supply ownership validation and convert keys to rclone identifiers.
"""

from LiuXin_alpha.storage.api import Location


RcloneHttpReadOnlyStoreLocation = Location


__all__ = ["RcloneHttpReadOnlyStoreLocation"]
