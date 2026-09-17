"""
Expose the transient storage-manager implementation.

TransientStorageManager retains manager metadata only for its instance lifetime;
attached Stores still own real byte publication and deletion. Database persistence
adapters live in sibling modules and are not imported through this initializer.
"""

from LiuXin_alpha.storage.storage_manager.manager import TransientStorageManager


__all__ = [
    "TransientStorageManager",
]
