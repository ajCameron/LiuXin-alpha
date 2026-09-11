"""
Expose the transient storage-manager implementation and its compatibility name.

TransientStorageManager retains manager metadata only for its instance lifetime;
attached Stores still own real byte publication and deletion. The historical
InMemoryStorageManager name points to that same class. Database persistence
adapters live in sibling modules and are not imported through this initializer.
"""

from LiuXin_alpha.storage.storage_manager.manager import (
    InMemoryStorageManager,
    TransientStorageManager,
)


__all__ = ["InMemoryStorageManager", "TransientStorageManager"]
