"""
Concrete implementations of the replacement storage-manager contract.

``InMemoryStorageManager`` is the executable reference implementation. It
keeps manager-owned metadata in process while routing bytes to real
``StoreAPI`` instances.
"""

from LiuXin_alpha.storage.storage_manager.manager import InMemoryStorageManager


__all__ = ["InMemoryStorageManager"]
