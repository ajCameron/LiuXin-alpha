"""Explicitly transient implementation of the storage-manager contract."""

from LiuXin_alpha.storage.storage_manager.manager import (
    InMemoryStorageManager,
    TransientStorageManager,
)


__all__ = ["InMemoryStorageManager", "TransientStorageManager"]
