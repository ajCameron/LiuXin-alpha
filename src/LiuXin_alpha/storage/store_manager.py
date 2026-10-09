"""Compatibility imports for the durable application storage manager.

New code should import :class:`StorageManager` from
``LiuXin_alpha.storage.durable_manager``.  This module remains intentionally
small so existing integrations keep working while ``storage_manager`` can
unambiguously mean the repository-neutral manager package.
"""

from LiuXin_alpha.storage.durable_manager import (
    StorageBootstrapIssue,
    StorageBootstrapReport,
    StorageManager,
)

__all__ = [
    "StorageBootstrapIssue",
    "StorageBootstrapReport",
    "StorageManager",
]
