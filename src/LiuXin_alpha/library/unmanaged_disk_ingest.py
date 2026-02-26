"""
Compatibility wrapper for unmanaged disk registration helpers.

Canonical home:
- LiuXin_alpha.storage.reconcile
"""

from __future__ import annotations

from LiuXin_alpha.storage.reconcile import (
    StoreDbSyncReport,
    UnmanagedDiskRegistrationReport,
    ensure_unmanaged_store_for_disk,
    main,
    register_existing_disk_as_unmanaged_store,
    register_existing_disk_with_database_path,
)

__all__ = [
    "StoreDbSyncReport",
    "UnmanagedDiskRegistrationReport",
    "ensure_unmanaged_store_for_disk",
    "register_existing_disk_as_unmanaged_store",
    "register_existing_disk_with_database_path",
    "main",
]


if __name__ == "__main__":
    raise SystemExit(main())
