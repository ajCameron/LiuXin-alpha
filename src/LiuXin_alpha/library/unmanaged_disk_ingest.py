"""
Preserve legacy imports for canonical storage reconciliation declarations.

Export the same report classes and local registration callables without wrapping
their arguments or results. Importing this module performs no registration; running
it as a script delegates to the standalone local-disk reconciliation CLI.

Example:
    >>> StoreDbSyncReport is UnmanagedDiskRegistrationReport
    True
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
