from __future__ import annotations

from LiuXin_alpha.storage.store_backend_plugins.on_disk_existing_unmanaged_drive.on_disk_unmanaged_single_file import (
    OnDiskUnmanagedSingleFile,
)


class OnDiskExistingManagedSingleFile(OnDiskUnmanagedSingleFile):
    """
    Existing-managed store single-file container.

    Reuses the on-disk single-file implementation.
    """

