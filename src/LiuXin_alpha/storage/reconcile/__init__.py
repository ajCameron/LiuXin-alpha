"""
Export legacy file-registration and database-driven SquashFS publication workflows.

Local disk and rclone registration populate legacy Store/file/link rows, while
SquashFS designation/publication records archive state and can adopt verified
Asset/Replica identities through a StorageManager. Their report values describe
phase-specific observations and partial effects rather than an all-run transaction.
Imports expose the underlying callables directly; no workflow runs on import.
"""

from __future__ import annotations

from LiuXin_alpha.storage.reconcile.models import (
    SquashfsArchivePublishReport,
    SquashfsDesignationReport,
    UnmanagedDiskRegistrationReport,
)
from LiuXin_alpha.storage.reconcile.squashfs_db_sync import (
    LOCKED_SQUASHFS_STORE_KIND,
    OPEN_SQUASHFS_STORE_KIND,
    SQUASHFS_DESIGNATION_LINK_TYPE,
    designate_files_for_squashfs_store,
    ensure_open_squashfs_store,
    publish_open_squashfs_store,
    publish_squashfs_archive_from_file_ids,
)
from LiuXin_alpha.storage.reconcile.store_db_sync import (
    ensure_rclone_http_readonly_store,
    ensure_unmanaged_store_for_disk,
    main,
    register_existing_disk_as_unmanaged_store,
    register_existing_disk_with_database_path,
    register_rclone_http_readonly_store_files,
    register_rclone_http_readonly_with_database_path,
)

__all__ = [
    "UnmanagedDiskRegistrationReport",
    "SquashfsDesignationReport",
    "SquashfsArchivePublishReport",
    "OPEN_SQUASHFS_STORE_KIND",
    "LOCKED_SQUASHFS_STORE_KIND",
    "SQUASHFS_DESIGNATION_LINK_TYPE",
    "ensure_open_squashfs_store",
    "designate_files_for_squashfs_store",
    "publish_open_squashfs_store",
    "publish_squashfs_archive_from_file_ids",
    "ensure_rclone_http_readonly_store",
    "ensure_unmanaged_store_for_disk",
    "register_existing_disk_as_unmanaged_store",
    "register_existing_disk_with_database_path",
    "register_rclone_http_readonly_store_files",
    "register_rclone_http_readonly_with_database_path",
    "main",
]
