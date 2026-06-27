"""Concrete application job handlers."""

from LiuXin_alpha.jobs.handlers.existing_drive_squashfs_backup import (
    ExistingDriveSquashfsBackupJobHandler,
    ExistingDriveSquashfsBackupJobPayload,
)

__all__ = [
    "ExistingDriveSquashfsBackupJobHandler",
    "ExistingDriveSquashfsBackupJobPayload",
]
