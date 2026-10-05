"""
Expose the supported handlers compatibility surface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise   init   through a consuming regression::

        python -m pytest -q tests/jobs/test_jobs_worker.py
"""

from LiuXin_alpha.jobs.handlers.existing_drive_squashfs_backup import (
    ExistingDriveSquashfsBackupJobHandler,
    ExistingDriveSquashfsBackupJobPayload,
)

__all__ = [
    "ExistingDriveSquashfsBackupJobHandler",
    "ExistingDriveSquashfsBackupJobPayload",
]
