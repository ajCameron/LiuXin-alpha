"""Concrete backup workflow implementations.

Backup workflows are intentionally separate from raw store plugins:
- store plugins read/write bytes on one medium
- store containers wrap one configured store
- the storage manager orchestrates stores
- backup workflows coordinate staged/exported artifacts and resume state
"""

from __future__ import annotations

from LiuXin_alpha.storage.backup.squashfs_backup_workflow import SquashfsBackupWorkflow

__all__ = ["SquashfsBackupWorkflow"]
