"""Concrete backup workflow implementations.

Backup workflows are intentionally separate from raw store plugins:
- store plugins read/write bytes on one medium
- store containers wrap one configured store
- the storage manager orchestrates stores
- backup workflows coordinate staged/exported artifacts and resume state
"""

from __future__ import annotations

from LiuXin_alpha.storage.backup.backup_artifact_registry import BackupArtifactRegistry, RegisteredBackupArtifact
from LiuXin_alpha.storage.backup.backup_workflow_repository import BackupWorkflowRepository
from LiuXin_alpha.storage.backup.squashfs_backup_workflow import SquashfsBackupWorkflow
from LiuXin_alpha.storage.backup.store_backup_planner import PlannedBackupPack, StoreBackupPlanner
from LiuXin_alpha.storage.backup.prototype_pipeline import ConsoleReporter, ExistingDriveSquashfsPrototype, IndexedStoreRun, PackExecutionRun, PrototypeRunResult

__all__ = [
    "BackupArtifactRegistry",
    "BackupWorkflowRepository",
    "PlannedBackupPack",
    "RegisteredBackupArtifact",
    "SquashfsBackupWorkflow",
    "StoreBackupPlanner",
    "ConsoleReporter",
    "ExistingDriveSquashfsPrototype",
    "IndexedStoreRun",
    "PackExecutionRun",
    "PrototypeRunResult",
]
