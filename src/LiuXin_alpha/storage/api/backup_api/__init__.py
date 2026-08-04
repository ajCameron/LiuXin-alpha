"""Backup-workflow API surface and persistence row helpers.

Examples:
    Describe a local file selected for a backup::

        from LiuXin_alpha.storage.api.backup_api import BackupSourceKind, BackupSourceSpec

        source = BackupSourceSpec(BackupSourceKind.LOCAL_PATH, "/books/book.epub")
"""

from __future__ import annotations

from LiuXin_alpha.storage.api.backup_api.backup_workflow_api import BackupWorkflowAPI
from LiuXin_alpha.storage.api.backup_api.backup_workflow_models import (
    BackupSourceKind,
    BackupSourceResult,
    BackupSourceSpec,
    BackupWorkflowKind,
    BackupWorkflowResult,
    BackupWorkflowResumeState,
    BackupWorkflowSpec,
    BackupWorkflowStatus,
    BackupWorkflowStepKind,
)
from LiuXin_alpha.storage.api.backup_api.backup_workflow_rows import (
    BackupPresenceLinkRow,
    BackupWorkflowOutputRow,
    BackupWorkflowRow,
    BackupWorkflowSourceRow,
    BackupWorkflowStateRow,
)

__all__ = [
    "BackupWorkflowAPI",
    "BackupSourceKind",
    "BackupSourceResult",
    "BackupSourceSpec",
    "BackupWorkflowKind",
    "BackupWorkflowResult",
    "BackupWorkflowResumeState",
    "BackupWorkflowSpec",
    "BackupWorkflowStatus",
    "BackupWorkflowStepKind",
    "BackupPresenceLinkRow",
    "BackupWorkflowOutputRow",
    "BackupWorkflowRow",
    "BackupWorkflowSourceRow",
    "BackupWorkflowStateRow",
]
