"""Persistence facade for backup intent, checkpoints, and results."""

from __future__ import annotations

import abc

from collections.abc import Iterator

from LiuXin_alpha.storage.api2.workflow_api.backup_api.models import (
    BackupSourceSpec,
    BackupWorkflowResult,
    BackupWorkflowResumeState,
    BackupWorkflowSpec,
    RegisteredBackupArtifact,
)
from LiuXin_alpha.storage.api2.workflow_api.models import WorkflowID, WorkflowStatus


class BackupWorkflowRepositoryAPI(abc.ABC):
    """Persist workflow values without exposing database row implementations.

    This replaces the legacy public fixed-table row classes.  Database schemas,
    transactions, JSON encoding, and row mutation remain implementation details
    below this repository facade.

    Example:
        >>> workflow_id = repository.save_workflow_spec(spec)  # doctest: +SKIP
        >>> repository.save_resume_state(workflow_id, state)  # doctest: +SKIP
    """

    @abc.abstractmethod
    def save_workflow_spec(
        self,
        spec: BackupWorkflowSpec,
        *,
        workflow_id: WorkflowID | None = None,
        status: WorkflowStatus = WorkflowStatus.DRAFT,
    ) -> WorkflowID:
        """Create or replace durable workflow intent and return its id.

        Example:
            >>> workflow_id = repository.save_workflow_spec(spec)  # doctest: +SKIP
        """
        ...

    @abc.abstractmethod
    def load_workflow_spec(self, workflow_id: WorkflowID) -> BackupWorkflowSpec:
        """Load immutable workflow intent by identifier.

        Example:
            >>> spec = repository.load_workflow_spec(3)  # doctest: +SKIP
        """
        ...

    @abc.abstractmethod
    def iter_workflow_specs(
        self,
        *,
        status: WorkflowStatus | None = None,
    ) -> Iterator[tuple[WorkflowID, BackupWorkflowSpec]]:
        """Iterate over workflow intent, optionally filtered by status.

        Example:
            >>> drafts = list(  # doctest: +SKIP
            ...     repository.iter_workflow_specs(status=WorkflowStatus.DRAFT),
            ... )
        """
        ...

    @abc.abstractmethod
    def save_resume_state(
        self,
        workflow_id: WorkflowID,
        state: BackupWorkflowResumeState,
    ) -> None:
        """Atomically persist the latest resumable checkpoint.

        Example:
            >>> repository.save_resume_state(3, state)  # doctest: +SKIP
        """
        ...

    @abc.abstractmethod
    def load_resume_state(self, workflow_id: WorkflowID) -> BackupWorkflowResumeState:
        """Load the latest checkpoint or synthesize a draft checkpoint.

        Example:
            >>> state = repository.load_resume_state(3)  # doctest: +SKIP
        """
        ...

    @abc.abstractmethod
    def record_result(
        self,
        workflow_id: WorkflowID,
        result: BackupWorkflowResult,
    ) -> None:
        """Persist one terminal workflow result and output identity.

        Example:
            >>> repository.record_result(3, result)  # doctest: +SKIP
        """
        ...

    @abc.abstractmethod
    def record_backup_presence(
        self,
        workflow_id: WorkflowID,
        artifact: RegisteredBackupArtifact,
        source: BackupSourceSpec,
        *,
        archive_path: str,
        protected: bool = True,
        immutable: bool = True,
    ) -> bool:
        """Record an idempotent protected link from source to backup artifact.

        Example:
            >>> created = repository.record_backup_presence(  # doctest: +SKIP
            ...     3, artifact, source, archive_path="books/a.epub",
            ... )
        """
        ...

    @abc.abstractmethod
    def delete_workflow(
        self,
        workflow_id: WorkflowID,
        *,
        require_terminal: bool = True,
    ) -> bool:
        """Delete workflow persistence without deleting artifact bytes.

        Example:
            >>> deleted = repository.delete_workflow(3)  # doctest: +SKIP
        """
        ...


__all__ = ["BackupWorkflowRepositoryAPI"]
