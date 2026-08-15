"""
Persistence facade for backup intent, checkpoints, and results.
"""

from __future__ import annotations

import abc

from collections.abc import Iterator

from LiuXin_alpha.storage.api.workflow_api.backup_api.models import (
    BackupSourceDeclaration,
    BackupWorkflowResult,
    BackupWorkflowCheckpoint,
    BackupWorkflowDeclaration,
    BackupArtifactRegistration,
)
from LiuXin_alpha.storage.api.workflow_api.models import WorkflowID, WorkflowStatus


class BackupWorkflowRepositoryAPI(abc.ABC):
    """
    Persist workflow values without exposing database row implementations.

    This replaces the legacy public fixed-table row classes.  Database schemas,
    transactions, JSON encoding, and row mutation remain implementation details
    below this repository facade.

    Example:
        >>> workflow_id = repository.save_workflow_declaration(  # doctest: +SKIP
        ...     declaration,
        ... )
        >>> repository.save_checkpoint(workflow_id, checkpoint)  # doctest: +SKIP
    """

    @abc.abstractmethod
    def save_workflow_declaration(
        self,
        declaration: BackupWorkflowDeclaration,
        *,
        workflow_id: WorkflowID | None = None,
        status: WorkflowStatus = WorkflowStatus.DRAFT,
    ) -> WorkflowID:
        """
        Create or replace durable workflow intent and return its id.

        Example:
            >>> workflow_id = repository.save_workflow_declaration(  # doctest: +SKIP
            ...     declaration,
            ... )


        :param declaration:
        :param workflow_id:
        :param status:
        :return:
        """
        ...

    @abc.abstractmethod
    def load_workflow_declaration(self, workflow_id: WorkflowID) -> BackupWorkflowDeclaration:
        """
        Load immutable workflow intent by identifier.

        Example:
            >>> declaration = repository.load_workflow_declaration(  # doctest: +SKIP
            ...     3,
            ... )


        :param workflow_id:
        :return:
        """
        ...

    @abc.abstractmethod
    def iter_workflow_declarations(
        self,
        *,
        status: WorkflowStatus | None = None,
    ) -> Iterator[tuple[WorkflowID, BackupWorkflowDeclaration]]:
        """
        Iterate over workflow intent, optionally filtered by status.

        Example:
            >>> drafts = list(  # doctest: +SKIP
            ...     repository.iter_workflow_declarations(status=WorkflowStatus.DRAFT),
            ... )


        :param status:
        :return:
        """
        ...

    @abc.abstractmethod
    def save_checkpoint(
        self,
        workflow_id: WorkflowID,
        checkpoint: BackupWorkflowCheckpoint,
    ) -> None:
        """
        Atomically persist the latest resumable checkpoint.

        Example:
            >>> repository.save_checkpoint(3, checkpoint)  # doctest: +SKIP


        :param workflow_id:
        :param checkpoint:
        :return:
        """
        ...

    @abc.abstractmethod
    def load_checkpoint(self, workflow_id: WorkflowID) -> BackupWorkflowCheckpoint:
        """
        Load the latest checkpoint or synthesize a draft checkpoint.

        Example:
            >>> checkpoint = repository.load_checkpoint(3)  # doctest: +SKIP


        :param workflow_id:
        :return:
        """
        ...

    @abc.abstractmethod
    def record_result(
        self,
        workflow_id: WorkflowID,
        result: BackupWorkflowResult,
    ) -> None:
        """
        Persist one terminal workflow result and output identity.

        Example:
            >>> repository.record_result(3, result)  # doctest: +SKIP


        :param workflow_id:
        :param result:
        :return:
        """
        ...

    @abc.abstractmethod
    def record_backup_presence(
        self,
        workflow_id: WorkflowID,
        registration: BackupArtifactRegistration,
        source: BackupSourceDeclaration,
        *,
        archive_path: str,
        protected: bool = True,
        immutable: bool = True,
    ) -> bool:
        """
        Record an idempotent protected link from source to backup artifact.

        Example:
            >>> created = repository.record_backup_presence(  # doctest: +SKIP
            ...     3, registration, source, archive_path="books/a.epub",
            ... )


        :param workflow_id:
        :param registration:
        :param source:
        :param archive_path:
        :param protected:
        :param immutable:
        :return:
        """
        ...

    @abc.abstractmethod
    def delete_workflow(
        self,
        workflow_id: WorkflowID,
        *,
        require_terminal: bool = True,
    ) -> bool:
        """
        Delete workflow persistence without deleting artifact bytes.

        Example:
            >>> deleted = repository.delete_workflow(3)  # doctest: +SKIP


        :param workflow_id:
        :param require_terminal:
        :return:
        """
        ...


__all__ = ["BackupWorkflowRepositoryAPI"]
