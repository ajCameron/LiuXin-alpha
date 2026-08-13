"""Resumable backup workflow contract above the storage manager."""

from __future__ import annotations

import abc

from typing import TYPE_CHECKING, Self
from uuid import UUID

from LiuXin_alpha.storage.api.models import Location
from LiuXin_alpha.storage.api.workflow_api.backup_api.models import (
    BackupSourceSpec,
    BackupWorkflowKind,
    BackupWorkflowResult,
    BackupWorkflowResumeState,
    BackupWorkflowSpec,
)
from LiuXin_alpha.storage.api.workflow_api.base_api import StorageWorkflowAPI

if TYPE_CHECKING:
    from LiuXin_alpha.storage.api.storage_manager_api import StorageManagerAPI


class BackupWorkflowAPI(
    StorageWorkflowAPI[
        BackupWorkflowSpec,
        BackupWorkflowResumeState,
        BackupWorkflowResult,
    ],
    abc.ABC,
):
    """Contract for one resumable backup or archival artifact workflow.

    Workflows designate sources, stage verified bytes, seal an artifact,
    optionally register it as a store, record protected presence, and expose a
    durable checkpoint after every resumable unit.

    Example:
        >>> workflow.designate_location(  # doctest: +SKIP
        ...     Location(UUID(int=1), "objects/42"),
        ...     archive_path="books/novel.epub",
        ... )
        >>> state = workflow.run_next()  # doctest: +SKIP
    """

    @property
    @abc.abstractmethod
    def workflow_kind(self) -> BackupWorkflowKind:
        """Return the stable backup workflow implementation family.

        Example:
            >>> kind = workflow.workflow_kind  # doctest: +SKIP
        """
        ...

    @abc.abstractmethod
    def designate_local_path(
        self,
        source_path: str,
        *,
        archive_path: str | None = None,
    ) -> BackupSourceSpec:
        """Add a local filesystem source to immutable workflow intent.

        Example:
            >>> source = workflow.designate_local_path(  # doctest: +SKIP
            ...     "/incoming/a.epub", archive_path="books/a.epub",
            ... )
        """
        ...

    @abc.abstractmethod
    def designate_location(
        self,
        source_location: Location,
        *,
        archive_path: str | None = None,
    ) -> BackupSourceSpec:
        """Add a managed storage Location to immutable workflow intent.

        Example:
            >>> source = workflow.designate_location(  # doctest: +SKIP
            ...     Location(UUID(int=1), "objects/42"),
            ...     archive_path="books/a.epub",
            ... )
        """
        ...

    @classmethod
    @abc.abstractmethod
    def from_spec(
        cls,
        spec: BackupWorkflowSpec,
        *,
        storage_manager: StorageManagerAPI | None = None,
    ) -> Self:
        """Construct a fresh concrete workflow from durable intent.

        Example:
            >>> workflow = ConcreteWorkflow.from_spec(  # doctest: +SKIP
            ...     spec, storage_manager=manager,
            ... )
        """
        ...

    @classmethod
    @abc.abstractmethod
    def from_resume_state(
        cls,
        resume_state: BackupWorkflowResumeState,
        *,
        storage_manager: StorageManagerAPI | None = None,
    ) -> Self:
        """Reconstruct a concrete workflow from one durable checkpoint.

        Example:
            >>> workflow = ConcreteWorkflow.from_resume_state(  # doctest: +SKIP
            ...     state, storage_manager=manager,
            ... )
        """
        ...


__all__ = ["BackupWorkflowAPI"]
