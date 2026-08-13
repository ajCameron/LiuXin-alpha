"""Registration facade for completed backup artifacts."""

from __future__ import annotations

import abc

from collections.abc import Iterator

from LiuXin_alpha.storage.api.workflow_api.backup_api.models import (
    BackupWorkflowResult,
    RegisteredBackupArtifact,
)
from LiuXin_alpha.storage.api.workflow_api.models import WorkflowID


class BackupArtifactRegistryAPI(abc.ABC):
    """Register completed artifacts as Stores and preserve source presence.

    Artifact creation and store registration are separate operations: a sealed
    artifact may exist before it becomes a configured read-only Store.

    Example:
        >>> artifact = registry.register_artifact(3, result)  # doctest: +SKIP
    """

    @abc.abstractmethod
    def register_artifact(
        self,
        workflow_id: WorkflowID,
        result: BackupWorkflowResult,
        *,
        store_name: str | None = None,
        link_sources: bool = True,
    ) -> RegisteredBackupArtifact:
        """Register a successful workflow output as a configured Store.

        Example:
            >>> artifact = registry.register_artifact(  # doctest: +SKIP
            ...     3, result, store_name="nightly-pack",
            ... )
        """
        ...

    @abc.abstractmethod
    def get_registered_artifact(
        self,
        workflow_id: WorkflowID,
    ) -> RegisteredBackupArtifact | None:
        """Return an artifact registration for one workflow, if present.

        Example:
            >>> artifact = registry.get_registered_artifact(3)  # doctest: +SKIP
        """
        ...

    @abc.abstractmethod
    def iter_registered_artifacts(self) -> Iterator[RegisteredBackupArtifact]:
        """Iterate over registered backup artifacts.

        Example:
            >>> artifacts = list(registry.iter_registered_artifacts())  # doctest: +SKIP
        """
        ...


__all__ = ["BackupArtifactRegistryAPI"]
