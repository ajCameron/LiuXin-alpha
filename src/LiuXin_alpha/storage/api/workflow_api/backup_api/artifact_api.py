"""
Registration facade for completed backup artifacts.
"""

from __future__ import annotations

import abc

from collections.abc import Iterator

from LiuXin_alpha.storage.api.workflow_api.backup_api.models import (
    BackupWorkflowResult,
    BackupArtifactRegistration,
)
from LiuXin_alpha.storage.api.workflow_api.models import WorkflowID


class BackupArtifactRegistryAPI(abc.ABC):
    """
    Register completed artifacts as Stores and preserve source presence.

    Artifact creation and store registration are separate operations: a sealed
    artifact may exist before it becomes a configured read-only Store.

    Example:
        >>> registration = registry.register_artifact(3, result)  # doctest: +SKIP
    """

    @abc.abstractmethod
    def register_artifact(
        self,
        workflow_id: WorkflowID,
        result: BackupWorkflowResult,
        *,
        store_name: str | None = None,
        link_sources: bool = True,
    ) -> BackupArtifactRegistration:
        """
        Register a successful workflow output as a configured Store.

        Example:
            >>> registration = registry.register_artifact(  # doctest: +SKIP
            ...     3, result, store_name="nightly-pack",
            ... )


        :param workflow_id:
        :param result:
        :param store_name:
        :param link_sources:
        :return:
        """
        ...

    @abc.abstractmethod
    def get_artifact_registration(
        self,
        workflow_id: WorkflowID,
    ) -> BackupArtifactRegistration | None:
        """
        Return an artifact registration for one workflow, if present.

        Example:
            >>> registration = registry.get_artifact_registration(3)  # doctest: +SKIP


        :param workflow_id:
        :return:
        """
        ...

    @abc.abstractmethod
    def iter_artifact_registrations(self) -> Iterator[BackupArtifactRegistration]:
        """
        Iterate over registered backup artifacts.

        Example:
            >>> registrations = list(  # doctest: +SKIP
            ...     registry.iter_artifact_registrations(),
            ... )


        :return:
        """
        ...


__all__ = ["BackupArtifactRegistryAPI"]
