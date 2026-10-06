
"""
Expose backup-image Store registration and source-presence lookup.

Registry metadata is separate from building the archive and from cataloguing its whole
image as a derived atomic Asset. Concrete registration may have partial persistent effects.
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
    Expose completed sealed backup images as configured read-only Stores.

    A backup image is one workflow output container, not necessarily a complete snapshot of the
    source Store. It can exist before registration. This service records the Store that exposes its
    members and can link those members to source Asset identities. That is separate from ingesting
    the image itself as one atomic Asset with a derivation recipe. Physical format support and local
    mounting requirements belong to the implementation. Registration can span several writes and a
    manager attachment, with no cross-operation rollback promised by the ABC.

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
        Register a successful backup output and optionally record its member presence.

        The database registry requires successful result fields, a compatible workflow ID, and a
        locally accessible artifact file for a new registration. It reuses an existing registration
        before checking the file or applying new naming/linking options. New registration writes
        Store/output metadata and source links before optional manager attachment; a later failure
        can leave earlier writes visible. It does not execute the build or prove member contents by
        reading the archive.

        The current implementation creates a SquashFS read-only Store configuration. A Location
        artifact requires a manager-backed Store exposing a safe local path.

        Example:
            >>> registration = registry.register_artifact(  # doctest: +SKIP
            ...     3, result, store_name="nightly-pack",
            ... )

        :param workflow_id: Identifier of the persisted workflow declaration and execution that produced the artifact; result.workflow_id may be None or this ID.
        :param result: Terminal successful outcome containing an output artifact reference and source declaration.
        :param store_name: Optional name for a newly created Store; existing registrations or reused Store rows keep their names.
        :param link_sources: Whether a new registration should insert protected member-presence records from the declaration.

        :return: Registration value for the existing or newly associated Store, including the registry-reported presence count.
        """
        ...

    @abc.abstractmethod
    def get_artifact_registration(
        self,
        workflow_id: WorkflowID,
    ) -> BackupArtifactRegistration | None:
        """
        Read the registration currently associated with a workflow, if any.

        The database registry reconstructs this from output and Store metadata and counts current
        presence links for that Store. Lookup does not reopen the image or verify it is still
        readable; malformed references or missing Store identity can raise rather than returning
        None.

        Example:
            >>> registration = registry.get_artifact_registration(3)  # doctest: +SKIP


        :param workflow_id: Workflow identifier whose registered output is requested.
        :return: Stored artifact/Store association, or None when no registered output is present.
        """
        ...

    @abc.abstractmethod
    def iter_artifact_registrations(self) -> Iterator[BackupArtifactRegistration]:
        """
        Yield available workflow-to-artifact registrations from registry metadata.

        The database implementation resolves each workflow at most once while iterating output rows.
        The interface does not guarantee a stable snapshot or sorted order; reconstruction errors
        may occur after earlier values have been yielded.

        Example:
            >>> registrations = list(  # doctest: +SKIP
            ...     registry.iter_artifact_registrations(),
            ... )


        :return: Iterator of reconstructed registration values; iteration does not validate artifact bytes.
        """
        ...


__all__ = ["BackupArtifactRegistryAPI"]
