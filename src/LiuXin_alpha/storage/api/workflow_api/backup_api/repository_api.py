"""
Persist backup workflow values without exposing row and encoding details.

Declaration, state, result, and presence writes remain separate operations. Their API does
not create a transaction spanning multiple rows, reserve artifacts, or verify physical bytes.
"""

from __future__ import annotations

import abc
from collections.abc import Iterable, Iterator, Mapping
from typing import cast

from LiuXin_alpha.storage.api.models import Digest, Location, StoreUUID
from LiuXin_alpha.storage.api.storage_manager_api.models import (
    DigitalAssetID,
    ReplicaID,
)
from LiuXin_alpha.storage.api.workflow_api.backup_api.models import (
    BackupArtifactRegistration,
    BackupSourceDeclaration,
    BackupSourceKind,
    BackupWorkflowCheckpoint,
    BackupWorkflowDeclaration,
    BackupWorkflowKind,
    BackupWorkflowResult,
)
from LiuXin_alpha.storage.api.workflow_api.models import WorkflowID, WorkflowStatus


class BackupWorkflowRepositoryAPI(abc.ABC):
    """
    Persist backup intent and execution evidence behind domain values.

    Callers exchange declarations, checkpoints, results, and presence descriptions without depending
    on row classes or JSON envelopes. These methods do not run workflows, verify bytes, or start a
    transaction by virtue of the interface. The database adapter performs successive row updates;
    transaction scope, rollback, and concurrent-writer coordination belong to its caller/database
    integration.

    Example:
        >>> workflow_id = repository.save_workflow_declaration(  # doctest: +SKIP
        ...     declaration,
        ... )
        >>> repository.save_checkpoint(workflow_id, checkpoint)  # doctest: +SKIP
    """

    def save_workflow(
        self,
        workflow_name: str,
        workflow_kind: BackupWorkflowKind,
        output_target: str | Location,
        *,
        sources: Iterable[BackupSourceDeclaration] = (),
        verify_after_build: bool = True,
        cleanup_staging_after_success: bool = False,
        staging_target: str | Location | None = None,
        options: Mapping[str, str] | Iterable[tuple[str, str]] = (),
        workflow_id: WorkflowID | None = None,
        status: WorkflowStatus = WorkflowStatus.DRAFT,
    ) -> WorkflowID:
        """
        Construct durable backup intent and delegate to save_workflow_declaration.

        Materialize sources and options once. The declaration performs value normalization, while
        the precise persistence method remains responsible for ID allocation or replacement,
        status storage, transaction boundaries, and partial failures.

        Example:
            >>> workflow_id = repository.save_workflow(  # doctest: +SKIP
            ...     "nightly", BackupWorkflowKind.SQUASHFS_PACK, "nightly.sqsh",
            ... )


        :param workflow_name: Human-readable workflow name retained in durable intent.
        :param workflow_kind: Implementation-family discriminator retained by the declaration.
        :param output_target: Local path or routed Location for the completed artifact.
        :param sources: Ordered source declarations materialized into the workflow declaration.
        :param verify_after_build: Whether the workflow should request post-build verification.
        :param cleanup_staging_after_success: Whether supported staging should be removed after success.
        :param staging_target: Optional local path or routed Location used for staging.
        :param options: Mapping or ordered key/value pairs for implementation-specific settings.
        :param workflow_id: None to allocate an ID, or an existing ID to replace.
        :param status: Lifecycle status stored with the declaration.
        :return: The identity returned by save_workflow_declaration.
        """

        if isinstance(options, Mapping):
            option_values = tuple(cast(Mapping[str, str], options).items())
        else:
            option_values = tuple(
                cast(Iterable[tuple[str, str]], cast(object, options))
            )
        return self.save_workflow_declaration(
            BackupWorkflowDeclaration(
                workflow_name,
                workflow_kind,
                output_target,
                sources=tuple(sources),
                verify_after_build=verify_after_build,
                cleanup_staging_after_success=cleanup_staging_after_success,
                staging_target=staging_target,
                options=option_values,
            ),
            workflow_id=workflow_id,
            status=status,
        )

    @abc.abstractmethod
    def save_workflow_declaration(
        self,
        declaration: BackupWorkflowDeclaration,
        *,
        workflow_id: WorkflowID | None = None,
        status: WorkflowStatus = WorkflowStatus.DRAFT,
    ) -> WorkflowID:
        """
        Create durable intent or replace the declaration associated with an existing ID.

        The database adapter replaces ordered source rows and resets the workflow row's status/error
        fields. It does not clear separately stored checkpoint or output rows, so callers must
        coordinate replacement with previously recorded execution state. A multi-row failure is not
        covered by a transaction opened by this method.

        Example:
            >>> workflow_id = repository.save_workflow_declaration(  # doctest: +SKIP
            ...     declaration,
            ... )


        :param declaration: Backup name, targets, ordered sources, and build settings to persist.
        :param workflow_id: None to allocate a new repository ID, or an existing ID whose declaration should be replaced.
        :param status: Workflow-row lifecycle classification written with the intent; defaults to DRAFT.
        :return: Repository identifier of the newly created or updated workflow; a missing replacement target may raise.
        """
        ...

    @abc.abstractmethod
    def load_workflow_declaration(self, workflow_id: WorkflowID) -> BackupWorkflowDeclaration:
        """
        Reconstruct persisted intent and its source order for a workflow ID.

        Absence is an error rather than an empty declaration. Storage decoding or value-validation
        failures propagate; returning intent does not confirm source or destination availability.

        Example:
            >>> declaration = repository.load_workflow_declaration(  # doctest: +SKIP
            ...     3,
            ... )


        :param workflow_id: Repository identifier of the declaration to reconstruct.
        :return: BackupWorkflowDeclaration populated from persisted targets, sources, flags, and options.
        """
        ...

    @abc.abstractmethod
    def iter_workflow_declarations(
        self,
        *,
        status: WorkflowStatus | None = None,
    ) -> Iterator[tuple[WorkflowID, BackupWorkflowDeclaration]]:
        """
        Yield stored identifiers and declarations, optionally selected by workflow status.

        Filtering uses the repository's workflow status, not a fresh execution or artifact check.
        The database adapter sorts by numeric ID and reconstructs declarations during iteration;
        concurrent changes and later decoding failures can affect an in-progress traversal.

        Example:
            >>> drafts = list(  # doctest: +SKIP
            ...     repository.iter_workflow_declarations(status=WorkflowStatus.DRAFT),
            ... )


        :param status: Optional lifecycle filter; None includes workflows in every stored status.
        :return: Iterator of (workflow_id, declaration) pairs matching the selected stored status.
        """
        ...

    @abc.abstractmethod
    def save_checkpoint(
        self,
        workflow_id: WorkflowID,
        checkpoint: BackupWorkflowCheckpoint,
    ) -> None:
        """
        Replace current checkpoint evidence for an existing durable declaration.

        A checkpoint's optional ID must agree with workflow_id, and its declaration must match
        stored intent. The database adapter writes state and then updates the workflow status/error
        fields, without opening an enclosing transaction or comparing a revision token. Returning
        normally does not establish a separate commit boundary.

        Example:
            >>> repository.save_checkpoint(3, checkpoint)  # doctest: +SKIP


        :param workflow_id: Existing workflow whose current checkpoint is being replaced.
        :param checkpoint: State/evidence value with matching declaration and either no ID or the same ID.
        :return: None after the adapter writes checkpoint and workflow-status metadata; mismatch or persistence errors propagate.
        """
        ...

    @abc.abstractmethod
    def load_checkpoint(self, workflow_id: WorkflowID) -> BackupWorkflowCheckpoint:
        """
        Reconstruct the latest stored checkpoint or initial state from durable intent.

        When no state row exists, the database adapter uses the workflow row's status and error with
        zero counters, rather than unconditionally inventing DRAFT. Its current encoding does not
        round-trip updated_at. Missing intent and invalid persisted values raise; staging files are
        not inspected.

        Example:
            >>> checkpoint = repository.load_checkpoint(3)  # doctest: +SKIP


        :param workflow_id: Identifier of the workflow whose resumable state is requested.
        :return: BackupWorkflowCheckpoint built from stored evidence or the existing workflow row and declaration.
        """
        ...

    @abc.abstractmethod
    def record_result(
        self,
        workflow_id: WorkflowID,
        result: BackupWorkflowResult,
    ) -> None:
        """
        Persist a terminal result's checkpoint and any reported output reference.

        The database adapter uses final_checkpoint when supplied; otherwise it synthesizes a
        checkpoint from the result. The result type does not cross-check those two views. Checkpoint
        persistence occurs before output metadata, and the successful flag is recorded evidence
        rather than a new byte verification. This does not configure an artifact Store or insert
        member-presence links.

        Example:
            >>> repository.record_result(3, result)  # doctest: +SKIP


        :param workflow_id: Existing workflow ID; result.workflow_id must be None or agree.
        :param result: Terminal result and optional final checkpoint/output metadata to retain.
        :return: None after recording the checkpoint and any non-None output; later failures may follow earlier writes.
        """
        ...

    def record_backup_source_presence(
        self,
        workflow_id: WorkflowID,
        registration: BackupArtifactRegistration,
        source_kind: BackupSourceKind,
        source_identifier: str | Location,
        *,
        archive_path: str,
        expected_size: int | None = None,
        expected_digest: Digest | None = None,
        source_digital_asset_id: DigitalAssetID | None = None,
        source_replica_id: ReplicaID | None = None,
        source_store_ref: StoreUUID | None = None,
        protected: bool = True,
        immutable: bool = True,
    ) -> bool:
        """
        Construct source evidence and delegate backup-presence persistence.

        The source declaration validates its discriminator, identifier, expectations, and archive
        path before the repository is called. The underlying method remains responsible for
        deduplication and persistence and performs no physical verification.

        Example:
            >>> created = repository.record_backup_source_presence(  # doctest: +SKIP
            ...     workflow_id, registration, BackupSourceKind.LOCAL_PATH,
            ...     "/books/a.epub", archive_path="books/a.epub",
            ... )


        :param workflow_id: Existing workflow identity associated with the presence evidence.
        :param registration: Registered artifact and backup Store reference.
        :param source_kind: Source discriminator interpreted by BackupSourceDeclaration.
        :param source_identifier: Local path text or routed Location selected by source_kind.
        :param archive_path: Member path retained on the source and used as the presence key.
        :param expected_size: Optional expected source size in bytes.
        :param expected_digest: Optional expected source byte identity.
        :param source_digital_asset_id: Optional atomic Asset provenance identity.
        :param source_replica_id: Optional source Replica provenance identity.
        :param source_store_ref: Optional Store UUID, inferred from a Location when omitted.
        :param protected: Whether repository policy should protect a new presence link.
        :param immutable: Whether repository policy should prevent changes to a new link.
        :return: The boolean returned by record_backup_presence.
        """

        source = BackupSourceDeclaration(
            source_kind,
            source_identifier,
            archive_path=archive_path,
            expected_size=expected_size,
            expected_digest=expected_digest,
            source_digital_asset_id=source_digital_asset_id,
            source_replica_id=source_replica_id,
            source_store_ref=source_store_ref,
        )
        return self.record_backup_presence(
            workflow_id,
            registration,
            source,
            archive_path=archive_path,
            protected=protected,
            immutable=immutable,
        )

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
        Insert member-presence metadata unless the Store/member key already exists.

        The database implementation deduplicates by backup Store and exact archive_path. An existing
        row returns False without replacing its source, workflow, or protection flags. The
        declaration is recorded as evidence; this method does not read the member or normalize the
        supplied path. Protection and immutability concern repository metadata, not physical write
        protection of archive bytes.

        Example:
            >>> created = repository.record_backup_presence(  # doctest: +SKIP
            ...     3, registration, source, archive_path="books/a.epub",
            ... )


        :param workflow_id: Workflow ID to record on a newly inserted presence link.
        :param registration: Artifact reference and backup Store UUID whose membership is recorded.
        :param source: Source designation/provenance to retain for the member.
        :param archive_path: Exact member-name key; callers supply the intended normalized spelling.
        :param protected: Whether repository policy should protect the inserted link from deletion.
        :param immutable: Whether repository policy should prevent changes to the inserted link.
        :return: True for a newly inserted presence record; false when the Store/member key already exists.
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
        Remove workflow persistence subject to lifecycle and database constraints.

        This operation leaves physical artifact bytes untouched. FAILED counts as terminal and is
        eligible under the default lifecycle check. Associated-row cleanup and protected-link
        behavior follow the database schema; the interface does not promise cascading physical or
        Store removal.

        Example:
            >>> deleted = repository.delete_workflow(3)  # doctest: +SKIP


        :param workflow_id: Workflow identifier to remove, if present.
        :param require_terminal: Require a terminal stored status before deletion; false also permits DRAFT or RUNNING.
        :return: True when the workflow row is deleted, false when absent; lifecycle or database constraints can raise.
        """
        ...


__all__ = ["BackupWorkflowRepositoryAPI"]
