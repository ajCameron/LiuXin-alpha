"""
Define backup source designation and reconstruction on the generic workflow API.

The public boundary separates resumable artifact creation from explicit checkpoint
persistence, completed-image Store registration, and sealed-Asset provenance recording.
"""

from __future__ import annotations

import abc
from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING, Self, cast

from LiuXin_alpha.storage.api.models import Location
from LiuXin_alpha.storage.api.storage_manager_api.models import (
    DigitalAssetID,
    DigitalAssetIngestResult,
    DigitalAssetRecord,
    DigitalAssetResolution,
)
from LiuXin_alpha.storage.api.workflow_api.backup_api.models import (
    BackupSourceDeclaration,
    BackupWorkflowCheckpoint,
    BackupWorkflowDeclaration,
    BackupWorkflowKind,
    BackupWorkflowResult,
)
from LiuXin_alpha.storage.api.workflow_api.base_api import StorageWorkflowAPI

if TYPE_CHECKING:
    from LiuXin_alpha.storage.api.storage_manager_api import StorageManagerAPI


class BackupWorkflowAPI(
    StorageWorkflowAPI[
        BackupWorkflowDeclaration,
        BackupWorkflowCheckpoint,
        BackupWorkflowResult,
    ],
    abc.ABC,
):
    """
    Specialize checkpointable execution for backup or archival artifact creation.

    Designation records ordered source intent before execution; implementations stage members, seal
    output, and expose checkpoints suitable for explicit repository persistence. Store registration
    and protected-presence recording have separate facades and are not automatically performed by
    the SquashFS implementation. This ABC supplies no repository, background worker, or rollback
    mechanism.

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
        """
        Identify the backup implementation family that interprets this workflow and its options.

        Example:
            >>> kind = workflow.workflow_kind  # doctest: +SKIP


        :return: BackupWorkflowKind discriminator, currently SQUASHFS_PACK for the supported builder.
        """
        ...

    @abc.abstractmethod
    def designate_local_path(
        self,
        source_path: str,
        *,
        archive_path: str | None = None,
        asset: (
            DigitalAssetID
            | DigitalAssetRecord
            | DigitalAssetIngestResult
            | DigitalAssetResolution
            | None
        ) = None,
    ) -> BackupSourceDeclaration:
        """
        Add a local source to the workflow's designated member order.

        The SquashFS implementation requires DRAFT, expands/resolves the path, and records the
        current size when it is a file. Without an Asset identity a nonexistent path can be
        designated and fail later during staging. A supplied Asset is refreshed through the manager,
        checked against the observed size, and supplies an expected digest. This operation does not
        copy or hash source bytes itself.

        Example:
            >>> source = workflow.designate_local_path(  # doctest: +SKIP
            ...     "/incoming/a.epub", archive_path="books/a.epub",
            ... )


        :param source_path: Local filesystem source spelling, resolved by the concrete workflow.
        :param archive_path: Optional member name; SquashFS uses the source basename when omitted or empty, then normalizes and checks uniqueness.
        :param asset: Optional atomic Asset ID, record, ingest result, or resolution supplying catalogue identity and expected byte evidence.
        :return: Source declaration appended to current intent; invalid state, duplicate names, or identity mismatches may raise.
        """
        ...

    @abc.abstractmethod
    def designate_location(
        self,
        source_location: Location,
        *,
        archive_path: str | None = None,
        asset: (
            DigitalAssetID
            | DigitalAssetRecord
            | DigitalAssetIngestResult
            | DigitalAssetResolution
            | None
        ) = None,
    ) -> BackupSourceDeclaration:
        """
        Add a routed Store source and any available catalogue identity to intended members.

        The SquashFS implementation requires DRAFT. With a manager it reads current file info,
        consults non-deleted Replicas at that exact Location, and rejects conflicting explicit Asset
        identity or size. An inferred Asset supplies its recorded digest. Without a manager an
        unassociated Location can be designated, but later staging needs routing. Designation does
        not pin a backend object version or guarantee future bytes remain unchanged.

        Example:
            >>> source = workflow.designate_location(  # doctest: +SKIP
            ...     Location(UUID(int=1), "objects/42"),
            ...     archive_path="books/a.epub",
            ... )


        :param source_location: Managed Location naming the source object; retained as the physical read route.
        :param archive_path: Optional member spelling; SquashFS falls back to a zero-padded source ordinal when falsey.
        :param asset: Optional atomic Asset input; when omitted, the implementation may infer identity from a non-deleted Replica at the Location.
        :return: Appended source declaration with captured expectations and any inferred Asset/Replica references.
        """
        ...

    @classmethod
    def create(
        cls,
        workflow_name: str,
        workflow_kind: BackupWorkflowKind,
        output_target: str | Location,
        *,
        sources: Iterable[BackupSourceDeclaration] = (),
        verify_after_build: bool = True,
        cleanup_staging_after_success: bool = False,
        staging_target: str | Location | None = None,
        options: Mapping[str, str] | Iterable[tuple[str, str]] = (),
        storage_manager: StorageManagerAPI | None = None,
    ) -> Self:
        """
        Construct backup intent from ordinary values and delegate to from_declaration.

        Materialize sources and options once while preserving their order; the declaration
        canonicalizes options and validates selected intent fields. Concrete reconstruction remains
        the sole owner of workflow-family validation, staging setup, and borrowed manager binding.

        Example:
            >>> workflow = ConcreteWorkflow.create(  # doctest: +SKIP
            ...     "nightly", BackupWorkflowKind.SQUASHFS_PACK, "nightly.sqsh",
            ... )


        :param workflow_name: Human-readable workflow name retained by the declaration.
        :param workflow_kind: Implementation-family discriminator interpreted by the concrete class.
        :param output_target: Local path or routed Location for the completed artifact.
        :param sources: Ordered source declarations materialized into durable intent.
        :param verify_after_build: Whether the concrete workflow should verify after building.
        :param cleanup_staging_after_success: Whether supported staging should be removed after success.
        :param staging_target: Optional local path or routed Location used for staging.
        :param options: Mapping or ordered key/value pairs for implementation-specific settings.
        :param storage_manager: Optional borrowed manager forwarded to from_declaration.
        :return: The workflow instance returned by the concrete declaration reconstruction method.
        """

        if isinstance(options, Mapping):
            option_values = tuple(cast(Mapping[str, str], options).items())
        else:
            option_values = tuple(
                cast(Iterable[tuple[str, str]], cast(object, options))
            )
        return cls.from_declaration(
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
            storage_manager=storage_manager,
        )

    @classmethod
    @abc.abstractmethod
    def from_declaration(
        cls,
        declaration: BackupWorkflowDeclaration,
        *,
        storage_manager: StorageManagerAPI | None = None,
    ) -> Self:
        """
        Construct fresh executable state from persisted backup intent.

        This starts from intent rather than resuming progress or saving repository state. The
        concrete class validates its family and options and may initialize local staging. SquashFS
        requires local staging and a persistent staging path for a Location output; accepting a
        declaration value alone does not prove those requirements are met.

        Example:
            >>> workflow = ConcreteWorkflow.from_declaration(  # doctest: +SKIP
            ...     declaration, storage_manager=manager,
            ... )


        :param declaration: Targets, ordered sources, and implementation options to reconstruct.
        :param storage_manager: Optional borrowed manager for managed sources, output routing, and catalogue lookup; local-only work may omit it.
        :return: New instance of the concrete workflow class initialized from the declaration.
        """
        ...

    @classmethod
    @abc.abstractmethod
    def from_checkpoint(
        cls,
        checkpoint: BackupWorkflowCheckpoint,
        *,
        storage_manager: StorageManagerAPI | None = None,
    ) -> Self:
        """
        Reconstruct executable state at a persisted resume position.

        SquashFS accepts DRAFT, RUNNING, and FAILED and restores the status, counters, reports,
        milestones, output, and error. It retains FAILED: run_to_completion() immediately reports
        that failure until run_next() explicitly retries it. Referenced staging files must still be
        usable; reconstruction is not proof that an earlier process completed an unrecorded physical
        operation.

        Example:
            >>> workflow = ConcreteWorkflow.from_checkpoint(  # doctest: +SKIP
            ...     checkpoint, storage_manager=manager,
            ... )


        :param checkpoint: Resumable state whose declaration and retained evidence are restored.
        :param storage_manager: Optional borrowed routing/catalogue manager required by managed operations after reconstruction.
        :return: Concrete instance retaining the checkpoint state; unsupported or non-resumable checkpoints may raise.
        """
        ...


__all__ = ["BackupWorkflowAPI"]
