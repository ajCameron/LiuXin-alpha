"""
Backup workflow intent, checkpoint, result, and planning values.
"""

from __future__ import annotations

import dataclasses

from datetime import datetime
from enum import StrEnum
from uuid import UUID

from LiuXin_alpha.storage.api.models import Digest, Location, StoreUUID
from LiuXin_alpha.storage.api.storage_manager_api.models import (
    ReplicaID,
    DigitalAssetID,
)
from LiuXin_alpha.storage.api.workflow_api.models import WorkflowID, WorkflowStatus
import LiuXin_alpha.storage.utils.workflow as workflow_utils


class BackupWorkflowKind(StrEnum):
    """
    Stable backup workflow implementation families.

    Example:
        >>> BackupWorkflowKind.SQUASHFS_PACK.value
        'squashfs_pack'
    """

    SQUASHFS_PACK = "squashfs_pack"


class BackupSourceKind(StrEnum):
    """
    Kinds of source a backup workflow may designate.

    Example:
        >>> BackupSourceKind.STORE_LOCATION.value
        'store_location'
    """

    LOCAL_PATH = "local_path"
    STORE_LOCATION = "store_location"


class BackupWorkflowStepKind(StrEnum):
    """
    Coarse idempotent steps represented in resume state.

    Example:
        >>> BackupWorkflowStepKind.VERIFY_ARTIFACT.value
        'verify_artifact'
    """

    STAGE_SOURCES = "stage_sources"
    SEAL_ARTIFACT = "seal_artifact"
    VERIFY_ARTIFACT = "verify_artifact"
    REGISTER_ARTIFACT = "register_artifact"
    RECORD_PRESENCE = "record_presence"
    CLEANUP = "cleanup"


@dataclasses.dataclass(slots=True, frozen=True)
class BackupSourceDeclaration:
    """
    Declarative designation of one source included in a backup artifact.

    Local sources use a path string; managed sources use a storage ``Location``.
    Optional ids preserve catalogue provenance without exposing it to stores or
    drivers.

    Example:
        >>> source = BackupSourceDeclaration(
        ...     BackupSourceKind.STORE_LOCATION,
        ...     Location(UUID(int=1), "objects/42"),
        ...     archive_path="books/novel.epub",
        ...     expected_size=4,
        ... )
        >>> source.source_store_ref
        UUID('00000000-0000-0000-0000-000000000001')
    """

    source_kind: BackupSourceKind
    source_identifier: str | Location
    archive_path: str | None = None
    expected_size: int | None = None
    expected_digest: Digest | None = None
    source_digital_asset_id: DigitalAssetID | None = None
    source_replica_id: ReplicaID | None = None
    source_store_ref: StoreUUID | None = None

    def __post_init__(self) -> None:
        """
        Validate identifier type, size, archive path, and store identity.

        Example:
            >>> BackupSourceDeclaration(BackupSourceKind.LOCAL_PATH, "")
            Traceback (most recent call last):
            ...
            ValueError: local backup source path must not be empty.


        :return:
        """
        if self.source_kind is BackupSourceKind.LOCAL_PATH:
            if not isinstance(self.source_identifier, str) or not self.source_identifier:
                raise ValueError("local backup source path must not be empty.")
        elif self.source_kind is BackupSourceKind.STORE_LOCATION:
            if not isinstance(self.source_identifier, Location):
                raise TypeError("store-location backup sources require a Location.")
            if self.source_store_ref is None:
                object.__setattr__(
                    self,
                    "source_store_ref",
                    self.source_identifier.store_ref,
                )
            elif self.source_store_ref != self.source_identifier.store_ref:
                raise ValueError("source_store_ref must match the source Location.")
        else:
            raise ValueError(f"unknown backup source kind: {self.source_kind!r}.")
        if self.expected_size is not None and self.expected_size < 0:
            raise ValueError("expected_size must not be negative.")
        if self.archive_path is not None:
            object.__setattr__(
                self,
                "archive_path",
                workflow_utils.normalize_archive_path(self.archive_path),
            )

    @property
    def location(self) -> Location | None:
        """
        Return the managed Location, or ``None`` for a local path source.

        Example:
            >>> source = BackupSourceDeclaration(
            ...     BackupSourceKind.STORE_LOCATION,
            ...     Location(UUID(int=1), "objects/42"),
            ... )
            >>> source.location
            Location(store_ref=UUID('00000000-0000-0000-0000-000000000001'), key='objects/42')


        :return:
        """
        if isinstance(self.source_identifier, Location):
            return self.source_identifier
        return None


@dataclasses.dataclass(slots=True, frozen=True)
class BackupWorkflowDeclaration:
    """
    Immutable durable intent for one backup workflow.

    ``output_target`` and ``staging_target`` may be local implementation paths
    or routed store Locations.  Workflow code, rather than a raw driver,
    decides how staging and final publication are coordinated.

    Example:
        >>> source = BackupSourceDeclaration(BackupSourceKind.LOCAL_PATH, "/books/a.epub")
        >>> declaration = BackupWorkflowDeclaration(
        ...     "nightly", BackupWorkflowKind.SQUASHFS_PACK,
        ...     Location(UUID(int=2), "packs/nightly.sqsh"),
        ...     sources=(source,),
        ... )
        >>> declaration.option_map()
        {}
    """

    workflow_name: str
    workflow_kind: BackupWorkflowKind
    output_target: str | Location
    sources: tuple[BackupSourceDeclaration, ...] = ()
    verify_after_build: bool = True
    cleanup_staging_after_success: bool = False
    staging_target: str | Location | None = None
    options: tuple[tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        """
        Validate names, targets, unique archive paths, and option keys.

        Example:
            >>> BackupWorkflowDeclaration("", BackupWorkflowKind.SQUASHFS_PACK, "out.sqsh")
            Traceback (most recent call last):
            ...
            ValueError: workflow_name must not be empty.


        :return:
        """
        if not self.workflow_name.strip():
            raise ValueError("workflow_name must not be empty.")
        if isinstance(self.output_target, str) and not self.output_target.strip():
            raise ValueError("output_target must not be empty.")
        paths = tuple(
            source.archive_path
            for source in self.sources
            if source.archive_path is not None
        )
        if len(paths) != len(set(paths)):
            raise ValueError("backup archive paths must be unique.")
        normalized_options = tuple(
            sorted((str(key), str(value)) for key, value in self.options)
        )
        option_keys = tuple(key for key, _value in normalized_options)
        if len(option_keys) != len(set(option_keys)):
            raise ValueError("backup workflow option keys must be unique.")
        # Options are map-like durable intent.  Canonical ordering keeps
        # equality stable across JSON object persistence and reconstruction.
        object.__setattr__(self, "options", normalized_options)

    def option_map(self) -> dict[str, str]:
        """
        Return implementation-specific options as a mutable mapping.

        Example:
            >>> declaration = BackupWorkflowDeclaration(
            ...     "nightly", BackupWorkflowKind.SQUASHFS_PACK, "out.sqsh",
            ...     options=(("compression", "zstd"),),
            ... )
            >>> declaration.option_map()["compression"]
            'zstd'


        :return:
        """
        return dict(self.options)


@dataclasses.dataclass(slots=True, frozen=True)
class BackupSourceStagingReport:
    """
    Outcome of staging one designated source.

    Example:
        >>> report = BackupSourceStagingReport(
        ...     0, "/books/a.epub", "books/a.epub", bytes_staged=42,
        ... )
        >>> report.ok
        True
    """

    source_index: int
    source_identifier: str | Location
    archive_path: str
    staged_location: Location | None = None
    bytes_staged: int | None = None
    digest_verified: bool | None = None
    ok: bool = True
    error: str | None = None

    def __post_init__(self) -> None:
        """
        Validate source position, byte counts, path, and error consistency.

        Example:
            >>> BackupSourceStagingReport(-1, "a", "a")
            Traceback (most recent call last):
            ...
            ValueError: source_index must not be negative.


        :return:
        """
        if self.source_index < 0:
            raise ValueError("source_index must not be negative.")
        if self.bytes_staged is not None and self.bytes_staged < 0:
            raise ValueError("bytes_staged must not be negative.")
        object.__setattr__(
            self,
            "archive_path",
            workflow_utils.normalize_archive_path(self.archive_path),
        )
        if self.ok and self.error is not None:
            raise ValueError("a successful source result must not contain an error.")


@dataclasses.dataclass(slots=True, frozen=True)
class BackupWorkflowCheckpoint:
    """
    Durable execution checkpoint from which backup work can resume.

    Example:
        >>> declaration = BackupWorkflowDeclaration(
        ...     "nightly", BackupWorkflowKind.SQUASHFS_PACK, "out.sqsh",
        ... )
        >>> checkpoint = BackupWorkflowCheckpoint(
        ...     declaration, WorkflowStatus.DRAFT,
        ... )
        >>> checkpoint.remaining_source_count
        0
    """

    declaration: BackupWorkflowDeclaration
    status: WorkflowStatus
    workflow_id: WorkflowID | None = None
    next_source_index: int = 0
    staged_source_count: int = 0
    source_reports: tuple[BackupSourceStagingReport, ...] = ()
    completed_steps: tuple[BackupWorkflowStepKind, ...] = ()
    output_artifact_reference: str | Location | None = None
    last_error: str | None = None
    updated_at: datetime | None = None

    def __post_init__(self) -> None:
        """
        Validate counters, step uniqueness, and terminal error state.

        Example:
            >>> declaration = BackupWorkflowDeclaration(
            ...     "nightly", BackupWorkflowKind.SQUASHFS_PACK, "out.sqsh",
            ... )
            >>> BackupWorkflowCheckpoint(
            ...     declaration, WorkflowStatus.DRAFT, next_source_index=-1,
            ... )
            Traceback (most recent call last):
            ...
            ValueError: workflow source counters must not be negative.


        :return:
        """
        if self.next_source_index < 0 or self.staged_source_count < 0:
            raise ValueError("workflow source counters must not be negative.")
        if self.next_source_index > len(self.declaration.sources):
            raise ValueError("next_source_index exceeds the designated source count.")
        if self.staged_source_count > self.next_source_index:
            raise ValueError("staged_source_count exceeds next_source_index.")
        if len(self.completed_steps) != len(set(self.completed_steps)):
            raise ValueError("completed workflow steps must be unique.")
        if self.status is WorkflowStatus.FAILED and not self.last_error:
            raise ValueError("failed workflow state requires last_error.")

    @property
    def remaining_source_count(self) -> int:
        """
        Return the number of designated sources not yet attempted.

        Example:
            >>> source = BackupSourceDeclaration(BackupSourceKind.LOCAL_PATH, "/books/a")
            >>> declaration = BackupWorkflowDeclaration(
            ...     "nightly", BackupWorkflowKind.SQUASHFS_PACK, "out", (source,),
            ... )
            >>> BackupWorkflowCheckpoint(
            ...     declaration, WorkflowStatus.DRAFT,
            ... ).remaining_source_count
            1


        :return:
        """
        return len(self.declaration.sources) - self.next_source_index


@dataclasses.dataclass(slots=True, frozen=True)
class BackupWorkflowResult:
    """
    Terminal outcome for one backup workflow execution.

    Example:
        >>> declaration = BackupWorkflowDeclaration(
        ...     "nightly", BackupWorkflowKind.SQUASHFS_PACK, "out.sqsh",
        ... )
        >>> result = BackupWorkflowResult(
        ...     declaration, WorkflowStatus.COMPLETE,
        ...     output_artifact_reference="out.sqsh",
        ... )
        >>> result.successful
        True
    """

    declaration: BackupWorkflowDeclaration
    status: WorkflowStatus
    workflow_id: WorkflowID | None = None
    output_artifact_reference: str | Location | None = None
    source_reports: tuple[BackupSourceStagingReport, ...] = ()
    completed_steps: tuple[BackupWorkflowStepKind, ...] = ()
    last_error: str | None = None
    final_checkpoint: BackupWorkflowCheckpoint | None = None

    def __post_init__(self) -> None:
        """
        Require a terminal status and consistent success or failure data.

        Example:
            >>> declaration = BackupWorkflowDeclaration(
            ...     "nightly", BackupWorkflowKind.SQUASHFS_PACK, "out.sqsh",
            ... )
            >>> BackupWorkflowResult(declaration, WorkflowStatus.RUNNING)
            Traceback (most recent call last):
            ...
            ValueError: backup workflow result requires terminal status.


        :return:
        """
        if not self.status.terminal:
            raise ValueError("backup workflow result requires terminal status.")
        if (
            self.status is WorkflowStatus.COMPLETE
            and self.output_artifact_reference is None
        ):
            raise ValueError("completed backup workflow requires an output artifact.")
        if self.status is WorkflowStatus.FAILED and not self.last_error:
            raise ValueError("failed backup workflow result requires last_error.")

    @property
    def successful(self) -> bool:
        """
        Return whether the workflow completed with an output artifact.

        Example:
            >>> declaration = BackupWorkflowDeclaration(
            ...     "nightly", BackupWorkflowKind.SQUASHFS_PACK, "out.sqsh",
            ... )
            >>> BackupWorkflowResult(
            ...     declaration, WorkflowStatus.COMPLETE,
            ...     output_artifact_reference="out.sqsh",
            ... ).successful
            True


        :return:
        """
        return (
            self.status is WorkflowStatus.COMPLETE
            and self.output_artifact_reference is not None
        )


@dataclasses.dataclass(slots=True, frozen=True)
class BackupPackPlan:
    """
    One size-bounded artifact plan produced from store inventory.

    Example:
        >>> declaration = BackupWorkflowDeclaration(
        ...     "pack-1", BackupWorkflowKind.SQUASHFS_PACK, "pack-1.sqsh",
        ... )
        >>> BackupPackPlan(1, declaration, 0, 0).estimated_size_bytes
        0
    """

    pack_index: int
    workflow_declaration: BackupWorkflowDeclaration
    source_count: int
    estimated_size_bytes: int

    def __post_init__(self) -> None:
        """
        Validate positive indices and non-negative source and size counts.

        Example:
            >>> declaration = BackupWorkflowDeclaration(
            ...     "pack", BackupWorkflowKind.SQUASHFS_PACK, "pack.sqsh",
            ... )
            >>> BackupPackPlan(0, declaration, 0, 0)
            Traceback (most recent call last):
            ...
            ValueError: pack_index must be positive.


        :return:
        """
        if self.pack_index < 1:
            raise ValueError("pack_index must be positive.")
        if self.source_count < 0 or self.estimated_size_bytes < 0:
            raise ValueError("pack source and size counts must not be negative.")
        if self.source_count != len(self.workflow_declaration.sources):
            raise ValueError(
                "source_count must match workflow_declaration.sources."
            )


@dataclasses.dataclass(slots=True, frozen=True)
class BackupArtifactRegistration:
    """
    Completed artifact registered as a readable configured Store.

    Example:
        >>> registration = BackupArtifactRegistration(
        ...     workflow_id=3, backup_store_ref=UUID(int=2),
        ...     backup_store_name="nightly-pack",
        ...     artifact_reference="/backups/nightly.sqsh",
        ... )
        >>> registration.presence_links_created
        0
    """

    workflow_id: WorkflowID | None
    backup_store_ref: StoreUUID
    backup_store_name: str
    artifact_reference: str | Location
    presence_links_created: int = 0

    def __post_init__(self) -> None:
        """
        Validate store naming and the created-link count.

        Example:
            >>> BackupArtifactRegistration(None, "archive", "", "artifact.sqsh")
            Traceback (most recent call last):
            ...
            ValueError: backup_store_name must not be empty.


        :return:
        """
        if not self.backup_store_name.strip():
            raise ValueError("backup_store_name must not be empty.")
        if self.presence_links_created < 0:
            raise ValueError("presence_links_created must not be negative.")


__all__ = [
    "BackupPackPlan",
    "BackupSourceKind",
    "BackupSourceStagingReport",
    "BackupSourceDeclaration",
    "BackupWorkflowKind",
    "BackupWorkflowResult",
    "BackupWorkflowCheckpoint",
    "BackupWorkflowDeclaration",
    "BackupWorkflowStepKind",
    "BackupArtifactRegistration",
]
