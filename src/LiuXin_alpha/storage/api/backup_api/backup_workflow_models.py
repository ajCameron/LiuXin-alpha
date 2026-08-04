"""Backup workflow value objects.

These objects describe backup workflow intent, progress, and results. They are
meant to be serializable/persistable later, without forcing that persistence
shape into raw store plugins today.

Examples:
    Compose immutable source and workflow specifications::

        source = BackupSourceSpec(BackupSourceKind.LOCAL_PATH, "/books/a.epub")
        spec = BackupWorkflowSpec(
            "nightly", BackupWorkflowKind.SQUASHFS_PACK, "/backups/nightly.sqfs",
            sources=(source,),
        )
"""

from __future__ import annotations

import dataclasses

from enum import StrEnum


class BackupWorkflowKind(StrEnum):
    """High-level workflow family.

    Examples:
        Persist enum values as stable strings::

            kind = BackupWorkflowKind.SQUASHFS_PACK
            assert str(kind) == "squashfs_pack"
    """

    SQUASHFS_PACK = "squashfs_pack"


class BackupWorkflowStatus(StrEnum):
    """Lifecycle state for one workflow execution.

    Examples:
        Test whether a workflow finished successfully::

            finished = state.status is BackupWorkflowStatus.COMPLETE
    """

    DRAFT = "draft"
    RUNNING = "running"
    FAILED = "failed"
    COMPLETE = "complete"
    CANCELLED = "cancelled"


class BackupSourceKind(StrEnum):
    """Source types that a workflow may designate.

    Examples:
        Label a managed location distinctly from a local path::

            kind = BackupSourceKind.STORE_LOCATION
    """

    LOCAL_PATH = "local_path"
    STORE_LOCATION = "store_location"


class BackupWorkflowStepKind(StrEnum):
    """Coarse execution steps useful for resume/checkpoint state.

    Examples:
        Record that artifact verification is complete::

            completed = (BackupWorkflowStepKind.VERIFY_ARTIFACT,)
    """

    STAGE_SOURCES = "stage_sources"
    SEAL_ARTIFACT = "seal_artifact"
    VERIFY_ARTIFACT = "verify_artifact"
    CLEANUP = "cleanup"


@dataclasses.dataclass(slots=True, frozen=True)
class BackupSourceSpec:
    """Declarative designation of one source to include in a backup artifact.

    `source_identifier` remains the durable source locator used by the workflow
    itself. Optional ids let later orchestration layers tie the source back to a
    DB-tracked file / replica row without forcing raw store plugins to know about
    the database.

    Examples:
        Designate a local EPUB and its name inside the artifact::

            source = BackupSourceSpec(
                source_kind=BackupSourceKind.LOCAL_PATH,
                source_identifier="/books/a.epub",
                archive_path="library/a.epub",
            )
    """

    source_kind: BackupSourceKind
    source_identifier: str
    archive_path: str | None = None
    expected_size: int | None = None
    expected_hash: str | None = None
    source_file_id: int | None = None
    source_asset_replica_id: int | None = None
    source_store_id: int | None = None


@dataclasses.dataclass(slots=True, frozen=True)
class BackupWorkflowSpec:
    """Declarative backup workflow specification.

    This is the durable intent/configuration object. It deliberately excludes
    mutable progress counters and live errors.

    Examples:
        Build a specification from designated sources::

            spec = BackupWorkflowSpec(
                workflow_name="nightly",
                workflow_kind=BackupWorkflowKind.SQUASHFS_PACK,
                output_url="/backups/nightly.sqfs",
                sources=(source,),
            )
    """

    workflow_name: str
    workflow_kind: BackupWorkflowKind
    output_url: str
    sources: tuple[BackupSourceSpec, ...] = ()
    verify_after_build: bool = True
    cleanup_staging_after_success: bool = False
    staging_root: str | None = None
    options: tuple[tuple[str, str], ...] = ()

    def option_map(self) -> dict[str, str]:
        """Return workflow options as a mutable lookup mapping.

        Examples:
            Resolve an implementation-specific compression option::

                spec = BackupWorkflowSpec(
                    "nightly", BackupWorkflowKind.SQUASHFS_PACK, "out.sqfs",
                    options=(("compression", "zstd"),),
                )
                assert spec.option_map()["compression"] == "zstd"
        """
        return dict(self.options)


@dataclasses.dataclass(slots=True, frozen=True)
class BackupSourceResult:
    """Outcome of staging one designated source.

    Examples:
        Record a successful staging result::

            result = BackupSourceResult(0, "/books/a.epub", "books/a.epub")
            assert result.ok
    """

    source_index: int
    source_identifier: str
    archive_path: str
    staged_location_url: str | None = None
    ok: bool = True
    error: str | None = None


@dataclasses.dataclass(slots=True, frozen=True)
class BackupWorkflowResumeState:
    """Durable execution checkpoint for resuming a workflow later.

    Examples:
        Initialise a checkpoint before staging begins::

            state = BackupWorkflowResumeState(spec, BackupWorkflowStatus.DRAFT)
    """

    spec: BackupWorkflowSpec
    status: BackupWorkflowStatus
    next_source_index: int = 0
    staged_source_count: int = 0
    source_results: tuple[BackupSourceResult, ...] = ()
    completed_steps: tuple[BackupWorkflowStepKind, ...] = ()
    output_artifact_url: str | None = None
    last_error: str | None = None


@dataclasses.dataclass(slots=True, frozen=True)
class BackupWorkflowResult:
    """Terminal outcome for one workflow execution.

    Examples:
        Represent a completed artifact::

            result = BackupWorkflowResult(
                spec,
                BackupWorkflowStatus.COMPLETE,
                output_artifact_url="/backups/nightly.sqfs",
            )
    """

    spec: BackupWorkflowSpec
    status: BackupWorkflowStatus
    output_artifact_url: str | None = None
    source_results: tuple[BackupSourceResult, ...] = ()
    completed_steps: tuple[BackupWorkflowStepKind, ...] = ()
    last_error: str | None = None
    resume_state: BackupWorkflowResumeState | None = None
