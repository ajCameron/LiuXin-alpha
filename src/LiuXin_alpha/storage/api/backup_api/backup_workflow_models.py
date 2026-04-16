"""Backup workflow value objects.

These objects describe backup workflow intent, progress, and results. They are
meant to be serializable/persistable later, without forcing that persistence
shape into raw store plugins today.
"""

from __future__ import annotations

import dataclasses

from enum import StrEnum


class BackupWorkflowKind(StrEnum):
    """High-level workflow family."""

    SQUASHFS_PACK = "squashfs_pack"


class BackupWorkflowStatus(StrEnum):
    """Lifecycle state for one workflow execution."""

    DRAFT = "draft"
    RUNNING = "running"
    FAILED = "failed"
    COMPLETE = "complete"
    CANCELLED = "cancelled"


class BackupSourceKind(StrEnum):
    """Source types that a workflow may designate."""

    LOCAL_PATH = "local_path"
    STORE_LOCATION = "store_location"


class BackupWorkflowStepKind(StrEnum):
    """Coarse execution steps useful for resume/checkpoint state."""

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
    the database."""

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
        return dict(self.options)


@dataclasses.dataclass(slots=True, frozen=True)
class BackupSourceResult:
    """Outcome of staging one designated source."""

    source_index: int
    source_identifier: str
    archive_path: str
    staged_location_url: str | None = None
    ok: bool = True
    error: str | None = None


@dataclasses.dataclass(slots=True, frozen=True)
class BackupWorkflowResumeState:
    """Durable execution checkpoint for resuming a workflow later."""

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
    """Terminal outcome for one workflow execution."""

    spec: BackupWorkflowSpec
    status: BackupWorkflowStatus
    output_artifact_url: str | None = None
    source_results: tuple[BackupSourceResult, ...] = ()
    completed_steps: tuple[BackupWorkflowStepKind, ...] = ()
    last_error: str | None = None
    resume_state: BackupWorkflowResumeState | None = None
