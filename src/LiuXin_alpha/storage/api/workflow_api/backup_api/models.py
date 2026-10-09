"""
Describe backup sources, persisted workflow declarations, checkpoints, outcomes, and pack estimates.

Frozen dataclasses enforce selected local invariants without reading bytes or resolving
catalogue references. They do not generally coerce enum/identifier types or deep-copy
collections. Member paths use the shared lexical normalizer; option pairs alone receive
canonical string conversion and ordering. Successful result flags are reported state,
not a fresh verification of the referenced artifact.

A BackupWorkflowDeclaration becomes durable workflow intent only after a repository
persists it. Checkpoints retain the same declaration so resumed work can be rejected
when its requested targets, sources, or build options no longer match that intent.
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
    Identify the concrete backup implementation that can interpret durable intent.

    SQUASHFS_PACK is the currently declared family. The enum records a discriminator; constructing
    it does not locate a builder or execute an archive tool.

    Example:
        >>> BackupWorkflowKind.SQUASHFS_PACK.value
        'squashfs_pack'
    """

    SQUASHFS_PACK = "squashfs_pack"


class BackupSourceKind(StrEnum):
    """
    Distinguish a local path designation from a routed Store Location.

    BackupSourceDeclaration requires these enum members themselves. Its identity checks do not
    coerce equivalent raw strings into a source kind.

    Example:
        >>> BackupSourceKind.STORE_LOCATION.value
        'store_location'
    """

    LOCAL_PATH = "local_path"
    STORE_LOCATION = "store_location"


class BackupWorkflowStepKind(StrEnum):
    """
    Label coarse backup milestones retained in checkpoint and result values.

    Staging, sealing, verification, registration, presence recording, and cleanup are independently
    named. A listed milestone is reported execution evidence, not proof of atomicity or idempotence.
    Concrete workflows may implement only some milestones; the SquashFS builder leaves registration
    and presence recording to separate services.

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
    Describe one intended archive member and optional catalogue provenance.

    Construction validates the source-kind/identifier pairing, nonnegative expected size, and
    normalized member path. Store references are inferred from a Location or checked for equality
    with it. It does not inspect bytes, resolve catalogue IDs, validate a supplied Digest object, or
    pin a physical object version. Local path spelling is retained, including whitespace; the path
    need not exist yet.

    Example:
        >>> source = BackupSourceDeclaration(
        ...     BackupSourceKind.STORE_LOCATION,
        ...     Location(UUID(int=1), "objects/42"),
        ...     archive_path="books/novel.epub",
        ...     expected_size=4,
        ... )
        >>> source.source_store_ref
        UUID('00000000-0000-0000-0000-000000000001')


    :ivar source_kind: LOCAL_PATH or STORE_LOCATION enum member selecting identifier validation.
    :ivar source_identifier: Nonempty local path string or the original routed Location.
    :ivar archive_path: Optional member spelling normalized by the shared lexical path helper; None leaves naming to the workflow.
    :ivar expected_size: Expected byte length, or None when unknown; only negativity is rejected here.
    :ivar expected_digest: Optional expected byte identity for later staging verification; retained without lookup.
    :ivar source_digital_asset_id: Optional atomic Asset provenance ID, retained without catalogue validation.
    :ivar source_replica_id: Optional source-copy provenance ID; does not itself pin the copy read later.
    :ivar source_store_ref: Optional Store UUID, inferred or cross-checked for a Location source.
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
        Validate the source discriminator and normalize a supplied archive path.

        LOCAL_PATH requires a nonempty string without trimming it; STORE_LOCATION requires Location
        and fills an omitted Store reference. Reject an unknown kind, a conflicting Store reference,
        or a negative expected size. Path normalization replaces backslashes, removes leading
        separators and empty/dot components, and rejects parent components or an empty result. It
        does not reject NULs, drive prefixes, or prove filesystem containment.

        Example:
            >>> BackupSourceDeclaration(BackupSourceKind.LOCAL_PATH, "")
            Traceback (most recent call last):
            ...
            ValueError: local backup source path must not be empty.


        :return: None after validation and any frozen-field normalization; invalid combinations raise TypeError or ValueError.
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
        Expose the stored identifier when it is a Location.

        The property checks the identifier type directly and returns the same object without
        routing, copying, or validating its current availability.

        Example:
            >>> source = BackupSourceDeclaration(
            ...     BackupSourceKind.STORE_LOCATION,
            ...     Location(UUID(int=1), "objects/42"),
            ... )
            >>> source.location
            Location(store_ref=UUID('00000000-0000-0000-0000-000000000001'), key='objects/42')


        :return: Original Location object for a managed source, otherwise None.
        """
        if isinstance(self.source_identifier, Location):
            return self.source_identifier
        return None


@dataclasses.dataclass(slots=True, frozen=True)
class BackupWorkflowDeclaration:
    """
    Collect persistable backup intent, ordered sources, and implementation-specific build settings.

    Frozen fields prevent reassignment but do not deep-copy caller-supplied sources or normalize
    them to a tuple. Only options are stringified, sorted, and tuple-collected. Construction permits
    no sources and does not check target existence, Store capabilities, workflow-kind type, flag
    types, or staging suitability. Nonblank names and string output targets retain their original
    whitespace.

    Example:
        >>> source = BackupSourceDeclaration(BackupSourceKind.LOCAL_PATH, "/books/a.epub")
        >>> declaration = BackupWorkflowDeclaration(
        ...     "nightly", BackupWorkflowKind.SQUASHFS_PACK,
        ...     Location(UUID(int=2), "packs/nightly.sqsh"),
        ...     sources=(source,),
        ... )
        >>> declaration.option_map()
        {}


    :ivar workflow_name: Nonblank display name; need not be unique and is not stripped.
    :ivar workflow_kind: Implementation-family discriminator, interpreted by the concrete workflow.
    :ivar output_target: Local output path or routed Location; only blank string targets are rejected here.
    :ivar sources: Ordered source designations; non-None archive paths must be unique.
    :ivar verify_after_build: Request the implementation's post-build verification; does not specify a universal full-content audit.
    :ivar cleanup_staging_after_success: Request staging cleanup after successful publication where implemented.
    :ivar staging_target: Optional staging path or Location, subject to concrete backend support.
    :ivar options: Key/value pairs canonicalized to a sorted tuple of strings with unique stringified keys.
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
        Check nonblank names, duplicate member paths, and canonical option keys.

        Only string output targets receive a blank check; staging targets and the workflow kind are
        not validated here. Ignore None member paths for uniqueness. Stringify and sort option
        pairs, then reject duplicate keys after conversion, so 1 and "1" collide. Sources remain the
        caller's original collection.

        Example:
            >>> BackupWorkflowDeclaration("", BackupWorkflowKind.SQUASHFS_PACK, "out.sqsh")
            Traceback (most recent call last):
            ...
            ValueError: workflow_name must not be empty.


        :return: None after storing canonical options; invalid names or duplicate paths/keys raise ValueError.
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
        # Options become part of persisted workflow intent. Canonical ordering
        # keeps equality stable across JSON object persistence and reconstruction.
        object.__setattr__(self, "options", normalized_options)

    def option_map(self) -> dict[str, str]:
        """
        Copy canonical option pairs into an independently mutable dictionary.

        Mutating the returned mapping does not change this declaration. Values are strings;
        interpretation of switches and defaults belongs to the workflow implementation.

        Example:
            >>> declaration = BackupWorkflowDeclaration(
            ...     "nightly", BackupWorkflowKind.SQUASHFS_PACK, "out.sqsh",
            ...     options=(("compression", "zstd"),),
            ... )
            >>> declaration.option_map()["compression"]
            'zstd'


        :return: Fresh dictionary mapping normalized option names to their string values.
        """
        return dict(self.options)


@dataclasses.dataclass(slots=True, frozen=True)
class BackupSourceStagingReport:
    """
    Record reported staging evidence for one designated source position.

    This value does not inspect the staged object or establish that the report matches a
    declaration. It checks nonnegative indices/counts, normalizes archive_path, and rejects an error
    attached to a truthy successful flag. A failed report may omit an error; digest evidence and
    success are independent fields.

    Example:
        >>> report = BackupSourceStagingReport(
        ...     0, "/books/a.epub", "books/a.epub", bytes_staged=42,
        ... )
        >>> report.ok
        True


    :ivar source_index: Zero-based declaration position; only a negative value is rejected here.
    :ivar source_identifier: Reported original local path or managed Location, retained without cross-checking.
    :ivar archive_path: Required member spelling normalized by the shared lexical path helper.
    :ivar staged_location: Optional route to staged bytes; availability is not checked.
    :ivar bytes_staged: Reported byte count, or None when unavailable; must not be negative.
    :ivar digest_verified: Optional report of digest verification: True, False, or None for unspecified evidence.
    :ivar ok: Reported staging success; a truthy value cannot accompany a non-None error.
    :ivar error: Optional failure detail; an empty string still conflicts with success.
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
        Validate nonnegative staging counters and normalize the member name.

        Reject any non-None error when ok is truthy, including an empty error string. No strict
        integer/bool type validation, report-to-source correspondence, or digest-evidence
        consistency check is performed.

        Example:
            >>> BackupSourceStagingReport(-1, "a", "a")
            Traceback (most recent call last):
            ...
            ValueError: source_index must not be negative.


        :return: None after path normalization and consistency checks; invalid values raise ValueError.
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
    Capture resumable position and reported evidence for one backup declaration.

    Counters must satisfy 0 <= staged_source_count <= next_source_index <= len(sources). Completed
    step labels must be unique, and the FAILED enum member requires a truthy last_error. Reports are
    not reconciled with the counters, step order is not enforced, and COMPLETE does not require
    output here. A checkpoint can describe failed work without proving the referenced staging data
    still exists.

    Example:
        >>> declaration = BackupWorkflowDeclaration(
        ...     "nightly", BackupWorkflowKind.SQUASHFS_PACK, "out.sqsh",
        ... )
        >>> checkpoint = BackupWorkflowCheckpoint(
        ...     declaration, WorkflowStatus.DRAFT,
        ... )
        >>> checkpoint.remaining_source_count
        0


    :ivar declaration: Intent whose ordered sources define the counter upper bound.
    :ivar status: Lifecycle classification; retained without enum coercion.
    :ivar workflow_id: Optional repository identity, not allocated or validated by this value.
    :ivar next_source_index: Position of the next source to process; a failed source can remain at this position.
    :ivar staged_source_count: Reported successfully staged count, no greater than next_source_index.
    :ivar source_reports: Collected source evidence, retained without counter or identity cross-checks.
    :ivar completed_steps: Unique reported milestone labels; completeness and chronological order are unchecked.
    :ivar output_artifact_reference: Optional published output reference; neither existence nor identity is verified.
    :ivar last_error: Required truthy detail for FAILED; may also be retained with other statuses.
    :ivar updated_at: Optional caller-supplied timestamp; no clock read or timezone validation occurs.
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
        Check source-position bounds, unique milestones, and required failure detail.

        Numeric comparisons do not enforce exact integer types. Only identity with
        WorkflowStatus.FAILED triggers the last_error requirement. Do not infer report completeness,
        artifact existence, or transition validity from successful construction.

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


        :return: None when the selected counters and failure fields are consistent; otherwise ValueError.
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
        Subtract the next-source position from the declaration's source count.

        This counts sources remaining from the resume position, not failed reports or unstaged
        bytes. An attempted source that failed without advancing the cursor remains included.

        Example:
            >>> source = BackupSourceDeclaration(BackupSourceKind.LOCAL_PATH, "/books/a")
            >>> declaration = BackupWorkflowDeclaration(
            ...     "nightly", BackupWorkflowKind.SQUASHFS_PACK, "out", (source,),
            ... )
            >>> BackupWorkflowCheckpoint(
            ...     declaration, WorkflowStatus.DRAFT,
            ... ).remaining_source_count
            1


        :return: Number of sources from next_source_index onward, using the current declaration collection length.
        """
        return len(self.declaration.sources) - self.next_source_index


@dataclasses.dataclass(slots=True, frozen=True)
class BackupWorkflowResult:
    """
    Describe a terminal backup outcome and its reported output and evidence.

    The status must expose a true terminal predicate. COMPLETE requires an output reference other
    than None; FAILED requires truthy error text. An empty output string can therefore satisfy
    construction and successful without naming an existing file. The final checkpoint, reports,
    milestone labels, and workflow ID are retained without mutual consistency checks.

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


    :ivar declaration: Backup intent associated with this result.
    :ivar status: Terminal lifecycle status: COMPLETE, FAILED, or CANCELLED.
    :ivar workflow_id: Optional durable workflow identity supplied by orchestration.
    :ivar output_artifact_reference: Reported output path or Location; required to be non-None for COMPLETE.
    :ivar source_reports: Reported staging evidence, retained without recounting or byte inspection.
    :ivar completed_steps: Reported milestones; uniqueness is not checked by this result type.
    :ivar last_error: Truthy error detail required for FAILED; other statuses may also carry it.
    :ivar final_checkpoint: Optional retained checkpoint; agreement with the result is not checked here.
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
        Require a terminal classification and the selected success/failure fields.

        Inspect status.terminal, then require a non-None output for the COMPLETE enum member and
        truthy error text for FAILED. CANCELLED needs neither. This validation does not compare
        final_checkpoint, verify artifact bytes, or require every source to have succeeded.

        Example:
            >>> declaration = BackupWorkflowDeclaration(
            ...     "nightly", BackupWorkflowKind.SQUASHFS_PACK, "out.sqsh",
            ... )
            >>> BackupWorkflowResult(declaration, WorkflowStatus.RUNNING)
            Traceback (most recent call last):
            ...
            ValueError: backup workflow result requires terminal status.


        :return: None for an accepted terminal value; invalid status/output/error combinations raise ValueError.
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
        Classify COMPLETE with a non-None output reference as successful.

        This checks stored fields only. It does not read the artifact, inspect source reports,
        require verification milestones, or reject an empty output string.

        Example:
            >>> declaration = BackupWorkflowDeclaration(
            ...     "nightly", BackupWorkflowKind.SQUASHFS_PACK, "out.sqsh",
            ... )
            >>> BackupWorkflowResult(
            ...     declaration, WorkflowStatus.COMPLETE,
            ...     output_artifact_reference="out.sqsh",
            ... ).successful
            True


        :return: True exactly when status is WorkflowStatus.COMPLETE and output_artifact_reference is not None.
        """
        return (
            self.status is WorkflowStatus.COMPLETE
            and self.output_artifact_reference is not None
        )


@dataclasses.dataclass(slots=True, frozen=True)
class BackupPackPlan:
    """
    Pair one backup declaration with its sequence number and estimated source size.

    The estimate is planning evidence, not a measured sealed-image size or an enforced byte ceiling.
    The source count must match the declaration, but no digest, target capacity, or size total is
    recomputed.

    Example:
        >>> declaration = BackupWorkflowDeclaration(
        ...     "pack-1", BackupWorkflowKind.SQUASHFS_PACK, "pack-1.sqsh",
        ... )
        >>> BackupPackPlan(1, declaration, 0, 0).estimated_size_bytes
        0


    :ivar pack_index: One-based ordinal within a planning result, required to be at least one.
    :ivar workflow_declaration: Intended artifact output and ordered member sources.
    :ivar source_count: Nonnegative count equal to the declaration source collection length.
    :ivar estimated_size_bytes: Nonnegative planning estimate, normally the sum of uncompressed source sizes.
    """

    pack_index: int
    workflow_declaration: BackupWorkflowDeclaration
    source_count: int
    estimated_size_bytes: int

    def __post_init__(self) -> None:
        """
        Check the pack ordinal, nonnegative estimates, and declaration source count.

        Comparisons do not coerce values or require strict integer types. No target-size threshold
        or relationship between the estimate and individual source sizes is enforced.

        Example:
            >>> declaration = BackupWorkflowDeclaration(
            ...     "pack", BackupWorkflowKind.SQUASHFS_PACK, "pack.sqsh",
            ... )
            >>> BackupPackPlan(0, declaration, 0, 0)
            Traceback (most recent call last):
            ...
            ValueError: pack_index must be positive.


        :return: None when ordinal and counts pass; inconsistent counts or negative values raise ValueError.
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
    Report the configured Store identity associated with a completed backup image.

    This passive value does not register, open, or verify a Store. Construction checks only a
    nonblank Store name and nonnegative presence-link count. The name retains whitespace, and
    references and identifiers are otherwise trusted.

    Example:
        >>> registration = BackupArtifactRegistration(
        ...     workflow_id=3, backup_store_ref=UUID(int=2),
        ...     backup_store_name="nightly-pack",
        ...     artifact_reference="/backups/nightly.sqsh",
        ... )
        >>> registration.presence_links_created
        0


    :ivar workflow_id: Optional durable workflow identity associated with registration.
    :ivar backup_store_ref: Stable UUID of the Store exposing the image contents.
    :ivar backup_store_name: Nonblank display name retained without trimming.
    :ivar artifact_reference: Path or Location naming the sealed image, without an existence check.
    :ivar presence_links_created: Count reported by the registry; a fresh call counts inserted links, while lookup counts current Store links.
    """

    workflow_id: WorkflowID | None
    backup_store_ref: StoreUUID
    backup_store_name: str
    artifact_reference: str | Location
    presence_links_created: int = 0

    def __post_init__(self) -> None:
        """
        Check the Store display name and nonnegative link count.

        The name must have non-whitespace content but is not stripped. Workflow IDs, Store UUIDs,
        artifact references, and strict count types are not validated here.

        Example:
            >>> BackupArtifactRegistration(None, "archive", "", "artifact.sqsh")
            Traceback (most recent call last):
            ...
            ValueError: backup_store_name must not be empty.


        :return: None for accepted values; a blank name or negative link count raises ValueError.
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
