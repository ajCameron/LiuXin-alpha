"""
Coordinate bounded adoption of local files and immutable container members.

Discovery classifies filenames and limited magic bytes; backend inventory validates
recognized containers later. Real runs adopt source files before expanding a FIFO
of container candidates. Built-in declarations cover SquashFS, ZIP, TAR, RAR, 7z,
and ISO; terminal ebook containers remain loose files unless expansion is enabled.

Limits govern selected counts, logical member bytes, materialization reservations,
and cooperative elapsed-time checkpoints. They do not interrupt arbitrary calls
or bound every directory allocation. Nested containers use an explicitly selected
local writable CACHE Store. Existing cache reuse checks stat size rather than
recomputing a digest. Persistence comes from the borrowed manager, and successful
writes survive subsequent callback, member, or container failures.

Example:
    >>> report = ingest_mixed_local_tree(manager, source_root, discovery_only=True)  # doctest: +SKIP
"""

from __future__ import annotations

import dataclasses
import logging
import math
import mimetypes
import os
import time
from collections import Counter, deque
from collections.abc import Callable, Iterable, Mapping
from pathlib import Path, PurePosixPath
from typing import TypedDict, Unpack, final
from urllib.parse import unquote_to_bytes, urlparse
from uuid import UUID, uuid4, uuid5

from LiuXin_alpha.storage.api import errors as storage_errors
from LiuXin_alpha.storage.api import models as storage_models
from LiuXin_alpha.storage.api import storage_manager_api as manager_api
from LiuXin_alpha.storage.api import store_api
from LiuXin_alpha.storage.utils.backend_registry import DEFAULT_BACKEND_REGISTRY
from LiuXin_alpha.utils.logging import get_compat_logger
from LiuXin_alpha.utils.text.safe_path_to_name import safe_path_to_name

_GIB = 1024 * 1024 * 1024
_OPERATION_NAMESPACE = UUID("51808ce6-c0a8-5f87-bb93-3844742822cc")
_WORKFLOW_VERSION = "mixed-local-ingest-v1"
_LOGGER = get_compat_logger(__name__)
_DEBUG_LOG_EVENTS = frozenset(
    {
        "member_adopted",
        "source_file_adopted",
        "source_file_classified",
        "symlink_skipped",
    }
)
_TERMINAL_EBOOK_SUFFIXES = frozenset(
    {
        ".epub",
        ".cbz",
        ".cbr",
        ".mobi",
        ".azw",
        ".azw3",
        ".pdf",
        ".fb2",
        ".fbz",
        ".lit",
        ".pdb",
        ".docx",
        ".odt",
        ".htmlz",
    }
)
_EBOOK_CONTAINER_FORMATS = {".epub": "zip", ".cbz": "zip", ".cbr": "rar"}

ProgressCallback = Callable[[str, Mapping[str, object]], None]
CancellationCallback = Callable[[], bool]
RangeReader = Callable[[int, int], bytes]
SourceMetadataFactory = Callable[
    [Path, str, "ContainerHandler | None"], manager_api.DigitalAssetMetadata
]


@dataclasses.dataclass(slots=True, frozen=True)
class MixedIngestBudget:
    """
    Configure orchestration and backend ceilings for one mixed-ingest run.

    Counts and sizes are checked for positivity, not strict integer types. Wall time is polled
    cooperatively and does not interrupt an in-flight filesystem, manager, or backend call. These
    limits do not bound every allocation: a directory is materialized before source-count checks and
    source file bytes have no total ingest-byte cap here.

    Example:
        >>> MixedIngestBudget(max_members=5).max_members
        5


    :ivar max_source_files: Maximum regular source paths retained by discovery.
    :ivar max_containers: Maximum scheduled/classified containers, including later duplicates or failures.
    :ivar max_container_depth: Maximum expansion depth, with top-level containers at depth one.
    :ivar max_members: Run-wide count of size/path/budget-accepted member attempts.
    :ivar max_members_per_container: Per-container count limit also passed to backend inventory policy.
    :ivar max_member_bytes: Maximum declared/stat member size in bytes.
    :ivar max_container_expanded_bytes: Per-container logical member-byte ceiling, also passed to backends.
    :ivar max_total_expanded_bytes: Cumulative logical member-byte ceiling across containers and depths.
    :ivar max_container_expansion_ratio: Finite ratio ceiling against max(container_size, 1), plus backend-specific checks.
    :ivar max_materialized_bytes: Run-wide reservation/retained-cache byte ceiling for newly materialized nested containers.
    :ivar max_temporary_bytes: Single nested-container materialization limit and selected backend spool-member ceiling.
    :ivar max_path_depth: Maximum POSIX path-component count for member keys and backend parsing.
    :ivar max_path_bytes: Maximum UTF-8 surrogatepass byte length for member keys; selected backends receive it too.
    :ivar max_wall_time_s: Finite positive elapsed-time limit observed at cooperative halt checkpoints.
    :ivar max_issues: Maximum retained aggregate issues; reaching the cap halts subsequent work.
    """

    max_source_files: int = 1_000_000
    max_containers: int = 10_000
    max_container_depth: int = 8
    max_members: int = 1_000_000
    max_members_per_container: int = 100_000
    max_member_bytes: int = 4 * _GIB
    max_container_expanded_bytes: int = 64 * _GIB
    max_total_expanded_bytes: int = 256 * _GIB
    max_container_expansion_ratio: float = 200.0
    max_materialized_bytes: int = 64 * _GIB
    max_temporary_bytes: int = 4 * _GIB
    max_path_depth: int = 256
    max_path_bytes: int = 65_535
    max_wall_time_s: float = 24 * 60 * 60
    max_issues: int = 10_000

    def __post_init__(self) -> None:
        """
        Reject nonpositive count/size/depth limits and invalid finite ratio/time settings.

        The range comparisons do not enforce integer types on count/size fields; unsupported operand
        types can raise their ordinary comparison errors.

        Example:
            >>> MixedIngestBudget(max_members=0)
            Traceback (most recent call last):
            ...
            ValueError: max_members must be positive.


        :return: None after validation; invalid ranges raise ValueError.
        """
        for name in (
            "max_source_files",
            "max_containers",
            "max_container_depth",
            "max_members",
            "max_members_per_container",
            "max_member_bytes",
            "max_container_expanded_bytes",
            "max_total_expanded_bytes",
            "max_materialized_bytes",
            "max_temporary_bytes",
            "max_path_depth",
            "max_path_bytes",
            "max_issues",
        ):
            if getattr(self, name) < 1:
                raise ValueError(f"{name} must be positive.")
        if (
            not math.isfinite(self.max_container_expansion_ratio)
            or self.max_container_expansion_ratio < 1
        ):
            raise ValueError(
                "max_container_expansion_ratio must be finite and at least 1."
            )
        if not math.isfinite(self.max_wall_time_s) or self.max_wall_time_s <= 0:
            raise ValueError("max_wall_time_s must be finite and positive.")


@dataclasses.dataclass(slots=True, frozen=True)
class ContainerHandler:
    """
    Map suffix and byte-signature heuristics to one immutable-container backend.

    Construction checks selected text/suffix/signature constraints without verifying backend
    availability or complete format validity. Empty signature/suffix collections are allowed, and
    names/kinds/protocols retain surrounding whitespace after the nonblank check.

    Example:
        >>> default_container_handlers()[1].matches_name('BOOK.ZIP')
        True


    :ivar format_name: Nonblank format label used in reports, matching, and backend-option selection.
    :ivar backend_kind: Nonblank registry selector, resolved when the Store is created/reused.
    :ivar protocol: Nonblank protocol label passed to backed-Store creation.
    :ivar suffixes: Lowercased unique dotted suffixes; order is preserved.
    :ivar magic_signatures: Ordered nonnegative byte offsets and nonempty signature byte sequences.
    """

    format_name: str
    backend_kind: str
    protocol: str
    suffixes: tuple[str, ...]
    magic_signatures: tuple[tuple[int, bytes], ...]

    def __post_init__(self) -> None:
        """
        Validate nonblank labels and selected signature constraints, then lowercase suffixes.

        Reject duplicate normalized suffixes and those not starting with a dot; no backend lookup,
        maximum offset, or mandatory nonempty collection check occurs.

        Example:
            >>> ContainerHandler('zip', 'zip_readonly', 'zip', ('.ZIP',), ()).suffixes
            ('.zip',)


        :return: None after replacing the frozen suffix field; invalid constraints raise ValueError.
        """
        if not self.format_name.strip():
            raise ValueError("handler format_name must not be empty.")
        if not self.backend_kind.strip():
            raise ValueError("handler backend_kind must not be empty.")
        if not self.protocol.strip():
            raise ValueError("handler protocol must not be empty.")
        normalized = tuple(suffix.lower() for suffix in self.suffixes)
        if any(not suffix.startswith(".") for suffix in normalized):
            raise ValueError("handler suffixes must start with '.'.")
        if len(normalized) != len(set(normalized)):
            raise ValueError("handler suffixes must be unique.")
        for offset, signature in self.magic_signatures:
            if offset < 0 or not signature:
                raise ValueError("handler magic signatures must be non-empty.")
        object.__setattr__(self, "suffixes", normalized)

    def matches_name(self, name: str) -> bool:
        """
        Check whether the lowercased name ends in any configured suffix.

        Do not strip whitespace, inspect bytes, or validate a path.

        Example:
            >>> default_container_handlers()[2].matches_name('archive.TAR.GZ')
            True


        :param name: Filename text compared case-insensitively with stored suffixes.
        :return: True when at least one suffix matches the end of the name.
        """
        lowered = name.lower()
        return any(lowered.endswith(suffix) for suffix in self.suffixes)

    def matches_probe(self, probe: bytes) -> bool:
        """
        Compare each configured signature with its slice of the supplied probe bytes.

        Short slices fail equality naturally; no read or structural archive validation occurs.

        Example:
            >>> default_container_handlers()[0].matches_probe(b'hsqs')
            True


        :param probe: Available leading file bytes containing any signature offsets to inspect.
        :return: True if any configured signature exactly matches its probe slice.
        """
        return any(
            probe[offset : offset + len(signature)] == signature
            for offset, signature in self.magic_signatures
        )


@dataclasses.dataclass(slots=True, frozen=True)
class ContainerMemberContext:
    """
    Carry technical parent/ancestry facts to a member metadata callback.

    Frozen fields are retained without validating paths, depth, or parent identity. Display paths
    may contain synthetic !/ ancestry separators and are not necessarily filesystem paths.

    Example:
        >>> context = ContainerMemberContext("pack.zip", "zip", 1, manager_api.DigitalAssetID(7), ("pack.zip",))
        >>> context.depth
        1


    :ivar container_path: Display path of the immediate containing archive.
    :ivar format_name: Selected handler format for that archive.
    :ivar depth: Expansion depth of the parent, starting at one.
    :ivar parent_digital_asset_id: Asset identity of the containing archive bytes.
    :ivar container_chain: Display ancestry including the immediate container.
    """

    container_path: str
    format_name: str
    depth: int
    parent_digital_asset_id: manager_api.DigitalAssetID
    container_chain: tuple[str, ...]


MemberMetadataFactory = Callable[
    [ContainerMemberContext, storage_models.StoreInventoryEntry], manager_api.DigitalAssetMetadata
]


def default_container_handlers() -> tuple[ContainerHandler, ...]:
    """
    Build the ordered built-in SquashFS, ZIP, TAR, RAR, 7z, and ISO handler tuple.

    Each call constructs fresh immutable declarations. Name/magic recognition is preliminary; this
    does not probe installed extractors or validate archive content. Compressed TAR variants rely on
    suffixes here.

    Example:
        >>> tuple(handler.format_name for handler in default_container_handlers())
        ('squashfs', 'zip', 'tar', 'rar', '7z', 'iso')


    :return: Ordered handler declarations used when no truthy custom collection is supplied.
    """

    return (
        ContainerHandler(
            "squashfs",
            "squashfs_readonly",
            "squashfs",
            (".sfs", ".sqfs", ".sqsh", ".squashfs"),
            ((0, b"hsqs"),),
        ),
        ContainerHandler(
            "zip",
            "zip_readonly",
            "zip",
            (".zip",),
            ((0, b"PK\x03\x04"), (0, b"PK\x05\x06"), (0, b"PK\x07\x08")),
        ),
        ContainerHandler(
            "tar",
            "tar_readonly",
            "tar",
            (
                ".tar",
                ".tar.gz",
                ".tgz",
                ".tar.bz2",
                ".tbz",
                ".tbz2",
                ".tar.xz",
                ".txz",
            ),
            ((257, b"ustar"),),
        ),
        ContainerHandler(
            "rar",
            "rar_readonly",
            "rar",
            (".rar",),
            ((0, b"Rar!\x1a\x07\x00"), (0, b"Rar!\x1a\x07\x01\x00")),
        ),
        ContainerHandler(
            "7z",
            "sevenzip_readonly",
            "7z",
            (".7z",),
            ((0, b"7z\xbc\xaf'\x1c"),),
        ),
        ContainerHandler(
            "iso",
            "iso_readonly",
            "iso",
            (".iso", ".udf"),
            ((32_769, b"CD001"),),
        ),
    )


@dataclasses.dataclass(slots=True, frozen=True)
class MixedIngestIssue:
    """
    Record one failure or limit decision with optional container ancestry.

    These unchecked frozen values do not roll back effects or redact diagnostics. fatal controls
    severity metadata; appending a fatal issue alone does not automatically invoke the halt
    operation.

    Example:
        >>> MixedIngestIssue('member', 'pack.zip', 'unreadable', 'OSError').fatal
        False


    :ivar stage: Boundary or limit category assigned by the caller.
    :ivar path: Source/container display path retained verbatim.
    :ivar message: Diagnostic text, potentially containing raw error details.
    :ivar error_type: Exception class name or synthetic limit identifier.
    :ivar container_chain: Optional ordered display ancestry.
    :ivar member_path: Optional member key at the failure boundary.
    :ivar fatal: Whether the issue is labeled fatal for reporting/log severity.
    """

    stage: str
    path: str
    message: str
    error_type: str
    container_chain: tuple[str, ...] = ()
    member_path: str | None = None
    fatal: bool = False


@dataclasses.dataclass(slots=True, frozen=True)
class ContainerIngestReport:
    """
    Describe one classified, attempted, or deduplicated container without revalidating its counters.

    Reports can reflect partial writes and conservative byte accounting. A Store reference does not
    establish completed enumeration; a duplicate can be ok without being processed.

    Example:
        >>> ContainerIngestReport('pack.zip', 'zip', 1).processed
        False


    :ivar path: Top-level or nested display path.
    :ivar format_name: Selected format label.
    :ivar depth: Expansion depth, with top-level containers at one.
    :ivar digital_asset_id: Container Asset identity, or None for classification-only records.
    :ivar source_replica_id: Adopted source/member Replica identity, or None.
    :ivar store_ref: Obtained backed-Store configuration identity, or None before setup succeeds.
    :ivar store_created: Whether a new backed-Store configuration was created.
    :ivar members_discovered: Member attempts accepted by size/path/byte checks, including later adoption errors.
    :ivar members_adopted: Successful manager member-adoption receipts.
    :ivar member_assets_created: New member Asset receipts adjusted for cached existing Locations.
    :ivar member_replicas_created: New member Replica receipts adjusted for cached existing Locations.
    :ivar nested_containers_discovered: Recognized nested candidates, including unscheduled/depth-limited candidates.
    :ivar expanded_bytes: Logical member sizes charged before adoption, including later failures.
    :ivar materialized_bytes: New nested-container reservation retained when a matching-size cache Replica remains.
    :ivar duplicate_of: Prior expansion display path, or selected ancestor path for a detected cycle.
    :ivar truncated: Whether this container was cut short or nested depth expansion was refused.
    :ivar issues: Container/member/limit issues retained by this report.
    """

    path: str
    format_name: str
    depth: int
    digital_asset_id: int | None = None
    source_replica_id: int | None = None
    store_ref: UUID | None = None
    store_created: bool = False
    members_discovered: int = 0
    members_adopted: int = 0
    member_assets_created: int = 0
    member_replicas_created: int = 0
    nested_containers_discovered: int = 0
    expanded_bytes: int = 0
    materialized_bytes: int = 0
    duplicate_of: str | None = None
    truncated: bool = False
    issues: tuple[MixedIngestIssue, ...] = ()

    @property
    def processed(self) -> bool:
        """
        Test for an obtained Store identity and absence of a duplicate marker.

        This does not inspect issues or truncation and need not equal a successful expansion.
        Aggregate containers_processed is incremented separately by the coordinator.

        Example:
            >>> ContainerIngestReport('pack.zip', 'zip', 1, store_ref=UUID(int=1), truncated=True).processed
            True


        :return: True exactly when store_ref is non-None and duplicate_of is None.
        """
        return self.store_ref is not None and self.duplicate_of is None

    @property
    def ok(self) -> bool:
        """
        Require no issues and no truncation on this record.

        Classification-only and duplicate records can pass without any Store or adopted member.

        Example:
            >>> ContainerIngestReport('pack.zip', 'zip', 1).ok
            True


        :return: Whether this report has neither issues nor truncation.
        """
        return not self.issues and not self.truncated


@dataclasses.dataclass(slots=True, frozen=True)
class MixedIngestReport:
    """
    Freeze one run's classification/adoption observations and resource accounting.

    Persistence depends on the supplied manager. Logical expanded-byte charges are not measured
    transfer bytes; nested levels can account for the same underlying content more than once.
    Counters and child reports are accepted without cross-validation.

    Example:
        >>> report.members_adopted  # doctest: +SKIP


    :ivar run_id: Caller-supplied or generated run correlation UUID.
    :ivar source_root: Expanded/resolved source directory as text.
    :ivar discovery_only: Whether only source classification was requested.
    :ivar source_store_ref: Source Store identity, or None before setup/discovery-only execution.
    :ivar source_store_created: Whether the run created its source Store configuration.
    :ivar files_examined: Regular source paths retained by discovery, not necessarily all subsequently classified/adopted.
    :ivar files_adopted: Successful source-file adoption receipts.
    :ivar loose_files: Classified/adopted top-level files without a selected container handler.
    :ivar skipped_symlinks: Symlink entries skipped by filesystem discovery.
    :ivar top_level_containers: Top-level files recognized as containers, including scheduling-limit refusals.
    :ivar containers_discovered: Containers admitted by classification/scheduling before duplicate elimination.
    :ivar containers_processed: Container Stores obtained for inventory attempts, including later inventory failures.
    :ivar containers_deduplicated: Previously expanded identities and detected ancestor cycles skipped by the queue.
    :ivar members_discovered: Run-wide budget-accepted member attempts before adoption.
    :ivar members_adopted: Successful member-adoption receipts across containers.
    :ivar assets_created: Source/member Asset creation receipts adjusted for cached Location presence.
    :ivar replicas_created: Source/member Replica creation receipts adjusted for cached Location presence.
    :ivar expanded_bytes: Cumulative logical member sizes charged before adoption, including later failures.
    :ivar materialized_bytes: Accounted new nested-container cache bytes, excluding already-present matching-size cache Replicas.
    :ivar recognized_formats: Format/count pairs sorted by format label.
    :ivar containers: Ordered container classification/attempt/duplicate reports.
    :ivar issues: Bounded aggregate issue tuple, possibly shorter than all child issue lists.
    :ivar truncated: Whether any tracked limit/halt marked the run incomplete.
    :ivar halt_reason: First retained run halt reason, or None.
    :ivar elapsed_s: Nonnegative clock difference at report construction in seconds.
    """

    run_id: UUID
    source_root: str
    discovery_only: bool
    source_store_ref: UUID | None
    source_store_created: bool
    files_examined: int
    files_adopted: int
    loose_files: int
    skipped_symlinks: int
    top_level_containers: int
    containers_discovered: int
    containers_processed: int
    containers_deduplicated: int
    members_discovered: int
    members_adopted: int
    assets_created: int
    replicas_created: int
    expanded_bytes: int
    materialized_bytes: int
    recognized_formats: tuple[tuple[str, int], ...] = ()
    containers: tuple[ContainerIngestReport, ...] = ()
    issues: tuple[MixedIngestIssue, ...] = ()
    truncated: bool = False
    halt_reason: str | None = None
    elapsed_s: float = 0.0

    @property
    def ok(self) -> bool:
        """
        Check aggregate issues, truncation, and halt reason without inspecting nested reports.

        The coordinator normally propagates child issues, but a manually constructed inconsistent
        record can still pass this predicate.

        Example:
            >>> report.ok  # doctest: +SKIP


        :return: True when aggregate issues is empty, truncated is false, and halt_reason is None.
        """
        return not self.issues and not self.truncated and self.halt_reason is None


@dataclasses.dataclass(slots=True, frozen=True)
class _ContainerCandidate:
    """
    Retain one adopted container candidate queued for possible expansion.

    This unchecked frozen record carries already-selected identities and ancestry. It does not prove
    the bytes are a valid archive or that a backing Store exists.

    Example:
        >>> candidate.depth  # doctest: +SKIP


    :ivar display_path: Human-readable source or !/-joined nested path.
    :ivar filename: Name used for Store naming and format-related display.
    :ivar handler: Selected declaration controlling backend creation/options.
    :ivar digital_asset_id: Adopted container Asset identity.
    :ivar source_replica_id: Preferred adopted source/member Replica identity.
    :ivar size_bytes: Container Asset size used for ratio and materialization accounting.
    :ivar depth: Expansion depth beginning at one.
    :ivar ancestry: Ancestor SHA-256 values used to reject content cycles.
    :ivar chain: Display ancestry including this container.
    :ivar top_level: Whether the bytes are directly available from the source tree rather than a nested member.
    """
    display_path: str
    filename: str
    handler: ContainerHandler
    digital_asset_id: manager_api.DigitalAssetID
    source_replica_id: manager_api.ReplicaID
    size_bytes: int
    depth: int
    ancestry: tuple[str, ...]
    chain: tuple[str, ...]
    top_level: bool


@dataclasses.dataclass(slots=True)
class _Discovery:
    """
    Accumulate filesystem source paths and scan diagnostics before classification/adoption.

    Example:
        >>> _Discovery().files
        []


    :ivar files: Regular paths in traversal order until a completed scan sorts them.
    :ivar skipped_symlinks: Count of observed symlink entries skipped.
    :ivar issues: Discovery/count-limit diagnostics with a local issue cap.
    :ivar truncated: Whether discovery halted early or hit a count/issue limit.
    """
    files: list[Path] = dataclasses.field(default_factory=list)
    skipped_symlinks: int = 0
    issues: list[MixedIngestIssue] = dataclasses.field(default_factory=list)
    truncated: bool = False


@dataclasses.dataclass(slots=True)
class _RunState:
    """
    Maintain mutable counters, reports, and halt state for one coordinator invocation.

    Lists and counters are fresh per instance. The coordinator updates this state around external
    effects; it is not a transaction, persistence object, or thread-safe progress snapshot.

    Example:
        >>> _RunState(0.0, UUID(int=1), Path("/source")).members_adopted
        0


    :ivar started: Initial injected clock reading for elapsed-time comparisons.
    :ivar run_id: Correlation identity assigned before execution.
    :ivar source_root: Resolved source directory.
    :ivar source_store_ref: Source Store identity, or None before setup/discovery-only execution.
    :ivar source_store_created: Whether the run created its source Store configuration.
    :ivar files_examined: Regular source paths retained by discovery, not necessarily all subsequently classified/adopted.
    :ivar files_adopted: Successful source-file adoption receipts.
    :ivar loose_files: Classified/adopted top-level files without a selected container handler.
    :ivar skipped_symlinks: Symlink entries skipped by filesystem discovery.
    :ivar top_level_containers: Top-level files recognized as containers, including scheduling-limit refusals.
    :ivar containers_discovered: Containers admitted by classification/scheduling before duplicate elimination.
    :ivar containers_processed: Container Stores obtained for inventory attempts, including later inventory failures.
    :ivar containers_deduplicated: Previously expanded identities and detected ancestor cycles skipped by the queue.
    :ivar members_discovered: Run-wide budget-accepted member attempts before adoption.
    :ivar members_adopted: Successful member-adoption receipts across containers.
    :ivar assets_created: Source/member Asset creation receipts adjusted for cached Location presence.
    :ivar replicas_created: Source/member Replica creation receipts adjusted for cached Location presence.
    :ivar expanded_bytes: Cumulative logical member sizes charged before adoption, including later failures.
    :ivar materialized_bytes: Accounted new nested-container cache bytes, excluding already-present matching-size cache Replicas.
    :ivar formats: Mutable admitted-container format counter.
    :ivar containers: Ordered report list accumulated during classification/queue processing.
    :ivar issues: Bounded aggregate issue list.
    :ivar truncated: Whether a limit or halt marked incomplete work.
    :ivar halt_reason: First retained halt reason, or None.
    :ivar container_limit_reported: Whether the scheduling limit has already emitted its single aggregate issue.
    """
    started: float
    run_id: UUID
    source_root: Path
    source_store_ref: UUID | None = None
    source_store_created: bool = False
    files_examined: int = 0
    files_adopted: int = 0
    loose_files: int = 0
    skipped_symlinks: int = 0
    top_level_containers: int = 0
    containers_discovered: int = 0
    containers_processed: int = 0
    containers_deduplicated: int = 0
    members_discovered: int = 0
    members_adopted: int = 0
    assets_created: int = 0
    replicas_created: int = 0
    expanded_bytes: int = 0
    materialized_bytes: int = 0
    formats: Counter[str] = dataclasses.field(default_factory=Counter)
    containers: list[ContainerIngestReport] = dataclasses.field(default_factory=list)
    issues: list[MixedIngestIssue] = dataclasses.field(default_factory=list)
    truncated: bool = False
    halt_reason: str | None = None
    container_limit_reported: bool = False


@final
class MixedFormatIngestCoordinator:
    """
    Coordinate local source adoption, immutable container views, and cumulative expansion limits.

    Discovery-only mode classifies top-level files without manager writes or cache creation. Real
    runs adopt source files first, then process containers breadth-first. Nested expansion requires
    a configured local writable cache when encountered. The caller owns the manager; no all-run
    transaction or cleanup of successful prior writes is added.

    The active-run marker rejects ordinary reentry but is not a synchronization lock. Location
    caches and resolved materialization Store selection persist across calls. Budgets are checked at
    defined boundaries, not by asynchronously interrupting arbitrary backend operations.

    Example:
        >>> report = MixedFormatIngestCoordinator(manager, materialization_root=cache_root).ingest(source_root)  # doctest: +SKIP
    """

    def __init__(
        self,
        manager: manager_api.StorageManagerAPI,
        *,
        budget: MixedIngestBudget | None = None,
        handlers: Iterable[ContainerHandler] | None = None,
        recursive_filesystem: bool = True,
        recurse_containers: bool = True,
        expand_ebook_containers: bool = False,
        continue_on_error: bool = True,
        verify_source_files: bool = False,
        verify_members: bool = False,
        materialization_store_ref: UUID | None = None,
        materialization_root: str | os.PathLike[str] | None = None,
        unsquashfs_exe: str = "unsquashfs",
        rar_extractor_exe: str | None = None,
        backend_timeout_s: float = 60.0,
        progress_callback: ProgressCallback | None = None,
        cancellation_callback: CancellationCallback | None = None,
        source_metadata_factory: SourceMetadataFactory | None = None,
        member_metadata_factory: MemberMetadataFactory | None = None,
        log_checkpoint_every: int = 1_000,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        """
        Validate configuration relationships, materialize handlers, and retain policy/callback
        state.

        An empty list/tuple selects defaults because it is falsey; a truthy iterable yielding no
        handlers is rejected. Enforce format/suffix uniqueness, finite positive backend timeout,
        mutually exclusive cache selectors, and positive checkpoint frequency before int conversion.
        No scan, Store startup, or manager load is performed. Mutable referenced values are not
        deep-copied.

        Example:
            >>> coordinator = MixedFormatIngestCoordinator(manager, recurse_containers=False)  # doctest: +SKIP


        :param manager: Caller-owned manager providing Store/Asset/Replica operations.
        :param budget: Truthy budget value, or None for built-in ceilings.
        :param handlers: Truthy iterable of unique-format/unique-suffix handlers, otherwise built-in declarations.
        :param recursive_filesystem: Whether discovery descends into nonsymlink directories.
        :param recurse_containers: Whether adopted members can schedule nested container views.
        :param expand_ebook_containers: Whether terminal ebook suffixes may be identified as containers.
        :param continue_on_error: Whether ordinary handled source/member/container errors become issues instead of re-raising.
        :param verify_source_files: Verification policy forwarded to source adoption.
        :param verify_members: Verification policy forwarded to member adoption.
        :param materialization_store_ref: Existing local writable CACHE Store identity, mutually exclusive with a root at construction.
        :param materialization_root: Local cache path expanded/resolved at construction; created lazily for nested work.
        :param unsquashfs_exe: Executable selector supplied to SquashFS backend options.
        :param rar_extractor_exe: Optional extractor selector supplied to RAR backend options.
        :param backend_timeout_s: Finite positive per-call backend ceiling, bounded by the wall-time budget rather than its remaining time.
        :param progress_callback: Optional synchronous event/details observer; exceptions follow the enclosing boundary.
        :param cancellation_callback: Optional boolean cancellation poll; its exceptions propagate.
        :param source_metadata_factory: Truthy source metadata hook, otherwise the built-in name/provenance factory.
        :param member_metadata_factory: Truthy member metadata hook, otherwise the built-in context/hint factory.
        :param log_checkpoint_every: Positive value int-converted for periodic successful-source/member logging.
        :param clock: Callable providing elapsed-time readings in seconds, normally time.monotonic.
        :return: None after creating an idle coordinator with fresh Location cache state.
        """
        if backend_timeout_s <= 0 or not math.isfinite(backend_timeout_s):
            raise ValueError("backend_timeout_s must be finite and positive.")
        if materialization_store_ref is not None and materialization_root is not None:
            raise ValueError(
                "supply materialization_store_ref or materialization_root, not both."
            )
        if log_checkpoint_every < 1:
            raise ValueError("log_checkpoint_every must be positive.")
        selected_handlers = tuple(handlers or default_container_handlers())
        if not selected_handlers:
            raise ValueError("at least one container handler is required.")
        format_names = tuple(handler.format_name for handler in selected_handlers)
        if len(format_names) != len(set(format_names)):
            raise ValueError("container handler format names must be unique.")
        suffixes = [suffix for handler in selected_handlers for suffix in handler.suffixes]
        if len(suffixes) != len(set(suffixes)):
            raise ValueError("container handler suffixes must be globally unique.")

        self.manager = manager
        self.budget = budget or MixedIngestBudget()
        self.handlers = selected_handlers
        self.recursive_filesystem = bool(recursive_filesystem)
        self.recurse_containers = bool(recurse_containers)
        self.expand_ebook_containers = bool(expand_ebook_containers)
        self.continue_on_error = bool(continue_on_error)
        self.verify_source_files = bool(verify_source_files)
        self.verify_members = bool(verify_members)
        self.materialization_store_ref = materialization_store_ref
        self.materialization_root = (
            None
            if materialization_root is None
            else Path(materialization_root).expanduser().resolve(strict=False)
        )
        self.unsquashfs_exe = str(unsquashfs_exe)
        self.rar_extractor_exe = (
            None if rar_extractor_exe is None else str(rar_extractor_exe)
        )
        self.backend_timeout_s = float(backend_timeout_s)
        self.progress_callback = progress_callback
        self.cancellation_callback = cancellation_callback
        self.source_metadata_factory = source_metadata_factory or _source_metadata
        self.member_metadata_factory = member_metadata_factory or _member_metadata
        self.log_checkpoint_every = int(log_checkpoint_every)
        self.clock = clock
        self._replica_locations_by_store: dict[UUID, set[storage_models.Location]] = {}
        self._active_run_id: UUID | None = None

    def ingest(
        self,
        source_root: str | os.PathLike[str],
        *,
        discovery_only: bool = False,
        run_id: UUID | None = None,
    ) -> MixedIngestReport:
        """
        Assign a run correlation identity, reject active reentry, and execute one ingest attempt.

        Log uncaught BaseException with the active context, re-raise, and clear the active marker in
        finally. Logging/stringification can themselves fail and mask an earlier error. The marker
        is not a concurrency lock, and caller-supplied run_id is not independently validated here.

        Example:
            >>> report = coordinator.ingest(source_root, discovery_only=True)  # doctest: +SKIP


        :param source_root: Existing directory path forwarded to execution after run identity is assigned.
        :param discovery_only: Request source classification without manager writes/cache creation.
        :param run_id: Correlation UUID, or None to generate a fresh uuid4.
        :return: Completed report; uncaught failures escape after attempted diagnostic logging.
        """

        if self._active_run_id is not None:
            raise storage_errors.StoragePreconditionFailed(
                "one MixedFormatIngestCoordinator cannot run concurrently"
            )
        effective_run_id = uuid4() if run_id is None else run_id
        self._active_run_id = effective_run_id
        try:
            return self._ingest_run(
                source_root,
                discovery_only=discovery_only,
                run_id=effective_run_id,
            )
        except BaseException as error:
            self._log_exception(
                "run_unhandled_exception",
                error,
                level=logging.CRITICAL,
                source_root=os.fspath(source_root),
                discovery_only=discovery_only,
            )
            raise
        finally:
            self._active_run_id = None

    def _ingest_run(
        self,
        source_root: str | os.PathLike[str],
        *,
        discovery_only: bool,
        run_id: UUID,
    ) -> MixedIngestReport:
        """
        Discover source paths, then classify only or adopt sources and process queued containers.

        Resolve/validate the root, check cache-root containment for real runs, and emit run_started
        before scanning. Discovery precedes source Store creation; a recorded early halt returns
        without that creation. Otherwise sources are adopted before container expansion. Location
        observations adjust creation counters without skipping adoption.

        Top-level setup/progress failures can escape. Source-file Exceptions are logged/reported or
        re-raised by policy, including callback failures after writes. No rollback is added. A
        container-count refusal stops scheduling extra containers while source adoption can continue
        until another halt condition.

        Example:
            >>> report = coordinator._ingest_run(source_root, discovery_only=False, run_id=run_id)  # doctest: +SKIP


        :param source_root: Path expanded/resolved and required to be an existing directory.
        :param discovery_only: Classify selected paths without creating manager metadata.
        :param run_id: Already-selected identity stored in the run state.
        :return: Final report after queue processing/classification and the complete event.
        """

        root = Path(source_root).expanduser().resolve(strict=False)
        if not root.exists():
            raise FileNotFoundError(str(root))
        if not root.is_dir():
            raise NotADirectoryError(str(root))
        if not discovery_only:
            self._validate_materialization_root(root)
        state = _RunState(started=self.clock(), run_id=run_id, source_root=root)
        self._progress(
            "run_started",
            source_root=str(root),
            discovery_only=discovery_only,
            recursive_filesystem=self.recursive_filesystem,
            recurse_containers=self.recurse_containers,
            expand_ebook_containers=self.expand_ebook_containers,
            continue_on_error=self.continue_on_error,
            verify_source_files=self.verify_source_files,
            verify_members=self.verify_members,
            materialization_store_ref=(
                None
                if self.materialization_store_ref is None
                else str(self.materialization_store_ref)
            ),
            materialization_root=(
                None
                if self.materialization_root is None
                else str(self.materialization_root)
            ),
            handlers=tuple(handler.format_name for handler in self.handlers),
            budget=dataclasses.asdict(self.budget),
        )
        discovery = self._discover(root, state)
        state.files_examined = len(discovery.files)
        state.skipped_symlinks = discovery.skipped_symlinks
        for issue in discovery.issues:
            self._record_issue(state, issue)
        state.truncated = discovery.truncated
        self._progress(
            "discovery_complete",
            source_root=str(root),
            files_examined=state.files_examined,
            skipped_symlinks=state.skipped_symlinks,
            discovery_only=discovery_only,
        )

        if discovery_only:
            self._classify_discovery(discovery.files, state)
            return self._finish(state, discovery_only=True)
        if state.halt_reason is not None:
            return self._finish(state, discovery_only=False)

        source_configuration, created = self._ensure_source_store(root)
        state.source_store_ref = source_configuration.store_uuid
        state.source_store_created = created
        self._progress(
            "source_store_ready",
            store_ref=str(source_configuration.store_uuid),
            store_name=source_configuration.store_name,
            store_kind=source_configuration.store_kind,
            created=created,
        )
        queue: deque[_ContainerCandidate] = deque()
        source_store = self.manager.get_store(source_configuration.store_uuid)
        for source_number, path in enumerate(discovery.files, start=1):
            if self._should_halt(state):
                break
            relative = path.relative_to(root).as_posix()
            try:
                handler = self._identify_path(path)
                location = source_store.locate(relative)
                info = source_store.stat(location)
                existed = self._has_replica_at(location)
                result = self.manager.adopt_location(
                    location,
                    operation_id=_operation_id(
                        "source",
                        str(source_configuration.store_uuid),
                        relative,
                        info.version or str(info.size),
                    ),
                    metadata=self.source_metadata_factory(path, relative, handler),
                    replica_mode=manager_api.ReplicaMode.UNMANAGED,
                    verify=self.verify_source_files,
                )
                state.files_adopted += 1
                state.assets_created += int(result.asset_created and not existed)
                state.replicas_created += int(result.replica_created and not existed)
                self._remember_replica(result.replica_record.location)
                self._progress(
                    "source_file_adopted",
                    path=str(path),
                    relative_path=relative,
                    size_bytes=result.asset_record.size_bytes,
                    format=(None if handler is None else handler.format_name),
                    digital_asset_id=int(result.asset_record.digital_asset_id),
                    replica_id=int(result.replica_record.replica_id),
                    asset_created=bool(result.asset_created and not existed),
                    replica_created=bool(result.replica_created and not existed),
                    source_number=source_number,
                    source_count=len(discovery.files),
                )
                if handler is None:
                    state.loose_files += 1
                else:
                    state.top_level_containers += 1
                    self._schedule_container(
                        queue,
                        state,
                        _ContainerCandidate(
                            display_path=str(path),
                            filename=path.name,
                            handler=handler,
                            digital_asset_id=result.asset_record.digital_asset_id,
                            source_replica_id=result.replica_record.replica_id,
                            size_bytes=result.asset_record.size_bytes,
                            depth=1,
                            ancestry=(),
                            chain=(str(path),),
                            top_level=True,
                        ),
                    )
                if source_number % self.log_checkpoint_every == 0:
                    self._progress(
                        "source_checkpoint",
                        files_adopted=state.files_adopted,
                        files_examined=state.files_examined,
                        loose_files=state.loose_files,
                        containers_discovered=state.containers_discovered,
                        assets_created=state.assets_created,
                        replicas_created=state.replicas_created,
                        elapsed_s=max(0.0, self.clock() - state.started),
                    )
            except Exception as error:
                self._handle_error(state, "source_file", str(path), error)

        # Top-level containers are always inventoried. ``recurse_containers``
        # controls only whether container members can schedule further views.
        self._process_queue(queue, state)
        return self._finish(state, discovery_only=False)

    def _discover(self, root: Path, state: _RunState) -> _Discovery:
        """
        Collect nonsymlink regular files using sorted directory snapshots and a LIFO directory
        stack.

        Push child directories in reverse order for ascending traversal; globally sort a fully
        completed result. Early count/issue returns retain traversal order. Check cancellation/time
        once per directory, not every entry, and materialize that directory before source-count
        checks. Limit truncation requires seeing an extra regular file. Type checks and later opens
        are separate observations.

        Handled discovery OSErrors accumulate locally, subject to the issue cap. Ordinary callback
        failures are not uniformly caught; for example, an OSError from a symlink event is
        classified by the entry guard.

        Example:
            >>> discovery = coordinator._discover(root, state)  # doctest: +SKIP


        :param root: Resolved source directory from which to begin traversal.
        :param state: Active run state used for cancellation/time checks and halt reporting.
        :return: Mutable source list and discovery counters/issues, possibly truncated.
        """
        result = _Discovery()
        pending = [root]
        while pending:
            if self._should_halt(state):
                result.truncated = True
                break
            directory = pending.pop()
            try:
                with os.scandir(directory) as iterator:
                    entries = sorted(iterator, key=lambda item: os.fsencode(item.name))
            except OSError as error:
                self._log_exception(
                    "discovery_directory_error", error, path=str(directory)
                )
                result.issues.append(_make_issue("discovery", str(directory), error))
                if not self.continue_on_error:
                    raise
                if len(result.issues) >= self.budget.max_issues:
                    result.truncated = True
                    return result
                continue
            child_directories: list[Path] = []
            for entry in entries:
                path = Path(entry.path)
                try:
                    if entry.is_symlink():
                        result.skipped_symlinks += 1
                        self._progress("symlink_skipped", path=str(path))
                        continue
                    if entry.is_dir(follow_symlinks=False):
                        if self.recursive_filesystem:
                            child_directories.append(path)
                        continue
                    if not entry.is_file(follow_symlinks=False):
                        continue
                except OSError as error:
                    self._log_exception(
                        "discovery_entry_error", error, path=str(path)
                    )
                    result.issues.append(_make_issue("discovery", str(path), error))
                    if not self.continue_on_error:
                        raise
                    if len(result.issues) >= self.budget.max_issues:
                        result.truncated = True
                        return result
                    continue
                if len(result.files) >= self.budget.max_source_files:
                    result.truncated = True
                    result.issues.append(
                        MixedIngestIssue(
                            "source_file_limit",
                            str(path),
                            f"source discovery stopped at configured limit "
                            f"{self.budget.max_source_files}",  # pyright: ignore[reportImplicitStringConcatenation]
                            "SourceFileLimitReached",
                        )
                    )
                    return result
                result.files.append(path)
            pending.extend(reversed(child_directories))
        result.files.sort(key=lambda path: os.fsencode(str(path)))
        return result

    def _classify_discovery(self, files: Iterable[Path], state: _RunState) -> None:
        """
        Classify selected source paths and append depth-one report declarations without adoption.

        Poll halt before each path. Handled identification failures become issues;
        source_file_classified callback errors occur outside that identification catch. The
        top-level count includes the extra recognized candidate that triggers the container limit,
        while admitted counts/reports do not.

        Example:
            >>> coordinator._classify_discovery(paths, state)  # doctest: +SKIP


        :param files: Source paths in discovery order.
        :param state: Mutable counters, format totals, issue list, and report collection.
        :return: None after classification finishes or a halt/container-count limit stops it.
        """
        for path in files:
            if self._should_halt(state):
                break
            try:
                handler = self._identify_path(path)
            except Exception as error:
                self._handle_error(state, "identify", str(path), error)
                continue
            self._progress(
                "source_file_classified",
                path=str(path),
                format=(None if handler is None else handler.format_name),
                terminal=handler is None,
            )
            if handler is None:
                state.loose_files += 1
                continue
            state.top_level_containers += 1
            if state.containers_discovered >= self.budget.max_containers:
                state.truncated = True
                self._record_issue(
                    state,
                    MixedIngestIssue(
                        "container_limit",
                        str(path),
                        f"container discovery stopped at configured limit "
                        f"{self.budget.max_containers}",  # pyright: ignore[reportImplicitStringConcatenation]
                        "ContainerLimitReached",
                    ),
                )
                break
            state.containers_discovered += 1
            state.formats[handler.format_name] += 1
            state.containers.append(
                ContainerIngestReport(
                    path=str(path), format_name=handler.format_name, depth=1
                )
            )

    def _process_queue(
        self, queue: deque[_ContainerCandidate], state: _RunState
    ) -> None:
        """
        Expand queued containers once per raw backend-kind/SHA-256 identity and reject ancestor byte
        cycles.

        Resolve Asset/digest outside container error isolation, so those errors escape even with
        continuation enabled. Mark an identity expanded before attempting it; a later duplicate is
        skipped even if the first attempt failed. Duplicate and cycle reports preserve adopted
        identities without rolling back source/member writes.

        Example:
            >>> coordinator._process_queue(queue, state)  # doctest: +SKIP


        :param queue: Mutable FIFO of adopted container candidates, extended by nested discoveries.
        :param state: Run counters/reports and cooperative halt state.
        :return: None after queue exhaustion or a halt; uncaught lookup/digest/callback errors propagate.
        """
        expanded: dict[tuple[str, str], str] = {}
        while queue and not self._should_halt(state):
            candidate = queue.popleft()
            asset = self.manager.get_digital_asset_record(candidate.digital_asset_id)
            digest = _sha256_value(asset)
            identity = (candidate.handler.backend_kind, digest)
            if digest in candidate.ancestry:
                issue = MixedIngestIssue(
                    "container_cycle",
                    candidate.display_path,
                    "container bytes repeat an ancestor and were not expanded again",
                    "ContainerCycleDetected",
                    container_chain=candidate.chain,
                )
                state.containers_deduplicated += 1
                self._record_issue(state, issue)
                state.containers.append(
                    ContainerIngestReport(
                        path=candidate.display_path,
                        format_name=candidate.handler.format_name,
                        depth=candidate.depth,
                        digital_asset_id=int(candidate.digital_asset_id),
                        source_replica_id=int(candidate.source_replica_id),
                        duplicate_of=candidate.chain[-2] if len(candidate.chain) > 1 else None,
                        truncated=True,
                        issues=(issue,),
                    )
                )
                continue
            if identity in expanded:
                state.containers_deduplicated += 1
                state.containers.append(
                    ContainerIngestReport(
                        path=candidate.display_path,
                        format_name=candidate.handler.format_name,
                        depth=candidate.depth,
                        digital_asset_id=int(candidate.digital_asset_id),
                        source_replica_id=int(candidate.source_replica_id),
                        duplicate_of=expanded[identity],
                    )
                )
                self._progress(
                    "container_deduplicated",
                    path=candidate.display_path,
                    format=candidate.handler.format_name,
                    depth=candidate.depth,
                    digital_asset_id=int(candidate.digital_asset_id),
                    sha256=digest,
                    duplicate_of=expanded[identity],
                )
                continue
            expanded[identity] = candidate.display_path
            report = self._process_container(candidate, digest, queue, state)
            state.containers.append(report)

    def _process_container(
        self,
        candidate: _ContainerCandidate,
        digest: str,
        queue: deque[_ContainerCandidate],
        state: _RunState,
    ) -> ContainerIngestReport:
        """
        Open one backed container, account for member work, and schedule permitted nested views.

        Nested candidates require a cache Store. If no nondeleted CACHE Replica has matching stat
        size, reserve the container size against single/total materialization bounds before Store
        creation. Backed-Store resolution performs any actual copy. After handled errors, release
        the accounting reservation only when the size-based cache predicate is still false; this
        does not delete cache bytes.

        For each entry, poll halt and count ceilings, obtain size, then validate path/size and
        logical byte budgets before charging counters and adopting. Those charges survive later
        adoption, metadata, nested-identification, or callback failures. Reaching a depth limit
        still adopts the nested bytes, records an issue, and continues siblings. A per-container
        byte/path limit stops this branch; run-wide member/byte ceilings halt the run.

        Handled member/container Exceptions are logged and recorded, with policy-controlled
        re-raising. container_started and container_complete callbacks, plus the final cache
        recheck, sit outside that main catch. Manager writes are incremental and never rolled back
        here.

        Example:
            >>> report = coordinator._process_container(candidate, digest, queue, state)  # doctest: +SKIP


        :param candidate: Adopted container identity, size, selected handler, and ancestry.
        :param digest: Claimed SHA-256 identity used for operation IDs and nested ancestry.
        :param queue: FIFO receiving admitted nested-container candidates.
        :param state: Run-wide counters, limits, issue list, and halt state.
        :return: Container report after normal completion/failure handling and its final callback.
        """
        issues: list[MixedIngestIssue] = []
        materialized = 0
        cache_ref: UUID | None = None
        configuration: manager_api.StoreConfiguration | None = None
        created = False
        members_discovered = members_adopted = 0
        assets_created = replicas_created = nested_discovered = expanded_bytes = 0
        truncated = False
        self._progress(
            "container_started",
            path=candidate.display_path,
            format=candidate.handler.format_name,
            depth=candidate.depth,
            digital_asset_id=int(candidate.digital_asset_id),
            source_replica_id=int(candidate.source_replica_id),
            size_bytes=candidate.size_bytes,
            sha256=digest,
            top_level=candidate.top_level,
            container_chain=candidate.chain,
        )
        try:
            if not candidate.top_level:
                cache_ref = self._ensure_materialization_store()
                if cache_ref is None:
                    raise storage_errors.StoragePreconditionFailed(
                        "nested container expansion requires a local writable "
                        + "materialization Store; supply materialization_store_ref "
                        + "or materialization_root"
                    )
                if not self._has_cache_replica(candidate.digital_asset_id, cache_ref):
                    if candidate.size_bytes > self.budget.max_temporary_bytes:
                        raise storage_errors.StoragePreconditionFailed(
                            f"nested container is {candidate.size_bytes} bytes, above "
                            + "the single-materialization limit "
                            + str(self.budget.max_temporary_bytes)
                        )
                    if (
                        state.materialized_bytes + candidate.size_bytes
                        > self.budget.max_materialized_bytes
                    ):
                        raise storage_errors.StoragePreconditionFailed(
                            "run-wide materialization byte limit would be exceeded"
                        )
                    state.materialized_bytes += candidate.size_bytes
                    materialized = candidate.size_bytes
                    self._progress(
                        "materialization_reserved",
                        path=candidate.display_path,
                        digital_asset_id=int(candidate.digital_asset_id),
                        cache_store_ref=str(cache_ref),
                        size_bytes=candidate.size_bytes,
                        run_materialized_bytes=state.materialized_bytes,
                    )
            configuration, created = self._ensure_container_store(
                candidate, cache_ref=cache_ref
            )
            self._progress(
                "container_store_ready",
                path=candidate.display_path,
                store_ref=str(configuration.store_uuid),
                store_name=configuration.store_name,
                store_kind=configuration.store_kind,
                created=created,
                cache_store_ref=(None if cache_ref is None else str(cache_ref)),
            )
            store = self.manager.get_store(configuration.store_uuid)
            state.containers_processed += 1
            for entry in store.iter_inventory_entries():
                if self._should_halt(state):
                    truncated = True
                    break
                if members_discovered >= self.budget.max_members_per_container:
                    issue = MixedIngestIssue(
                        "container_member_limit",
                        candidate.display_path,
                        f"container member limit reached: "
                        f"{self.budget.max_members_per_container}",  # pyright: ignore[reportImplicitStringConcatenation]
                        "ContainerMemberLimitReached",
                        container_chain=candidate.chain,
                    )
                    issues.append(issue)
                    self._record_issue(state, issue)
                    truncated = True
                    break
                if state.members_discovered >= self.budget.max_members:
                    issue = MixedIngestIssue(
                        "member_limit",
                        candidate.display_path,
                        f"run-wide member limit reached: {self.budget.max_members}",
                        "MemberLimitReached",
                        container_chain=candidate.chain,
                        fatal=True,
                    )
                    issues.append(issue)
                    self._halt(state, issue.message)
                    self._record_issue(state, issue)
                    truncated = True
                    break
                try:
                    size = entry.size
                    if size is None:
                        size = store.stat(entry.location).size
                    self._validate_member(entry.location.key, size)
                    prospective = expanded_bytes + size
                    if prospective > self.budget.max_container_expanded_bytes:
                        raise _ContainerLimit(
                            "container expanded-byte limit would be exceeded"
                        )
                    if prospective > (
                        self.budget.max_container_expansion_ratio
                        * max(candidate.size_bytes, 1)
                    ):
                        raise _ContainerLimit(
                            "container logical expansion-ratio limit would be exceeded"
                        )
                    if state.expanded_bytes + size > self.budget.max_total_expanded_bytes:
                        issue = MixedIngestIssue(
                            "expanded_byte_limit",
                            candidate.display_path,
                            "run-wide expanded-byte limit would be exceeded",
                            "ExpandedByteLimitReached",
                            container_chain=candidate.chain,
                            member_path=entry.location.key,
                            fatal=True,
                        )
                        issues.append(issue)
                        self._halt(state, issue.message)
                        self._record_issue(state, issue)
                        truncated = True
                        break
                    members_discovered += 1
                    state.members_discovered += 1
                    expanded_bytes += size
                    state.expanded_bytes += size
                    existed = self._has_replica_at(entry.location)
                    result = self.manager.adopt_location(
                        entry.location,
                        operation_id=_operation_id(
                            "member",
                            digest,
                            str(configuration.store_uuid),
                            entry.location.key,
                            entry.version or str(size),
                        ),
                        metadata=self.member_metadata_factory(
                            ContainerMemberContext(
                                container_path=candidate.display_path,
                                format_name=candidate.handler.format_name,
                                depth=candidate.depth,
                                parent_digital_asset_id=candidate.digital_asset_id,
                                container_chain=candidate.chain,
                            ),
                            entry,
                        ),
                        replica_mode=manager_api.ReplicaMode.ARCHIVE,
                        verify=self.verify_members,
                    )
                    members_adopted += 1
                    state.members_adopted += 1
                    asset_created_now = result.asset_created and not existed
                    replica_created_now = result.replica_created and not existed
                    assets_created += int(asset_created_now)
                    replicas_created += int(replica_created_now)
                    state.assets_created += int(asset_created_now)
                    state.replicas_created += int(replica_created_now)
                    self._remember_replica(result.replica_record.location)
                    nested_format: str | None = None
                    if self.recurse_containers and candidate.depth < self.budget.max_container_depth:
                        nested_handler = self._identify_store_entry(store, entry)
                        if nested_handler is not None:
                            nested_format = nested_handler.format_name
                            nested_discovered += 1
                            self._schedule_container(
                                queue,
                                state,
                                _ContainerCandidate(
                                    display_path=(
                                        candidate.display_path + "!/" + entry.location.key
                                    ),
                                    filename=(
                                        entry.hints.suggested_filename
                                        or PurePosixPath(entry.location.key).name
                                    ),
                                    handler=nested_handler,
                                    digital_asset_id=result.asset_record.digital_asset_id,
                                    source_replica_id=result.replica_record.replica_id,
                                    size_bytes=result.asset_record.size_bytes,
                                    depth=candidate.depth + 1,
                                    ancestry=(*candidate.ancestry, digest),
                                    chain=(
                                        *candidate.chain,
                                        candidate.display_path + "!/" + entry.location.key,
                                    ),
                                    top_level=False,
                                ),
                            )
                    elif self.recurse_containers:
                        nested_handler = self._identify_store_entry(store, entry)
                        if nested_handler is not None:
                            nested_format = nested_handler.format_name
                            nested_discovered += 1
                            issue = MixedIngestIssue(
                                "container_depth_limit",
                                candidate.display_path,
                                "nested container was catalogued but not expanded at "
                                + f"depth limit {self.budget.max_container_depth}",
                                "ContainerDepthLimitReached",
                                container_chain=candidate.chain,
                                member_path=entry.location.key,
                            )
                            issues.append(issue)
                            self._record_issue(state, issue)
                            truncated = True
                    self._progress(
                        "member_adopted",
                        container_path=candidate.display_path,
                        container_store_ref=str(configuration.store_uuid),
                        container_format=candidate.handler.format_name,
                        container_depth=candidate.depth,
                        member_path=entry.location.key,
                        size_bytes=size,
                        version=entry.version,
                        inventory_digest=(
                            None
                            if entry.digest is None
                            else f"{entry.digest.algorithm}:{entry.digest.value}"
                        ),
                        suggested_filename=entry.hints.suggested_filename,
                        media_type=entry.hints.media_type,
                        digital_asset_id=int(result.asset_record.digital_asset_id),
                        replica_id=int(result.replica_record.replica_id),
                        asset_created=asset_created_now,
                        replica_created=replica_created_now,
                        deduplicated=result.deduplicated,
                        verified=result.verified,
                        nested_format=nested_format,
                        container_members_adopted=members_adopted,
                        run_members_adopted=state.members_adopted,
                        run_expanded_bytes=state.expanded_bytes,
                    )
                    if state.members_adopted % self.log_checkpoint_every == 0:
                        self._progress(
                            "member_checkpoint",
                            container_path=candidate.display_path,
                            container_members_adopted=members_adopted,
                            run_members_discovered=state.members_discovered,
                            run_members_adopted=state.members_adopted,
                            run_expanded_bytes=state.expanded_bytes,
                            run_assets_created=state.assets_created,
                            run_replicas_created=state.replicas_created,
                            queued_containers=len(queue),
                            elapsed_s=max(0.0, self.clock() - state.started),
                        )
                except _ContainerLimit as error:
                    issue = _make_issue(
                        "container_byte_limit",
                        candidate.display_path,
                        error,
                        chain=candidate.chain,
                        member_path=entry.location.key,
                    )
                    issues.append(issue)
                    self._record_issue(state, issue)
                    truncated = True
                    break
                except Exception as error:
                    self._log_exception(
                        "member_error",
                        error,
                        container_path=candidate.display_path,
                        container_format=candidate.handler.format_name,
                        container_depth=candidate.depth,
                        container_chain=candidate.chain,
                        member_path=entry.location.key,
                        store_ref=str(configuration.store_uuid),
                    )
                    issue = _make_issue(
                        "member",
                        candidate.display_path,
                        error,
                        chain=candidate.chain,
                        member_path=entry.location.key,
                    )
                    issues.append(issue)
                    self._record_issue(state, issue)
                    if not self.continue_on_error:
                        raise
        except Exception as error:
            self._log_exception(
                "container_error",
                error,
                path=candidate.display_path,
                format=candidate.handler.format_name,
                depth=candidate.depth,
                digital_asset_id=int(candidate.digital_asset_id),
                source_replica_id=int(candidate.source_replica_id),
                size_bytes=candidate.size_bytes,
                container_chain=candidate.chain,
                cache_store_ref=(None if cache_ref is None else str(cache_ref)),
            )
            issue = _make_issue(
                "container",
                candidate.display_path,
                error,
                chain=candidate.chain,
            )
            issues.append(issue)
            self._record_issue(state, issue)
            if not self.continue_on_error:
                raise

        if (
            materialized
            and cache_ref is not None
            and not self._has_cache_replica(candidate.digital_asset_id, cache_ref)
        ):
            # The preflight reservation is conservative; report and retain it
            # only when checked publication actually left a durable CACHE
            # Replica behind (including a valid cache of a corrupt container).
            state.materialized_bytes -= materialized
            materialized = 0

        report = ContainerIngestReport(
            path=candidate.display_path,
            format_name=candidate.handler.format_name,
            depth=candidate.depth,
            digital_asset_id=int(candidate.digital_asset_id),
            source_replica_id=int(candidate.source_replica_id),
            store_ref=None if configuration is None else configuration.store_uuid,
            store_created=created,
            members_discovered=members_discovered,
            members_adopted=members_adopted,
            member_assets_created=assets_created,
            member_replicas_created=replicas_created,
            nested_containers_discovered=nested_discovered,
            expanded_bytes=expanded_bytes,
            materialized_bytes=materialized,
            truncated=truncated,
            issues=tuple(issues),
        )
        self._progress(
            "container_complete",
            path=candidate.display_path,
            format=candidate.handler.format_name,
            depth=candidate.depth,
            digital_asset_id=int(candidate.digital_asset_id),
            store_ref=(None if configuration is None else str(configuration.store_uuid)),
            store_created=created,
            ok=report.ok,
            members_discovered=members_discovered,
            members_adopted=members_adopted,
            member_assets_created=assets_created,
            member_replicas_created=replicas_created,
            nested_containers_discovered=nested_discovered,
            expanded_bytes=expanded_bytes,
            materialized_bytes=materialized,
            truncated=truncated,
            issue_count=len(issues),
        )
        return report

    def _schedule_container(
        self,
        queue: deque[_ContainerCandidate],
        state: _RunState,
        candidate: _ContainerCandidate,
    ) -> None:
        """
        Admit a candidate to the FIFO and counters, or report the container-count ceiling once.

        A refusal sets truncated but does not itself halt source adoption. The first refused
        candidate emits one issue; reaching the aggregate issue cap can then halt the run. Accepted
        state is updated before the discovery callback, so callback failure does not undo
        scheduling.

        Example:
            >>> coordinator._schedule_container(queue, state, candidate)  # doctest: +SKIP


        :param queue: Mutable container FIFO to append to when within budget.
        :param state: Run counters and single-limit-report marker.
        :param candidate: Container declaration to admit or refuse.
        :return: None after scheduling or limit reporting.
        """
        if state.containers_discovered >= self.budget.max_containers:
            state.truncated = True
            if not state.container_limit_reported:
                state.container_limit_reported = True
                self._record_issue(
                    state,
                    MixedIngestIssue(
                        "container_limit",
                        candidate.display_path,
                        f"run-wide container limit reached: {self.budget.max_containers}",
                        "ContainerLimitReached",
                        container_chain=candidate.chain,
                    ),
                )
            return
        state.containers_discovered += 1
        state.formats[candidate.handler.format_name] += 1
        queue.append(candidate)
        self._progress(
            "container_discovered",
            path=candidate.display_path,
            format=candidate.handler.format_name,
            depth=candidate.depth,
            digital_asset_id=int(candidate.digital_asset_id),
            source_replica_id=int(candidate.source_replica_id),
            size_bytes=candidate.size_bytes,
            top_level=candidate.top_level,
            queue_size=len(queue),
        )

    def _identify_path(self, path: Path) -> ContainerHandler | None:
        """
        Identify a local file by name first, then by a bounded leading probe if needed.

        Named/terminal cases may never open the file. The nested reader owns each file context; no
        race-free relationship with prior discovery is established.

        Example:
            >>> handler = coordinator._identify_path(path)  # doctest: +SKIP


        :param path: Local source path providing the filename and optional probe bytes.
        :return: Selected handler or None; file/probe failures propagate.
        """
        def read_range(offset: int, length: int) -> bytes:
            """
            Open the enclosing path, seek to the requested offset, read at most length bytes, and
            close.

            No extra range validation or file-identity check occurs.

            Example:
                >>> probe = read_range(0, 32774)  # doctest: +SKIP


            :param offset: Byte offset passed to seek.
            :param length: Byte count passed to read.
            :return: Read bytes, possibly shorter at EOF; I/O failures propagate.
            """
            with path.open("rb") as source:
                _ = source.seek(offset)
                return source.read(length)

        return self._identify(path.name, read_range)

    def _identify_store_entry(
        self, store: store_api.StoreAPI, entry: storage_models.StoreInventoryEntry
    ) -> ContainerHandler | None:
        """
        Identify a member using its suggested name or key basename and optional Store range reads.

        Name heuristics can select a handler without opening the member. Probe failures propagate to
        the enclosing member boundary.

        Example:
            >>> handler = coordinator._identify_store_entry(store, entry)  # doctest: +SKIP


        :param store: Container Store supplying bounded member streams.
        :param entry: Inventory entry supplying Location and advisory filename.
        :return: Selected handler or None without validating the complete container bytes.
        """
        name = entry.hints.suggested_filename or PurePosixPath(entry.location.key).name

        def read_range(offset: int, length: int) -> bytes:
            """
            Open a bounded stream for the enclosing member, read up to length bytes, and close it.

            Example:
                >>> probe = read_range(0, 512)  # doctest: +SKIP


            :param offset: Starting byte offset forwarded to Store.open_read.
            :param length: Requested stream range and maximum read length.
            :return: Available range bytes; Store/read/cleanup errors propagate.
            """
            with store.open_read(entry.location, offset=offset, length=length) as source:
                return source.read(length)

        return self._identify(name, read_range)

    def _identify(self, name: str, read_range: RangeReader) -> ContainerHandler | None:
        """
        Apply ebook policy, suffix priority, then the first matching magic signature.

        With expansion disabled, terminal ebook suffixes return None before byte inspection. With
        expansion enabled, EPUB/CBZ/CBR select their named ZIP/RAR handler directly or None if
        absent. Other suffix matches are ranked by each handler's longest configured suffix, not
        solely the matched suffix; ties retain handler order.

        Only unnamed cases call read_range once for the largest signature endpoint. Empty global
        signature sets raise ValueError there; custom signature offsets have no additional
        probe-size cap here. Recognition is not full archive validation.

        Example:
            >>> handler = coordinator._identify("pack.zip", reader)  # doctest: +SKIP


        :param name: Filename text used for terminal and suffix rules.
        :param read_range: Callback accepting byte offset/length for the optional leading probe.
        :return: Selected declaration or None; reader/invalid-registry errors propagate.
        """
        suffix = PurePosixPath(name).suffix.lower()
        if suffix in _TERMINAL_EBOOK_SUFFIXES and not self.expand_ebook_containers:
            return None
        if self.expand_ebook_containers and suffix in _EBOOK_CONTAINER_FORMATS:
            format_name = _EBOOK_CONTAINER_FORMATS[suffix]
            return next(
                (handler for handler in self.handlers if handler.format_name == format_name),
                None,
            )
        named = sorted(
            (handler for handler in self.handlers if handler.matches_name(name)),
            key=lambda handler: max(map(len, handler.suffixes)),
            reverse=True,
        )
        if named:
            return named[0]
        probe_size = max(
            offset + len(signature)
            for handler in self.handlers
            for offset, signature in handler.magic_signatures
        )
        probe = read_range(0, probe_size)
        return next(
            (handler for handler in self.handlers if handler.matches_probe(probe)),
            None,
        )

    def _ensure_source_store(
        self, root: Path
    ) -> tuple[manager_api.StoreConfiguration, bool]:
        """
        Reuse an available compatible UNMANAGED-capable source Store or create a read-only unmanaged
        one.

        Existing kind and Replica-mode policy must qualify. New construction/startup or availability
        failures are not followed by configuration removal in this coordinator.

        Example:
            >>> configuration, created = coordinator._ensure_source_store(root)  # doctest: +SKIP


        :param root: Resolved source directory whose file URI identifies the Store.
        :return: Available configuration and whether this call created it.
        """
        root_uri = root.as_uri()
        existing = self._configuration_for_root(root_uri)
        if existing is not None:
            canonical = DEFAULT_BACKEND_REGISTRY.canonical_kind(existing.store_kind)
            if canonical not in {
                "filesystem",
                "on_disk_existing_managed_drive",
                "on_disk_existing_unmanaged_drive",
            }:
                raise storage_errors.StoragePreconditionFailed(
                    f"Store root {root_uri!r} is configured as incompatible "
                    + f"backend {existing.store_kind!r}."
                )
            if manager_api.ReplicaMode.UNMANAGED not in existing.supported_replica_modes:
                raise storage_errors.StoragePreconditionFailed(
                    f"Store root {root_uri!r} does not permit UNMANAGED "
                    + "Replica adoption."
                )
            self._require_available(existing)
            return existing, False
        configuration = manager_api.StoreConfiguration.for_backend(
            _store_name("ingest-source", root),
            "on_disk_existing_unmanaged_drive",
            root,
            protocol="file",
            tags=("ingest-source", "unmanaged", "mixed-ingest"),
            modes=(manager_api.ReplicaMode.UNMANAGED,),
            operational_role="live",
            read_only=True,
            folders=True,
        )
        _ = self.manager.create_store(configuration, startup=True)
        self._require_available(configuration)
        return configuration, True

    def _ensure_materialization_store(self) -> UUID | None:
        """
        Resolve, validate, or lazily create the local writable CACHE Store for nested bytes.

        An explicit Store reference is preferred. Otherwise create the configured directory before
        root lookup and cache its resulting Store identity on the coordinator. This selection
        survives subsequent calls. Validation/creation/event failures do not remove earlier
        filesystem or manager effects. No selector returns None.

        Example:
            >>> cache_ref = coordinator._ensure_materialization_store()  # doctest: +SKIP


        :return: Available cache Store UUID, or None when neither selector is configured.
        """
        if self.materialization_store_ref is not None:
            configuration = self.manager.get_store_configuration(
                self.materialization_store_ref
            )
            self._validate_cache_configuration(configuration)
            self._require_available(configuration)
            self._progress(
                "materialization_store_ready",
                store_ref=str(configuration.store_uuid),
                store_name=configuration.store_name,
                store_kind=configuration.store_kind,
                created=False,
                configured_by="store_ref",
            )
            return configuration.store_uuid
        root = self.materialization_root
        if root is None:
            return None
        root.mkdir(parents=True, exist_ok=True)
        existing = self._configuration_for_root(root.as_uri())
        if existing is not None:
            self._validate_cache_configuration(existing)
            self._require_available(existing)
            self.materialization_store_ref = existing.store_uuid
            self._progress(
                "materialization_store_ready",
                store_ref=str(existing.store_uuid),
                store_name=existing.store_name,
                store_kind=existing.store_kind,
                created=False,
                configured_by="root",
            )
            return existing.store_uuid
        configuration = manager_api.StoreConfiguration.for_backend(
            _store_name("ingest-cache", root),
            "filesystem",
            root,
            protocol="file",
            tags=("cache", "ingest-materialization", "mixed-ingest"),
            modes=(manager_api.ReplicaMode.CACHE,),
            operational_role="cache",
            read_only=False,
            folders=True,
        )
        _ = self.manager.create_store(configuration, startup=True)
        self._require_available(configuration)
        self.materialization_store_ref = configuration.store_uuid
        self._progress(
            "materialization_store_ready",
            store_ref=str(configuration.store_uuid),
            store_name=configuration.store_name,
            store_kind=configuration.store_kind,
            created=True,
            configured_by="root",
        )
        return configuration.store_uuid

    def _ensure_container_store(
        self,
        candidate: _ContainerCandidate,
        *,
        cache_ref: UUID | None,
    ) -> tuple[manager_api.StoreConfiguration, bool]:
        """
        Reuse a unique equivalent backed Store or ask the manager to create one for the container
        Asset.

        Equivalence uses Asset ID, canonical backend kind, and the complete options mapping, not
        option-pair order or preferred Replica identity. An existing missing cache reference can be
        filled; a different already-set reference is retained. Availability is checked after
        updates/creation, with no rollback here.

        Example:
            >>> configuration, created = coordinator._ensure_container_store(candidate, cache_ref=cache_ref)  # doctest: +SKIP


        :param candidate: Container Asset/Replica identities and selected handler.
        :param cache_ref: Optional materialization Store identity passed to backed resolution.
        :return: Available backed configuration and creation flag; ambiguity or manager failures propagate.
        """
        options = self._backend_options(candidate.handler)
        option_pairs = tuple(options.items())
        canonical_kind = DEFAULT_BACKEND_REGISTRY.canonical_kind(
            candidate.handler.backend_kind
        )
        matches = tuple(
            configuration
            for configuration in self.manager.iter_store_configurations()
            if configuration.backing is not None
            and configuration.backing.digital_asset_id == candidate.digital_asset_id
            and DEFAULT_BACKEND_REGISTRY.canonical_kind(configuration.store_kind)
            == canonical_kind
            # Database serialization is allowed to reorder option pairs; the
            # mapping, not tuple order, is the durable backend identity here.
            and dict(configuration.backend_options) == options
        )
        if len(matches) > 1:
            raise storage_errors.StoragePreconditionFailed(
                "multiple equivalent backed Stores expose one container Asset"
            )
        if matches:
            configuration = matches[0]
            if (
                cache_ref is not None
                and configuration.backing is not None
                and configuration.backing.materialization_store_ref is None
            ):
                replacement = dataclasses.replace(
                    configuration,
                    backing=dataclasses.replace(
                        configuration.backing,
                        materialization_store_ref=cache_ref,
                    ),
                )
                configuration = self.manager.update_store(
                    configuration.store_uuid, replacement
                )
            self._require_available(configuration)
            return configuration, False
        configuration = self.manager.add_backed_store(
            _store_name(candidate.handler.format_name, Path(candidate.filename)),
            candidate.handler.backend_kind,
            candidate.digital_asset_id,
            source_replica_id=candidate.source_replica_id,
            materialization_store_ref=cache_ref,
            protocol=candidate.handler.protocol,
            tags=("archive", candidate.handler.format_name, "mixed-ingest"),
            modes=(manager_api.ReplicaMode.ARCHIVE,),
            operational_role="archive",
            folders=True,
            options=option_pairs,
            start=True,
        )
        self._require_available(configuration)
        return configuration, True

    def _backend_options(self, handler: ContainerHandler) -> dict[str, object]:
        """
        Translate workflow ceilings into format-specific immutable backend options.

        ISO receives UDF member/logical-ratio limits. RAR, 7z, and SquashFS cap member size by
        temporary capacity and include path-byte limits; ZIP/TAR retain streaming member limits.
        Only RAR/SquashFS receive the stable timeout here. Unknown custom format names receive the
        common non-ISO mapping.

        Example:
            >>> options = coordinator._backend_options(handler)  # doctest: +SKIP


        :param handler: Declaration whose exact format_name selects option branches.
        :return: Fresh backend option mapping used for durable Store identity and construction.
        """
        budget = self.budget
        common: dict[str, object] = {
            "max_inventory_entries": budget.max_members_per_container,
            "max_depth": budget.max_path_depth,
            "max_total_uncompressed_bytes": budget.max_container_expanded_bytes,
        }
        if handler.format_name == "iso":
            return {
                **common,
                "max_udf_member_bytes": min(
                    budget.max_member_bytes, budget.max_temporary_bytes
                ),
                "max_logical_expansion_ratio": budget.max_container_expansion_ratio,
                "max_path_bytes": budget.max_path_bytes,
            }
        member_limit = budget.max_member_bytes
        if handler.format_name in {"rar", "7z", "squashfs"}:
            # These readers may stage a whole member before exposing it. ZIP
            # and TAR stream directly and should not inherit a spool-only cap.
            member_limit = min(member_limit, budget.max_temporary_bytes)
        common.update(
            {
                "max_member_bytes": member_limit,
                "max_compression_ratio": budget.max_container_expansion_ratio,
            }
        )
        if handler.format_name in {"rar", "7z", "squashfs"}:
            common["max_path_bytes"] = budget.max_path_bytes
        if handler.format_name == "rar":
            common["extract_timeout_s"] = self._remaining_backend_timeout()
            if self.rar_extractor_exe is not None:
                common["extractor_exe"] = self.rar_extractor_exe
        if handler.format_name == "squashfs":
            common["timeout_s"] = self._remaining_backend_timeout()
            common["unsquashfs_exe"] = self.unsquashfs_exe
        return common

    def _validate_member(self, key: str, size: int) -> None:
        """
        Check nonnegative size, member-byte ceiling, encoded key length, and POSIX component depth.

        UTF-8 surrogatepass preserves undecodable-name distinctions. This helper does not reject
        traversal, absolute paths, or symlinks as a general path-safety policy; backend
        inventory/Location rules own those constraints.

        Example:
            >>> coordinator._validate_member("books/a.epub", 1024)  # doctest: +SKIP


        :param key: Member Location key whose encoded length/components are counted.
        :param size: Declared or stat-derived size in bytes.
        :return: None on accepted bounds; negative sizes raise StorageIntegrityError and ceilings raise _ContainerLimit.
        """
        if size < 0:
            raise storage_errors.StorageIntegrityError("container member reports a negative size")
        if size > self.budget.max_member_bytes:
            raise _ContainerLimit(
                f"member is {size} bytes, above limit {self.budget.max_member_bytes}"
            )
        if len(key.encode("utf-8", "surrogatepass")) > self.budget.max_path_bytes:
            raise _ContainerLimit("member path exceeds encoded-byte limit")
        depth = len(PurePosixPath(key).parts)
        if depth > self.budget.max_path_depth:
            raise _ContainerLimit("member path exceeds component-depth limit")

    def _validate_cache_configuration(
        self, configuration: manager_api.StoreConfiguration
    ) -> None:
        """
        Require a writable declaration supporting CACHE mode and the canonical filesystem backend.

        Inspect configuration only, without probing permissions, available space, contents, or
        physical health.

        Example:
            >>> coordinator._validate_cache_configuration(configuration)  # doctest: +SKIP


        :param configuration: Proposed materialization Store declaration.
        :return: None when policy qualifies; otherwise raise StoragePreconditionFailed.
        """
        if configuration.read_only:
            raise storage_errors.StoragePreconditionFailed(
                "materialization Store must be writable"
            )
        if manager_api.ReplicaMode.CACHE not in configuration.supported_replica_modes:
            raise storage_errors.StoragePreconditionFailed(
                "materialization Store must support CACHE Replicas"
            )
        if DEFAULT_BACKEND_REGISTRY.canonical_kind(configuration.store_kind) != "filesystem":
            raise storage_errors.StoragePreconditionFailed(
                "materialization Store must currently be a local filesystem Store"
            )

    def _validate_materialization_root(self, source_root: Path) -> None:
        """
        Reject a configured cache path equal to or below the resolved source directory.

        Check only materialization_root, not an explicit materialization_store_ref's location. The
        comparison is lexical between already-resolved paths and does not protect against later
        symlink/filesystem changes.

        Example:
            >>> coordinator._validate_materialization_root(root)  # doctest: +SKIP


        :param source_root: Resolved source directory that must not contain the configured cache root.
        :return: None for absent/outside cache roots; otherwise raise ValueError.
        """
        root = self.materialization_root
        if root is None:
            return
        try:
            _ = root.relative_to(source_root)
        except ValueError:
            return
        raise ValueError(
            "materialization_root must be outside source_root so cache files "
            + "cannot be rediscovered as new input"
        )

    def _configuration_for_root(
        self, root_uri: str
    ) -> manager_api.StoreConfiguration | None:
        """
        Select one configured root by canonical local-URI equality, rejecting multiple claims.

        No availability check or Store rebind is performed here.

        Example:
            >>> configuration = coordinator._configuration_for_root(root.as_uri())  # doctest: +SKIP


        :param root_uri: Path/local URI or opaque non-file URI used as the lookup target.
        :return: Unique configuration or None; duplicate roots raise StoragePreconditionFailed.
        """
        target = _canonical_local_uri(root_uri)
        matches = tuple(
            configuration
            for configuration in self.manager.iter_store_configurations()
            if _canonical_local_uri(configuration.store_root_uri) == target
        )
        if len(matches) > 1:
            raise storage_errors.StoragePreconditionFailed(
                f"multiple configured Stores claim local root {root_uri!r}"
            )
        return matches[0] if matches else None

    def _require_available(self, configuration: manager_api.StoreConfiguration) -> None:
        """
        Resolve a Store, rebind its configuration after StoreUnavailable, and require an available
        refreshed status.

        Other lookup errors propagate. Rebinding/probing may have effects before later failure; this
        is neither a write-capability check nor independent byte verification.

        Example:
            >>> coordinator._require_available(configuration)  # doctest: +SKIP


        :param configuration: Store identity and declaration to use for a possible rebind.
        :return: None on available status; otherwise propagate/raise StoreUnavailable.
        """
        try:
            store = self.manager.get_store(configuration.store_uuid)
        except storage_errors.StoreUnavailable:
            _ = self.manager.update_store(
                configuration.store_uuid, configuration
            )
            store = self.manager.get_store(configuration.store_uuid)
        status = store.status(refresh=True)
        if not status.available:
            raise storage_errors.StoreUnavailable(
                status.message or f"Store {configuration.store_name!r} is unavailable."
            )

    def _has_replica_at(self, location: storage_models.Location) -> bool:
        """
        Check a lazily populated per-Store set of nondeleted Replica Locations.

        The cache persists across coordinator runs and does not refresh external changes. Any state
        except DELETED counts; presence adjusts receipt counters without proving availability or
        skipping adoption.

        Example:
            >>> existed = coordinator._has_replica_at(location)  # doctest: +SKIP


        :param location: Exact Location compared against the cached set.
        :return: Whether the cached nondeleted-Location set contains this value.
        """
        locations = self._replica_locations_by_store.get(location.store_ref)
        if locations is None:
            locations = {
                record.location
                for record in self.manager.iter_replica_records(
                    store_ref=location.store_ref
                )
                if record.state is not manager_api.ReplicaState.DELETED
            }
            self._replica_locations_by_store[location.store_ref] = locations
        return location in locations

    def _remember_replica(self, location: storage_models.Location) -> None:
        """
        Add an adopted Location to the per-Store cache without querying remaining Replicas.

        If the Store was not previously loaded, its new cache initially contains only this Location.

        Example:
            >>> coordinator._remember_replica(location)  # doctest: +SKIP


        :param location: Location returned by manager adoption.
        :return: None after creating/updating the cached set.
        """
        self._replica_locations_by_store.setdefault(location.store_ref, set()).add(
            location
        )

    def _has_cache_replica(
        self, digital_asset_id: manager_api.DigitalAssetID, store_ref: UUID
    ) -> bool:
        """
        Find a nondeleted CACHE Replica of the Asset whose stat size matches the Asset size.

        Only stat StorageErrors are skipped. Other lookup/enumeration errors propagate. No digest,
        state-verification, or content comparison is performed, so success is a size-based cache
        observation.

        Example:
            >>> available = coordinator._has_cache_replica(asset_id, cache_ref)  # doctest: +SKIP


        :param digital_asset_id: Asset whose recorded size and CACHE Replicas are examined.
        :param store_ref: Store in which to search for an existing cache copy.
        :return: True on the first matching-size stat result, otherwise False.
        """
        asset = self.manager.get_digital_asset_record(digital_asset_id)
        for record in self.manager.iter_replica_records(store_ref=store_ref):
            if (
                record.digital_asset_id != digital_asset_id
                or record.mode is not manager_api.ReplicaMode.CACHE
                or record.state is manager_api.ReplicaState.DELETED
            ):
                continue
            try:
                info = self.manager.stat(record.location)
            except storage_errors.StorageError:
                continue
            if info.size == asset.size_bytes:
                return True
        return False

    def _remaining_backend_timeout(self) -> float:
        # Container configurations are durable, so use a stable per-call ceiling
        # rather than embedding the momentary run remainder in their identity.
        """
        Return the stable minimum of backend timeout and the total wall-time budget.

        Despite the historical name, this does not subtract elapsed time; stable values keep durable
        backed-Store option identity repeatable.

        Example:
            >>> seconds = coordinator._remaining_backend_timeout()  # doctest: +SKIP


        :return: Per-call timeout ceiling in seconds, independent of current run progress.
        """
        return min(self.backend_timeout_s, self.budget.max_wall_time_s)

    def _should_halt(self, state: _RunState) -> bool:
        """
        Honor an existing halt, then poll cancellation and elapsed time in that order.

        A truthy cancellation result or time strictly greater than the wall budget records a fatal
        issue and marks truncation. Polling is cooperative; callback/clock/logging errors propagate
        and ongoing external calls are not interrupted.

        Example:
            >>> halted = coordinator._should_halt(state)  # doctest: +SKIP


        :param state: Run state holding start time and any prior halt reason.
        :return: Whether subsequent orchestration should stop, potentially after mutating halt/issues.
        """
        if state.halt_reason is not None:
            return True
        if self.cancellation_callback is not None and self.cancellation_callback():
            self._halt(state, "ingest cancelled by callback")
            self._record_issue(
                state,
                MixedIngestIssue(
                    "cancelled",
                    str(state.source_root),
                    state.halt_reason or "ingest cancelled",
                    "IngestCancelled",
                    fatal=True,
                ),
            )
            return True
        if self.clock() - state.started > self.budget.max_wall_time_s:
            self._halt(state, "run-wide wall-time limit reached")
            self._record_issue(
                state,
                MixedIngestIssue(
                    "wall_time_limit",
                    str(state.source_root),
                    state.halt_reason or "wall-time limit reached",
                    "WallTimeLimitReached",
                    fatal=True,
                ),
            )
            return True
        return False

    def _halt(self, state: _RunState, reason: str) -> None:
        """
        Retain the first truthy halt reason, mark truncation, and log the initial transition.

        Subsequent calls keep an existing truthy reason and emit no new halt log. Logging failures
        occur after state mutation.

        Example:
            >>> coordinator._halt(state, "ingest cancelled")  # doctest: +SKIP


        :param state: Mutable run state to stop.
        :param reason: Requested reason used when no truthy reason has been retained.
        :return: None after marking the run halted and attempting any first-transition log.
        """
        first_halt = state.halt_reason is None
        state.halt_reason = state.halt_reason or reason
        state.truncated = True
        if first_halt:
            self._emit_log(
                logging.ERROR,
                "run_halted",
                "Mixed ingest run halted",
                reason=reason,
                files_adopted=state.files_adopted,
                containers_processed=state.containers_processed,
                members_adopted=state.members_adopted,
                expanded_bytes=state.expanded_bytes,
                materialized_bytes=state.materialized_bytes,
                elapsed_s=max(0.0, self.clock() - state.started),
            )

    def _record_issue(self, state: _RunState, issue: MixedIngestIssue) -> None:
        """
        Log an issue, append it within the aggregate cap, and halt when that cap is reached.

        Log every supplied issue even when the list is already full. A fatal flag chooses log
        severity; halt otherwise depends on the cap or a separate caller action. Child report issue
        lists are not trimmed by this helper.

        Example:
            >>> coordinator._record_issue(state, issue)  # doctest: +SKIP


        :param state: Run issue collection and halt state.
        :param issue: Issue value retained by reference when capacity remains.
        :return: None after logging and applicable list/halt updates.
        """
        self._emit_log(
            logging.ERROR if issue.fatal else logging.WARNING,
            "ingest_issue",
            issue.message,
            stage=issue.stage,
            path=issue.path,
            error_type=issue.error_type,
            container_chain=issue.container_chain,
            member_path=issue.member_path,
            fatal=issue.fatal,
            recorded_issue_count=min(
                len(state.issues) + 1, self.budget.max_issues
            ),
        )
        if len(state.issues) < self.budget.max_issues:
            state.issues.append(issue)
        if len(state.issues) >= self.budget.max_issues and state.halt_reason is None:
            self._halt(state, f"issue limit reached: {self.budget.max_issues}")

    def _handle_error(
        self, state: _RunState, stage: str, path: str, error: BaseException
    ) -> None:
        """
        Log an exception with traceback, record its nonfatal issue, then re-raise if continuation is
        disabled.

        No preceding manager or filesystem effect is undone. Logging/issue handling can fail before
        the original error is re-raised.

        Example:
            >>> coordinator._handle_error(state, "source_file", path, error)  # doctest: +SKIP


        :param state: Run state receiving the issue and possible issue-cap halt.
        :param stage: Boundary label used in the issue and suffixed error event.
        :param path: Source/container display path retained in diagnostics.
        :param error: Exception to describe and optionally raise again.
        :return: None when continuation is enabled and reporting succeeds; otherwise raise.
        """
        self._log_exception(stage + "_error", error, stage=stage, path=path)
        self._record_issue(state, _make_issue(stage, path, error))
        if not self.continue_on_error:
            raise error

    def _progress(self, event: str, **details: object) -> None:
        """
        Log an event, then synchronously deliver shallow details with active run identity to the
        observer.

        An active run_id overwrites any caller detail of the same name. Selected high-volume events
        log at DEBUG; others use INFO. Details are neither deep-copied nor scrubbed, and
        logging/callback errors propagate.

        Example:
            >>> coordinator._progress("source_checkpoint", files_adopted=2)  # doctest: +SKIP


        :param event: Event category used for log level and callback delivery.
        :param details: Keyword facts shallow-copied for callback/log context.
        :return: None after logging and optional observer delivery.
        """
        enriched = dict(details)
        if self._active_run_id is not None:
            enriched["run_id"] = str(self._active_run_id)
        self._emit_log(
            logging.DEBUG if event in _DEBUG_LOG_EVENTS else logging.INFO,
            event,
            "Mixed ingest event: " + event,
            **details,
        )
        if self.progress_callback is not None:
            self.progress_callback(event, enriched)

    def _emit_log(
        self,
        level: int,
        event: str,
        message: str,
        **details: object,
    ) -> None:
        """
        Emit a structured compatibility-logger record with event and shallow context metadata.

        Add/overwrite the active run_id without redaction; this helper does not catch logger
        failures or guarantee durable log publication.

        Example:
            >>> coordinator._emit_log(logging.INFO, "complete", "Ingest complete", count=2)  # doctest: +SKIP


        :param level: Logging severity passed unchanged to the logger.
        :param event: Value stored under liuxin_event.
        :param message: Human-readable log message.
        :param details: Facts copied into liuxin_context before adding active run identity.
        :return: None after the logger call returns.
        """
        context = dict(details)
        if self._active_run_id is not None:
            context["run_id"] = str(self._active_run_id)
        _LOGGER.log(
            level,
            message,
            extra={"liuxin_event": event, "liuxin_context": context},
        )

    def _log_exception(
        self,
        event: str,
        error: BaseException,
        *,
        level: int = logging.ERROR,
        **details: object,
    ) -> None:
        """
        Emit an exception record with its traceback, type/message, and active run context.

        Error type/message overwrite supplied details of those names. Raw exception text is retained
        without generic scrubbing; stringification/logging failures are not caught.

        Example:
            >>> coordinator._log_exception("container_error", error, path=path)  # doctest: +SKIP


        :param event: Structured failure category.
        :param error: Exception supplying class, message, and traceback.
        :param level: Logging severity, defaulting to ERROR.
        :param details: Additional shallow context fields.
        :return: None after the traceback-bearing log call returns.
        """
        context = dict(details)
        context.update(
            {
                "error_type": type(error).__name__,
                "error_message": str(error) or type(error).__name__,
            }
        )
        if self._active_run_id is not None:
            context["run_id"] = str(self._active_run_id)
        _LOGGER.log(
            level,
            "Mixed ingest exception: " + event,
            exc_info=(type(error), error, error.__traceback__),
            extra={"liuxin_event": event, "liuxin_context": context},
        )

    def _finish(self, state: _RunState, *, discovery_only: bool) -> MixedIngestReport:
        """
        Freeze current run counters/lists into a report and deliver the complete event.

        Clamp negative elapsed time to zero and sort format pairs. This copies containers into
        tuples without deep-copying their values or reconciling counters. Observer failure can
        prevent return after all prior writes and report construction.

        Example:
            >>> report = coordinator._finish(state, discovery_only=False)  # doctest: +SKIP


        :param state: Mutable state to project at completion or an observed halt.
        :param discovery_only: Requested mode recorded without independently checking state effects.
        :return: Report after successful complete-event logging/delivery.
        """
        elapsed = max(0.0, self.clock() - state.started)
        report = MixedIngestReport(
            run_id=state.run_id,
            source_root=str(state.source_root),
            discovery_only=discovery_only,
            source_store_ref=state.source_store_ref,
            source_store_created=state.source_store_created,
            files_examined=state.files_examined,
            files_adopted=state.files_adopted,
            loose_files=state.loose_files,
            skipped_symlinks=state.skipped_symlinks,
            top_level_containers=state.top_level_containers,
            containers_discovered=state.containers_discovered,
            containers_processed=state.containers_processed,
            containers_deduplicated=state.containers_deduplicated,
            members_discovered=state.members_discovered,
            members_adopted=state.members_adopted,
            assets_created=state.assets_created,
            replicas_created=state.replicas_created,
            expanded_bytes=state.expanded_bytes,
            materialized_bytes=state.materialized_bytes,
            recognized_formats=tuple(sorted(state.formats.items())),
            containers=tuple(state.containers),
            issues=tuple(state.issues),
            truncated=state.truncated,
            halt_reason=state.halt_reason,
            elapsed_s=elapsed,
        )
        self._progress(
            "complete",
            source_root=report.source_root,
            ok=report.ok,
            discovery_only=discovery_only,
            files_adopted=report.files_adopted,
            containers_processed=report.containers_processed,
            members_adopted=report.members_adopted,
            files_examined=report.files_examined,
            loose_files=report.loose_files,
            skipped_symlinks=report.skipped_symlinks,
            containers_discovered=report.containers_discovered,
            containers_deduplicated=report.containers_deduplicated,
            assets_created=report.assets_created,
            replicas_created=report.replicas_created,
            expanded_bytes=report.expanded_bytes,
            materialized_bytes=report.materialized_bytes,
            issue_count=len(report.issues),
            truncated=report.truncated,
            elapsed_s=report.elapsed_s,
            halt_reason=report.halt_reason,
        )
        return report


class _ContainerLimit(Exception):
    """
    Signal a branch-local member-size/path/expanded-byte ceiling inside container processing.

    The dedicated catch marks the current container truncated and stops its member loop; this
    exception does not itself halt every queued container.

    Example:
        >>> str(_ContainerLimit('member too large'))
        'member too large'
    """


class _MixedIngestOptions(TypedDict, total=False):
    """
    Describe optional constructor keywords accepted by ingest_mixed_local_tree.

    This total=False TypedDict supplies static typing only; dictionary construction does not
    validate values or apply defaults. The coordinator owns runtime checks.

    Example:
        >>> options: _MixedIngestOptions = {"recurse_containers": False}
        >>> options["recurse_containers"]
        False


    :ivar budget: Truthy budget value, or None for built-in ceilings.
    :ivar handlers: Truthy iterable of unique-format/unique-suffix handlers, otherwise built-in declarations.
    :ivar recursive_filesystem: Whether discovery descends into nonsymlink directories.
    :ivar recurse_containers: Whether adopted members can schedule nested container views.
    :ivar expand_ebook_containers: Whether terminal ebook suffixes may be identified as containers.
    :ivar continue_on_error: Whether ordinary handled source/member/container errors become issues instead of re-raising.
    :ivar verify_source_files: Verification policy forwarded to source adoption.
    :ivar verify_members: Verification policy forwarded to member adoption.
    :ivar materialization_store_ref: Existing local writable CACHE Store identity, mutually exclusive with a root at construction.
    :ivar materialization_root: Local cache path expanded/resolved at construction; created lazily for nested work.
    :ivar unsquashfs_exe: Executable selector supplied to SquashFS backend options.
    :ivar rar_extractor_exe: Optional extractor selector supplied to RAR backend options.
    :ivar backend_timeout_s: Finite positive per-call backend ceiling, bounded by the wall-time budget rather than its remaining time.
    :ivar progress_callback: Optional synchronous event/details observer; exceptions follow the enclosing boundary.
    :ivar cancellation_callback: Optional boolean cancellation poll; its exceptions propagate.
    :ivar source_metadata_factory: Truthy source metadata hook, otherwise the built-in name/provenance factory.
    :ivar member_metadata_factory: Truthy member metadata hook, otherwise the built-in context/hint factory.
    :ivar log_checkpoint_every: Positive value int-converted for periodic successful-source/member logging.
    :ivar clock: Callable providing elapsed-time readings in seconds, normally time.monotonic.
    """
    budget: MixedIngestBudget | None
    handlers: Iterable[ContainerHandler] | None
    recursive_filesystem: bool
    recurse_containers: bool
    expand_ebook_containers: bool
    continue_on_error: bool
    verify_source_files: bool
    verify_members: bool
    materialization_store_ref: UUID | None
    materialization_root: str | os.PathLike[str] | None
    unsquashfs_exe: str
    rar_extractor_exe: str | None
    backend_timeout_s: float
    progress_callback: ProgressCallback | None
    cancellation_callback: CancellationCallback | None
    source_metadata_factory: SourceMetadataFactory | None
    member_metadata_factory: MemberMetadataFactory | None
    log_checkpoint_every: int
    clock: Callable[[], float]


def ingest_mixed_local_tree(
    manager: manager_api.StorageManagerAPI,
    source_root: str | os.PathLike[str],
    *,
    discovery_only: bool = False,
    run_id: UUID | None = None,
    **options: Unpack[_MixedIngestOptions],
) -> MixedIngestReport:
    """
    Construct a fresh coordinator and run source classification or adoption once.

    Forward constructor options and execution selectors separately. Manager ownership remains with
    the caller; exceptions and partial effects are unchanged from direct coordinator use.

    Example:
        >>> report = ingest_mixed_local_tree(manager, source_root, discovery_only=True)  # doctest: +SKIP


    :param manager: Caller-owned manager supplying Store and metadata operations.
    :param source_root: Existing local directory passed to ingest.
    :param discovery_only: Request classification without manager writes.
    :param run_id: Optional correlation UUID, or None for a generated identity.
    :param options: Optional coordinator-constructor settings described by _MixedIngestOptions.
    :return: Coordinator report after successful finalization.
    """

    return MixedFormatIngestCoordinator(manager, **options).ingest(
        source_root, discovery_only=discovery_only, run_id=run_id
    )


def _source_metadata(
    path: Path, relative: str, handler: ContainerHandler | None
) -> manager_api.DigitalAssetMetadata:
    """
    Describe a source file using its basename, relative path, and optional selected format.

    Prefer the known container MIME mapping, otherwise guess by name. This is advisory metadata
    generation without a file read or ebook parsing.

    Example:
        >>> _source_metadata(Path('notes.txt'), 'notes.txt', None).original_name
        'notes.txt'


    :param path: Source path supplying the basename.
    :param relative: Source-Store key retained as provenance.
    :param handler: Selected container declaration, or None for an ordinary source file.
    :return: DigitalAssetMetadata with name, MIME guess, and workflow provenance.
    """
    attributes = [
        ("ingest.origin", "mixed-local-tree"),
        ("ingest.relative_path", relative),
    ]
    if handler is not None:
        attributes.append(("container.format", handler.format_name))
    media_type = _container_media_type(handler) or mimetypes.guess_type(path.name)[0]
    return manager_api.DigitalAssetMetadata(
        name=path.name,
        media_type=media_type,
        original_name=path.name,
        attributes=tuple(attributes),
    )


def _member_metadata(
    context: ContainerMemberContext, entry: storage_models.StoreInventoryEntry
) -> manager_api.DigitalAssetMetadata:
    """
    Combine parent context and advisory member hints into Asset metadata.

    Prefer the suggested filename/MIME; fall back to key basename and filename MIME guessing. Driver
    hint pairs are appended last and override duplicate built-in provenance keys through dict
    construction. No byte inspection or independent context validation occurs.

    Example:
        >>> metadata = _member_metadata(context, entry)  # doctest: +SKIP


    :param context: Parent format/depth/Asset facts supplied by orchestration.
    :param entry: Member Location and advisory filename/MIME/metadata hints.
    :return: Metadata with one value per attribute key and the selected name/MIME.
    """
    filename = entry.hints.suggested_filename or PurePosixPath(entry.location.key).name
    attributes = [
        ("ingest.origin", "mixed-local-tree"),
        ("container.format", context.format_name),
        ("container.depth", str(context.depth)),
        (
            "container.parent_asset_id",
            str(int(context.parent_digital_asset_id)),
        ),
        ("container.member_path", entry.location.key),
    ]
    attributes.extend(entry.hints.metadata)
    deduplicated = tuple(dict(attributes).items())
    return manager_api.DigitalAssetMetadata(
        name=filename,
        media_type=entry.hints.media_type or mimetypes.guess_type(filename)[0],
        original_name=filename,
        attributes=deduplicated,
    )


def _container_media_type(handler: ContainerHandler | None) -> str | None:
    """
    Map an exact built-in format label to its declared MIME type.

    Example:
        >>> _container_media_type(default_container_handlers()[1])
        'application/zip'


    :param handler: Selected declaration, or None when the file is not recognized as a container.
    :return: Known container MIME string or None for absent/unknown handlers.
    """
    if handler is None:
        return None
    return {
        "squashfs": "application/vnd.squashfs",
        "zip": "application/zip",
        "tar": "application/x-tar",
        "rar": "application/vnd.rar",
        "7z": "application/x-7z-compressed",
        "iso": "application/x-iso9660-image",
    }.get(handler.format_name)


def _make_issue(
    stage: str,
    path: str,
    error: BaseException,
    *,
    chain: tuple[str, ...] = (),
    member_path: str | None = None,
) -> MixedIngestIssue:
    """
    Describe an exception and ancestry as a nonfatal issue without changing the exception.

    Use the exception class name for an empty message. Text and paths are not redacted, and failures
    while stringifying the exception propagate.

    Example:
        >>> _make_issue('member', 'pack.zip', OSError('unreadable')).fatal
        False


    :param stage: Failure boundary label retained verbatim.
    :param path: Source/container display path.
    :param error: Exception supplying message and class name.
    :param chain: Ordered container display ancestry.
    :param member_path: Optional member key at the failure boundary.
    :return: Frozen issue value with the default fatal=False flag.
    """
    return MixedIngestIssue(
        stage=stage,
        path=path,
        message=str(error) or type(error).__name__,
        error_type=type(error).__name__,
        container_chain=chain,
        member_path=member_path,
    )


def _walk_identity_parts(*parts: str) -> str:
    """
    Encode ordered text components as surrogatepass UTF-8 hex separated by NUL.

    This ASCII representation preserves undecodable-name distinctions for uuid5; it does not
    normalize paths or hash file bytes.

    Example:
        >>> _walk_identity_parts('a', 'b').split(chr(0))
        ['61', '62']


    :param parts: Ordered identity components, possibly containing unpaired surrogates.
    :return: NUL-delimited ASCII identity string.
    """
    return "\0".join(
        part.encode("utf-8", "surrogatepass").hex() for part in parts
    )


def _operation_id(kind: str, *parts: str) -> UUID:
    """
    Derive a deterministic UUID5 from workflow version, operation kind, and encoded identity parts.

    The label supports repeatable manager calls; it is neither a content digest nor a reservation.

    Example:
        >>> _operation_id('source', 'a') == _operation_id('source', 'a')
        True


    :param kind: Operation category following the workflow version in the UUID seed.
    :param parts: Ordered additional identity text components.
    :return: UUID in the fixed mixed-ingest operation namespace.
    """
    return uuid5(
        _OPERATION_NAMESPACE,
        _walk_identity_parts(_WORKFLOW_VERSION, kind, *parts),
    )


def _sha256_value(record: manager_api.DigitalAssetRecord) -> str:
    """
    Read the first exactly named sha256 digest claim from the Asset record.

    No digest validation or byte hashing is performed; absence raises StorageIntegrityError.

    Example:
        >>> digest = _sha256_value(record)  # doctest: +SKIP


    :param record: Asset record whose digest declarations are inspected in order.
    :return: First matching digest value.
    """
    for digest in record.digests:
        if digest.algorithm == "sha256":
            return digest.value
    raise storage_errors.StorageIntegrityError(
        f"Digital Asset {record.digital_asset_id} has no SHA-256 identity."
    )


def _canonical_local_uri(value: str) -> str:
    """
    Resolve plain paths and local file URIs while retaining nonlocal authorities and other schemes.

    Percent-decoded file bytes use filesystem decoding before expanduser/resolve. Local URI
    query/fragment components are not retained in the resulting path URI. Resolution is not a
    race-free containment check.

    Example:
        >>> _canonical_local_uri('asset://digital-asset/7')
        'asset://digital-asset/7'


    :param value: Path or URI text used for configured-root matching.
    :return: Canonical local file URI or unchanged opaque/nonlocal URI.
    """
    parsed = urlparse(value)
    if parsed.scheme == "file":
        if parsed.netloc not in {"", "localhost"}:
            return value
        path = Path(os.fsdecode(unquote_to_bytes(parsed.path)))
        return path.expanduser().resolve(strict=False).as_uri()
    if parsed.scheme:
        return value
    return Path(value).expanduser().resolve(strict=False).as_uri()


def _store_name(prefix: str, path: Path) -> str:
    """
    Prepend a role label and colon to the shared sanitized path name.

    The helper's 160-character limit applies before the prefix. Its short input hash reduces
    collisions without proving unique identity.

    Example:
        >>> _store_name('zip', Path('pack.zip')).startswith('zip:')
        True


    :param prefix: Role or format label prepended without sanitization.
    :param path: Path supplied to safe_path_to_name with max_len=160.
    :return: Generated human-readable Store name.
    """
    return f"{prefix}:{safe_path_to_name(path, max_len=160)}"


__all__ = [
    "CancellationCallback",
    "ContainerHandler",
    "ContainerIngestReport",
    "ContainerMemberContext",
    "MemberMetadataFactory",
    "MixedFormatIngestCoordinator",
    "MixedIngestBudget",
    "MixedIngestIssue",
    "MixedIngestReport",
    "ProgressCallback",
    "SourceMetadataFactory",
    "default_container_handlers",
    "ingest_mixed_local_tree",
]
