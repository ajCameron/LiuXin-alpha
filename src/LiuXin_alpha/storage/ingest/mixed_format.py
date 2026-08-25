"""Bounded, restart-friendly ingestion of mixed local files and containers.

The coordinator catalogues every regular source file in place, exposes known
container formats through immutable Asset-backed Stores, and recursively does
the same for container members.  It deliberately owns orchestration limits in
addition to the per-backend parser limits: a collection of individually valid
archives must not be able to evade a run-wide byte, member, depth, or time
budget.

This module does not extract trees into caller-controlled paths.  Nested
containers are copied only into an explicitly configured managed cache Store,
then opened through the normal backed-Store resolver.
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

from LiuXin_alpha.storage import api
from LiuXin_alpha.storage.backend_registry import DEFAULT_BACKEND_REGISTRY
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
    [Path, str, "ContainerHandler | None"], api.DigitalAssetMetadata
]


@dataclasses.dataclass(slots=True, frozen=True)
class MixedIngestBudget:
    """Hard ceilings shared by an entire mixed-format ingest run."""

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
    """Declarative mapping from container signatures to one Store backend."""

    format_name: str
    backend_kind: str
    protocol: str
    suffixes: tuple[str, ...]
    magic_signatures: tuple[tuple[int, bytes], ...]

    def __post_init__(self) -> None:
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
        lowered = name.lower()
        return any(lowered.endswith(suffix) for suffix in self.suffixes)

    def matches_probe(self, probe: bytes) -> bool:
        return any(
            probe[offset : offset + len(signature)] == signature
            for offset, signature in self.magic_signatures
        )


@dataclasses.dataclass(slots=True, frozen=True)
class ContainerMemberContext:
    """Stable technical context supplied to member metadata enrichment."""

    container_path: str
    format_name: str
    depth: int
    parent_digital_asset_id: api.DigitalAssetID
    container_chain: tuple[str, ...]


MemberMetadataFactory = Callable[
    [ContainerMemberContext, api.StoreInventoryEntry], api.DigitalAssetMetadata
]


def default_container_handlers() -> tuple[ContainerHandler, ...]:
    """Return the built-in immutable-container handler registry."""

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
    """One isolated failure or enforced safety limit with full ancestry."""

    stage: str
    path: str
    message: str
    error_type: str
    container_chain: tuple[str, ...] = ()
    member_path: str | None = None
    fatal: bool = False


@dataclasses.dataclass(slots=True, frozen=True)
class ContainerIngestReport:
    """Result for one discovered top-level or nested container."""

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
        return self.store_ref is not None and self.duplicate_of is None

    @property
    def ok(self) -> bool:
        return not self.issues and not self.truncated


@dataclasses.dataclass(slots=True, frozen=True)
class MixedIngestReport:
    """Durable-catalogue and resource-accounting summary for one run."""

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
        return not self.issues and not self.truncated and self.halt_reason is None


@dataclasses.dataclass(slots=True, frozen=True)
class _ContainerCandidate:
    display_path: str
    filename: str
    handler: ContainerHandler
    digital_asset_id: api.DigitalAssetID
    source_replica_id: api.ReplicaID
    size_bytes: int
    depth: int
    ancestry: tuple[str, ...]
    chain: tuple[str, ...]
    top_level: bool


@dataclasses.dataclass(slots=True)
class _Discovery:
    files: list[Path] = dataclasses.field(default_factory=list)
    skipped_symlinks: int = 0
    issues: list[MixedIngestIssue] = dataclasses.field(default_factory=list)
    truncated: bool = False


@dataclasses.dataclass(slots=True)
class _RunState:
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
    """Catalogue a local mess using immutable Stores and cumulative limits.

    ``discovery_only`` performs a bounded top-level classification pass and
    does not create Stores, Digital Assets, Replicas, or cache files.  A real
    run adopts all regular source files, while recursive container expansion
    requires ``materialization_store_ref`` or ``materialization_root`` only
    when a nested container is actually encountered.
    """

    def __init__(
        self,
        manager: api.StorageManagerAPI,
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
        self._replica_locations_by_store: dict[UUID, set[api.Location]] = {}
        self._active_run_id: UUID | None = None

    def ingest(
        self,
        source_root: str | os.PathLike[str],
        *,
        discovery_only: bool = False,
        run_id: UUID | None = None,
    ) -> MixedIngestReport:
        """Run a discovery-only classification or a durable catalogue pass."""

        if self._active_run_id is not None:
            raise api.StoragePreconditionFailed(
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
        """Execute one already-identified run and retain its logging context."""

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
                    replica_mode=api.ReplicaMode.UNMANAGED,
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
        issues: list[MixedIngestIssue] = []
        materialized = 0
        cache_ref: UUID | None = None
        configuration: api.StoreConfiguration | None = None
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
                    raise api.StoragePreconditionFailed(
                        "nested container expansion requires a local writable "
                        + "materialization Store; supply materialization_store_ref "
                        + "or materialization_root"
                    )
                if not self._has_cache_replica(candidate.digital_asset_id, cache_ref):
                    if candidate.size_bytes > self.budget.max_temporary_bytes:
                        raise api.StoragePreconditionFailed(
                            f"nested container is {candidate.size_bytes} bytes, above "
                            + "the single-materialization limit "
                            + str(self.budget.max_temporary_bytes)
                        )
                    if (
                        state.materialized_bytes + candidate.size_bytes
                        > self.budget.max_materialized_bytes
                    ):
                        raise api.StoragePreconditionFailed(
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
                        replica_mode=api.ReplicaMode.ARCHIVE,
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
        def read_range(offset: int, length: int) -> bytes:
            with path.open("rb") as source:
                _ = source.seek(offset)
                return source.read(length)

        return self._identify(path.name, read_range)

    def _identify_store_entry(
        self, store: api.StoreAPI, entry: api.StoreInventoryEntry
    ) -> ContainerHandler | None:
        name = entry.hints.suggested_filename or PurePosixPath(entry.location.key).name

        def read_range(offset: int, length: int) -> bytes:
            with store.open_read(entry.location, offset=offset, length=length) as source:
                return source.read(length)

        return self._identify(name, read_range)

    def _identify(self, name: str, read_range: RangeReader) -> ContainerHandler | None:
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
    ) -> tuple[api.StoreConfiguration, bool]:
        root_uri = root.as_uri()
        existing = self._configuration_for_root(root_uri)
        if existing is not None:
            canonical = DEFAULT_BACKEND_REGISTRY.canonical_kind(existing.store_kind)
            if canonical not in {
                "filesystem",
                "on_disk_existing_managed_drive",
                "on_disk_existing_unmanaged_drive",
            }:
                raise api.StoragePreconditionFailed(
                    f"Store root {root_uri!r} is configured as incompatible "
                    + f"backend {existing.store_kind!r}."
                )
            if api.ReplicaMode.UNMANAGED not in existing.supported_replica_modes:
                raise api.StoragePreconditionFailed(
                    f"Store root {root_uri!r} does not permit UNMANAGED "
                    + "Replica adoption."
                )
            self._require_available(existing)
            return existing, False
        configuration = api.StoreConfiguration.for_backend(
            _store_name("ingest-source", root),
            "on_disk_existing_unmanaged_drive",
            root,
            protocol="file",
            tags=("ingest-source", "unmanaged", "mixed-ingest"),
            modes=(api.ReplicaMode.UNMANAGED,),
            operational_role="live",
            read_only=True,
            folders=True,
        )
        _ = self.manager.create_store(configuration, startup=True)
        self._require_available(configuration)
        return configuration, True

    def _ensure_materialization_store(self) -> UUID | None:
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
        configuration = api.StoreConfiguration.for_backend(
            _store_name("ingest-cache", root),
            "filesystem",
            root,
            protocol="file",
            tags=("cache", "ingest-materialization", "mixed-ingest"),
            modes=(api.ReplicaMode.CACHE,),
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
    ) -> tuple[api.StoreConfiguration, bool]:
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
            raise api.StoragePreconditionFailed(
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
            modes=(api.ReplicaMode.ARCHIVE,),
            operational_role="archive",
            folders=True,
            options=option_pairs,
            start=True,
        )
        self._require_available(configuration)
        return configuration, True

    def _backend_options(self, handler: ContainerHandler) -> dict[str, object]:
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
        if size < 0:
            raise api.StorageIntegrityError("container member reports a negative size")
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
        self, configuration: api.StoreConfiguration
    ) -> None:
        if configuration.read_only:
            raise api.StoragePreconditionFailed(
                "materialization Store must be writable"
            )
        if api.ReplicaMode.CACHE not in configuration.supported_replica_modes:
            raise api.StoragePreconditionFailed(
                "materialization Store must support CACHE Replicas"
            )
        if DEFAULT_BACKEND_REGISTRY.canonical_kind(configuration.store_kind) != "filesystem":
            raise api.StoragePreconditionFailed(
                "materialization Store must currently be a local filesystem Store"
            )

    def _validate_materialization_root(self, source_root: Path) -> None:
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
    ) -> api.StoreConfiguration | None:
        target = _canonical_local_uri(root_uri)
        matches = tuple(
            configuration
            for configuration in self.manager.iter_store_configurations()
            if _canonical_local_uri(configuration.store_root_uri) == target
        )
        if len(matches) > 1:
            raise api.StoragePreconditionFailed(
                f"multiple configured Stores claim local root {root_uri!r}"
            )
        return matches[0] if matches else None

    def _require_available(self, configuration: api.StoreConfiguration) -> None:
        try:
            store = self.manager.get_store(configuration.store_uuid)
        except api.StoreUnavailable:
            _ = self.manager.update_store(
                configuration.store_uuid, configuration
            )
            store = self.manager.get_store(configuration.store_uuid)
        status = store.status(refresh=True)
        if not status.available:
            raise api.StoreUnavailable(
                status.message or f"Store {configuration.store_name!r} is unavailable."
            )

    def _has_replica_at(self, location: api.Location) -> bool:
        locations = self._replica_locations_by_store.get(location.store_ref)
        if locations is None:
            locations = {
                record.location
                for record in self.manager.iter_replica_records(
                    store_ref=location.store_ref
                )
                if record.state is not api.ReplicaState.DELETED
            }
            self._replica_locations_by_store[location.store_ref] = locations
        return location in locations

    def _remember_replica(self, location: api.Location) -> None:
        self._replica_locations_by_store.setdefault(location.store_ref, set()).add(
            location
        )

    def _has_cache_replica(
        self, digital_asset_id: api.DigitalAssetID, store_ref: UUID
    ) -> bool:
        asset = self.manager.get_digital_asset_record(digital_asset_id)
        for record in self.manager.iter_replica_records(store_ref=store_ref):
            if (
                record.digital_asset_id != digital_asset_id
                or record.mode is not api.ReplicaMode.CACHE
                or record.state is api.ReplicaState.DELETED
            ):
                continue
            try:
                info = self.manager.stat(record.location)
            except api.StorageError:
                continue
            if info.size == asset.size_bytes:
                return True
        return False

    def _remaining_backend_timeout(self) -> float:
        # Container configurations are durable, so use a stable per-call ceiling
        # rather than embedding the momentary run remainder in their identity.
        return min(self.backend_timeout_s, self.budget.max_wall_time_s)

    def _should_halt(self, state: _RunState) -> bool:
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
        self._log_exception(stage + "_error", error, stage=stage, path=path)
        self._record_issue(state, _make_issue(stage, path, error))
        if not self.continue_on_error:
            raise error

    def _progress(self, event: str, **details: object) -> None:
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
    """Internal control-flow marker for a branch-local safety ceiling."""


class _MixedIngestOptions(TypedDict, total=False):
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
    manager: api.StorageManagerAPI,
    source_root: str | os.PathLike[str],
    *,
    discovery_only: bool = False,
    run_id: UUID | None = None,
    **options: Unpack[_MixedIngestOptions],
) -> MixedIngestReport:
    """Construct a coordinator and ingest one local source tree."""

    return MixedFormatIngestCoordinator(manager, **options).ingest(
        source_root, discovery_only=discovery_only, run_id=run_id
    )


def _source_metadata(
    path: Path, relative: str, handler: ContainerHandler | None
) -> api.DigitalAssetMetadata:
    attributes = [
        ("ingest.origin", "mixed-local-tree"),
        ("ingest.relative_path", relative),
    ]
    if handler is not None:
        attributes.append(("container.format", handler.format_name))
    media_type = _container_media_type(handler) or mimetypes.guess_type(path.name)[0]
    return api.DigitalAssetMetadata(
        name=path.name,
        media_type=media_type,
        original_name=path.name,
        attributes=tuple(attributes),
    )


def _member_metadata(
    context: ContainerMemberContext, entry: api.StoreInventoryEntry
) -> api.DigitalAssetMetadata:
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
    return api.DigitalAssetMetadata(
        name=filename,
        media_type=entry.hints.media_type or mimetypes.guess_type(filename)[0],
        original_name=filename,
        attributes=deduplicated,
    )


def _container_media_type(handler: ContainerHandler | None) -> str | None:
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
    return MixedIngestIssue(
        stage=stage,
        path=path,
        message=str(error) or type(error).__name__,
        error_type=type(error).__name__,
        container_chain=chain,
        member_path=member_path,
    )


def _walk_identity_parts(*parts: str) -> str:
    return "\0".join(
        part.encode("utf-8", "surrogatepass").hex() for part in parts
    )


def _operation_id(kind: str, *parts: str) -> UUID:
    return uuid5(
        _OPERATION_NAMESPACE,
        _walk_identity_parts(_WORKFLOW_VERSION, kind, *parts),
    )


def _sha256_value(record: api.DigitalAssetRecord) -> str:
    for digest in record.digests:
        if digest.algorithm == "sha256":
            return digest.value
    raise api.StorageIntegrityError(
        f"Digital Asset {record.digital_asset_id} has no SHA-256 identity."
    )


def _canonical_local_uri(value: str) -> str:
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
