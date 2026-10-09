"""
Compose transient discovery or SQLite-backed mixed-file ingestion for surfaces.

Requests carry coordinator options and callbacks, not a live service graph.
Discovery uses a transient StorageManager without opening the requested database;
real ingestion constructs Database and a database-backed manager here. Report
publication, process locking, signal handlers, and CLI exit codes belong to callers.
This layer neither wraps the run in a transaction nor converts raised failures
into result objects. Earlier database or storage effects can survive a failure.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from contextlib import nullcontext, redirect_stdout
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol
from uuid import UUID

from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.storage import api
from LiuXin_alpha.storage.ingest import (
    MixedFormatIngestCoordinator,
    MixedIngestBudget,
    MixedIngestReport,
)
from LiuXin_alpha.storage.durable_manager import StorageManager

type ProgressCallback = Callable[[str, Mapping[str, object]], None]
type CancellationCallback = Callable[[], bool]
type EventCallback = Callable[
    [int, str, str, Mapping[str, object]],
    None,
]


class TextOutput(Protocol):
    """
    Describe a writable text sink for legacy database-constructor stdout.

    Structural implementations need only write and flush; no file descriptor,
    seek support, ownership transfer, or close method is required. This protocol
    is not runtime-checkable. Redirecting stdout is process-global, not a
    thread-local capture of all ingestion output.

    Example:
        >>> import io
        >>> sink: TextOutput = io.StringIO()
        >>> sink.write('catalogue opened')
        16
    """

    def write(self, value: str, /) -> int:
        """
        Accept text written while database construction redirects stdout.

        Example:
            >>> import io
            >>> io.StringIO().write('ready')
            5


        :param value: Text fragment supplied positionally, possibly without a newline.
        :return: Number of characters accepted by the concrete sink.
        """
        ...

    def flush(self) -> None:
        """
        Flush buffered constructor output without closing the caller-owned sink.

        The application explicitly flushes after successful construction, not
        in a finally block. A sink failure propagates before service contexts
        have been entered.

        Example:
            >>> import io
            >>> io.StringIO().flush() is None
            True


        :return: None after the concrete sink performs its flush operation.
        """
        ...


@dataclass(frozen=True, slots=True)
class MixedIngestApplicationRequest:
    """
    Carry source selection, shared limits, and callback hooks for one ingest run.

    Construction stores values without path checks or cross-field validation.
    Frozen fields prevent reassignment, not mutation of referenced callbacks or
    sinks. Durable runs require database_path at execution; discovery ignores
    that path and database_stdout. Coordinator validation may fail only after
    application services have been constructed.

    Example:
        >>> request = MixedIngestApplicationRequest(
        ...     Path('incoming'), UUID(int=1), MixedIngestBudget(), discovery_only=True
        ... )
        >>> request.database_path is None
        True

    :ivar source_root: Local directory passed to the coordinator for resolution.
    :ivar run_id: Caller-assigned identity forwarded to the coordinator unchanged.
    :ivar budget: Shared count, byte, depth, expansion-ratio, and time ceilings.
    :ivar discovery_only: Classify source files without opening a durable catalogue.
    :ivar database_path: SQLite target for a real run, or None for discovery.
    :ivar recursive_filesystem: Whether the coordinator descends source directories.
    :ivar recurse_containers: Whether discovered nested containers may be expanded.
    :ivar expand_ebook_containers: Whether ebook containers such as EPUB expose members.
    :ivar continue_on_error: Coordinator policy for continuing after isolated failures.
    :ivar verify: Enable both source-file and container-member verification.
    :ivar materialization_root: Optional local cache root for nested-container bytes.
    :ivar unsquashfs_exe: SquashFS reader executable name or path.
    :ivar rar_extractor_exe: Explicit RAR extractor, or None for backend selection.
    :ivar backend_timeout_s: Per-backend timeout in seconds; must be finite and positive.
    :ivar log_checkpoint_every: Positive coordinator logging checkpoint interval.
    :ivar progress_callback: Optional synchronous event-name/detail progress consumer.
    :ivar cancellation_callback: Optional polled predicate requesting cooperative stop.
    :ivar event_callback: Optional level/event/message/detail consumer for setup events.
    :ivar database_stdout: Optional caller-owned sink for database construction only.
    """

    source_root: Path
    run_id: UUID
    budget: MixedIngestBudget
    discovery_only: bool = False
    database_path: Path | None = None
    recursive_filesystem: bool = True
    recurse_containers: bool = True
    expand_ebook_containers: bool = False
    continue_on_error: bool = True
    verify: bool = False
    materialization_root: str | None = None
    unsquashfs_exe: str = "unsquashfs"
    rar_extractor_exe: str | None = None
    backend_timeout_s: float = 60.0
    log_checkpoint_every: int = 1_000
    progress_callback: ProgressCallback | None = None
    cancellation_callback: CancellationCallback | None = None
    event_callback: EventCallback | None = None
    database_stdout: TextOutput | None = None


@dataclass(frozen=True, slots=True)
class MixedIngestApplicationResult:
    """
    Retain a coordinator report and application-mode facts after contexts exit.

    This is not a serialized CLI receipt or an independent durability check.
    The application returns mode ``discovery`` with no database and false
    metadata_is_durable, or mode ``ingest`` with the resolved database path and
    the manager's database-backed-metadata flag. Direct construction does not
    enforce that relationship. Report health alone determines ok; setup warning
    events are not added to the report by this layer.

    Example:
        >>> result = execute_mixed_ingest(request)  # doctest: +SKIP
        >>> result.report.run_id == request.run_id  # doctest: +SKIP
        True

    :ivar mode: Application branch label, normally ``discovery`` or ``ingest``.
    :ivar report: Unmodified coordinator report, including partial progress and issues.
    :ivar budget: The request's budget object, not a remaining-budget calculation.
    :ivar database_path: Resolved durable target, or None for discovery.
    :ivar metadata_is_durable: Manager metadata binding observed before context exit.
    """

    mode: str
    report: MixedIngestReport
    budget: MixedIngestBudget
    database_path: Path | None = None
    metadata_is_durable: bool = False

    @property
    def ok(self) -> bool:
        """
        Project coordinator report health without rechecking storage or setup events.

        MixedIngestReport rejects issues, truncation, and any halt reason. The
        projection does not additionally require metadata_is_durable or inspect
        bootstrap warnings emitted outside that report.

        Example:
            >>> result.ok == bool(result.report.ok)  # doctest: +SKIP
            True


        :return: Boolean value of the retained report's ok property.
        """

        return bool(self.report.ok)


def _emit(
    request: MixedIngestApplicationRequest,
    level: int,
    event: str,
    message: str,
    **details: object,
) -> None:
    """
    Deliver one setup event synchronously when the request supplies a consumer.

    Keyword details form a fresh outer dictionary; nested values are not copied
    or sanitized. This helper does not add run identity, log independently,
    suppress callback errors, or retry delivery.

    Example:
        >>> request = MixedIngestApplicationRequest(Path('.'), UUID(int=1), MixedIngestBudget())
        >>> _emit(request, logging.INFO, 'opened', 'Catalogue opened') is None
        True


    :param request: Application settings holding the optional event callback.
    :param level: Numeric logging severity passed to the callback unchanged.
    :param event: Machine-readable setup event name.
    :param message: Human-readable event description.
    :param details: Event-specific values passed as the callback's final mapping.
    :return: None after delivery, or immediately when no callback is configured.
    """
    callback = request.event_callback
    if callback is None:
        return
    callback(
        level,
        event,
        message,
        details,
    )


def _coordinator(
    manager: api.StorageManagerAPI,
    request: MixedIngestApplicationRequest,
) -> MixedFormatIngestCoordinator:
    """
    Construct the canonical coordinator with request-owned limits and hooks.

    One verify option enables both source and member verification. Built-in
    handler selection, metadata factories, clock, and materialization-Store
    selection remain coordinator defaults. Construction can validate settings
    and normalize the materialization path; it does not start ingestion or take
    ownership of the manager's context lifecycle.

    Example:
        >>> with StorageManager() as manager:  # doctest: +SKIP
        ...     coordinator = _coordinator(manager, request)


    :param manager: Storage services already selected for discovery or durable work.
    :param request: Settings whose coordinator-specific fields are forwarded.
    :return: A new coordinator ready for one explicitly invoked ingest call.
    :raises ValueError: Coordinator settings fail validation, including timeout or interval.
    """
    return MixedFormatIngestCoordinator(
        manager,
        budget=request.budget,
        recursive_filesystem=request.recursive_filesystem,
        recurse_containers=request.recurse_containers,
        expand_ebook_containers=request.expand_ebook_containers,
        continue_on_error=request.continue_on_error,
        verify_source_files=request.verify,
        verify_members=request.verify,
        materialization_root=request.materialization_root,
        unsquashfs_exe=request.unsquashfs_exe,
        rar_extractor_exe=request.rar_extractor_exe,
        backend_timeout_s=request.backend_timeout_s,
        progress_callback=request.progress_callback,
        cancellation_callback=request.cancellation_callback,
        log_checkpoint_every=request.log_checkpoint_every,
    )


def execute_mixed_ingest(
    request: MixedIngestApplicationRequest,
) -> MixedIngestApplicationResult:
    """
    Execute one discovery or SQLite-backed ingest and return its unmodified report.

    Discovery opens only a transient manager and emits no application setup
    events. A real run resolves the database path, decides create from exists(),
    and constructs SQLite with backup and its automatic storage manager disabled.
    That existence observation is not a reservation or database-schema check.

    Optional stdout redirection covers only Database construction, followed by
    an explicit sink flush. Manager construction and the open-complete event
    occur before entering ``with database, manager``; failures in those steps
    are not covered by that context cleanup. Once entered, manager exit precedes
    database exit. This wrapper provides no all-run rollback or cleanup of newly
    created paths on failure.

    Existing databases bootstrap registered Stores before ingestion. Issues and
    a false bootstrap ok emit warnings but do not prevent the coordinator call
    or independently make the returned result unhealthy. The durability flag
    records manager metadata binding before exit, not an fsync verification.
    Setup, callback, coordinator, and context-exit exceptions propagate without
    an application result; earlier persistent effects may already exist.

    Example:
        >>> request = MixedIngestApplicationRequest(Path('.'), UUID(int=1), MixedIngestBudget())
        >>> execute_mixed_ingest(request)
        Traceback (most recent call last):
        ...
        ValueError: database_path is required for durable mixed ingest


    :param request: Run settings and caller-owned callbacks; not mutated here.
    :return: Mode, report, original budget, and applicable database-binding facts.
    :raises ValueError: A real run lacks a database path or coordinator options are invalid.
    :raises FileNotFoundError: The coordinator cannot find the selected source directory.
    :raises NotADirectoryError: The coordinator source exists but is not a directory.
    """

    if request.discovery_only:
        with StorageManager() as manager:
            report = _coordinator(manager, request).ingest(
                request.source_root,
                discovery_only=True,
                run_id=request.run_id,
            )
        return MixedIngestApplicationResult(
            mode="discovery",
            report=report,
            budget=request.budget,
        )

    database_path = request.database_path
    if database_path is None:
        raise ValueError("database_path is required for durable mixed ingest")
    database_path = database_path.expanduser().resolve(strict=False)
    create = not database_path.exists()
    _emit(
        request,
        logging.INFO,
        "database_open_started",
        "Opening LiuXin catalogue",
        database=str(database_path),
        create=create,
    )
    stdout_context = (
        nullcontext()
        if request.database_stdout is None
        else redirect_stdout(request.database_stdout)
    )
    with stdout_context:
        database = Database(
            metadata={"database_path": str(database_path)},
            db_type="SQLite",
            create=create,
            backup=False,
            enable_storage_manager=False,
        )
    if request.database_stdout is not None:
        request.database_stdout.flush()
    _emit(
        request,
        logging.INFO,
        "database_open_complete",
        "LiuXin catalogue opened",
        database=str(database_path),
        created=create,
    )

    manager = StorageManager(db=database, startup_on_add=True)
    with database, manager:
        if not create:
            bootstrap = manager.load_from_database(startup=True)
            for issue in bootstrap.issues:
                _emit(
                    request,
                    logging.WARNING,
                    "store_bootstrap_issue",
                    "Store bootstrap warning",
                    store_ref=(
                        None if issue.store_ref is None else str(issue.store_ref)
                    ),
                    store_name=issue.store_name,
                    reason=issue.reason,
                )
            _emit(
                request,
                logging.INFO if bootstrap.ok else logging.WARNING,
                "store_bootstrap_complete",
                "Store bootstrap complete",
                discovered_configurations=bootstrap.discovered_configurations,
                loaded_stores=bootstrap.loaded_stores,
                skipped_configurations=bootstrap.skipped_configurations,
                failed_configurations=bootstrap.failed_configurations,
                issue_count=len(bootstrap.issues),
                ok=bootstrap.ok,
            )
        report = _coordinator(manager, request).ingest(
            request.source_root,
            run_id=request.run_id,
        )
        metadata_is_durable = manager.metadata_is_durable

    return MixedIngestApplicationResult(
        mode="ingest",
        report=report,
        budget=request.budget,
        database_path=database_path,
        metadata_is_durable=metadata_is_durable,
    )


__all__ = [
    "MixedIngestApplicationRequest",
    "MixedIngestApplicationResult",
    "MixedIngestBudget",
    "execute_mixed_ingest",
]
