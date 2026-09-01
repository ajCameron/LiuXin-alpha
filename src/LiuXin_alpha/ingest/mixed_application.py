"""Application composition for bounded mixed local ingestion.

This module owns the service graph needed by a mixed-ingest run.  Presentation
surfaces supply validated options and callbacks; they do not construct LiuXin
databases, storage managers, or storage coordinators themselves.
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
from LiuXin_alpha.storage.store_manager import StorageManager

type ProgressCallback = Callable[[str, Mapping[str, object]], None]
type CancellationCallback = Callable[[], bool]
type EventCallback = Callable[
    [int, str, str, Mapping[str, object]],
    None,
]


class TextOutput(Protocol):
    """Minimal text sink required to capture legacy database stdout."""

    def write(self, value: str, /) -> int: ...

    def flush(self) -> None: ...


@dataclass(frozen=True, slots=True)
class MixedIngestApplicationRequest:
    """Transport-neutral settings for one mixed-ingest application run."""

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
    """Domain result returned to CLI, RPC, or future scheduler surfaces."""

    mode: str
    report: MixedIngestReport
    budget: MixedIngestBudget
    database_path: Path | None = None
    metadata_is_durable: bool = False

    @property
    def ok(self) -> bool:
        """Return whether the coordinator completed without recorded issues."""

        return bool(self.report.ok)


def _emit(
    request: MixedIngestApplicationRequest,
    level: int,
    event: str,
    message: str,
    **details: object,
) -> None:
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
    """Run discovery or durable ingestion using the canonical service graph."""

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
                        None
                        if issue.store_ref is None
                        else str(issue.store_ref)
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
