"""
Coordinate operator-visible indexing and checkpointed SquashFS pack production.

The prototype composes Library, schema-dependent indexers, planning, execution, persistence,
and Store registration without a transaction spanning those phases. The direct-FRBR path
retains a legacy SHA-512-plus-size fingerprint in SHA-256-named columns. Progress and result
values report observed incremental effects; failures can leave completed earlier work.
"""

from __future__ import annotations

import dataclasses
import pathlib
import sys
import time
from collections.abc import Callable, Iterable, Sequence
from typing import Any, TYPE_CHECKING
from uuid import UUID, uuid4

from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.storage.api import BackupWorkflowDeclaration, WorkflowStatus
from LiuXin_alpha.storage.backup.backup_artifact_registry import BackupArtifactRegistry
from LiuXin_alpha.storage.backup.backup_workflow_repository import BackupWorkflowRepository
from LiuXin_alpha.storage.backup.squashfs_backup_workflow import SquashfsBackupWorkflow
from LiuXin_alpha.storage.backup.store_backup_planner import StoreBackupPlanner
from LiuXin_alpha.storage.reconcile import register_existing_disk_as_unmanaged_store
from LiuXin_alpha.storage.reconcile.models import UnmanagedDiskRegistrationReport
from LiuXin_alpha.storage.store_backend_plugins.on_disk_existing_unmanaged_drive import OnDiskUnmanagedStorageBackend
from LiuXin_alpha.utils.storage.local.file_properties import get_file_hash
from LiuXin_alpha.utils.text.safe_path_to_name import safe_path_to_name

if TYPE_CHECKING:
    from LiuXin_alpha.library import Library


def _now_ep_ms() -> int:
    """
    Read wall-clock Unix time in milliseconds and truncate it to an integer.

    This is neither a monotonic clock nor a uniqueness source; adjustments can make later
    observations earlier.

    Example:
        >>> isinstance(_now_ep_ms(), int)
        True


    :return: Integer epoch milliseconds from time.time().
    """
    return int(time.time() * 1000)


def _format_bytes(value: int) -> str:
    """
    Render an int-converted nonnegative byte count using binary units through TiB.

    Convert to int, clamp below zero, then scale a float by 1024. Bytes use an integer spelling;
    larger units use one decimal. TiB is the final unit even for larger values. Conversion/overflow
    failures propagate without a custom validation layer.

    Example:
        >>> _format_bytes(1536)
        '1.5 KiB'
        >>> _format_bytes(-1)
        '0 B'


    :param value: Int-convertible byte count to format for operator output.
    :return: Human-readable size with a B, KiB, MiB, GiB, or TiB suffix.
    """
    size = float(max(0, int(value)))
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    for unit in units:
        if size < 1024.0 or unit == units[-1]:
            if unit == "B":
                return f"{int(size)} {unit}"
            return f"{size:.1f} {unit}"
        size /= 1024.0
    return f"{int(value)} B"


def _render_progress(current: int, total: int | None, *, width: int = 28) -> str:
    """
    Render a bounded progress bar or repeating unknown-total pulse.

    Int-convert and clamp current below zero. None/nonpositive totals select a hash pulse cycling
    modulo width+1 with the unbounded current count. Positive totals are int-converted and bounded
    below by one; clamp displayed current at that total and show one-decimal percent. Width is not
    validated, and this helper does not print or retain progress.

    Example:
        >>> _render_progress(3, 4, width=4)
        '[###-] 3/4 ( 75.0%)'
        >>> _render_progress(6, None, width=4)
        '[#   ] 6'


    :param current: Observed count, int-converted and clamped to zero before display.
    :param total: Expected count or None/nonpositive input for the unknown-total pulse.
    :param width: Requested character width used directly in bar arithmetic and formatting.
    :return: Bracketed bar/pulse with count and optional percentage; malformed numeric/width input may raise.
    """
    current = max(0, int(current))
    if total is None or total <= 0:
        spinner = "#" * min(width, (current % (width + 1)))
        return f"[{spinner:<{width}}] {current}"
    total = max(1, int(total))
    clamped = min(current, total)
    filled = int(width * (clamped / total))
    bar = "#" * filled + "-" * (width - filled)
    pct = (clamped / total) * 100.0
    return f"[{bar}] {clamped}/{total} ({pct:5.1f}%)"


def _normalize_ebook_extensions(ebook_extensions: Iterable[str] | None) -> set[str]:
    """
    Collect lowercase suffix tokens, using project book extensions when input is None.

    Filter entries whose string spelling is whitespace-only, but do not strip whitespace from
    retained spellings. Strip leading dots only after lowercase conversion. Thus " EPUB " retains
    surrounding spaces while ".EPUB" becomes "epub". A supplied empty iterable stays empty, and a
    bare string is consumed character by character.

    Example:
        >>> sorted(_normalize_ebook_extensions([".EPUB", "mobi", " "]))
        ['epub', 'mobi']


    :param ebook_extensions: Optional iterable of extension spellings; None imports BOOK_EXTENSIONS as the default.
    :return: Set of normalized spellings, without a leading-dot or whitespace validity check beyond the stated transformation.
    """
    if ebook_extensions is None:
        from LiuXin_alpha.constants.file_extensions import BOOK_EXTENSIONS
        ebook_extensions = BOOK_EXTENSIONS
    return {str(x).lower().lstrip('.') for x in ebook_extensions if str(x).strip()}


def _table_columns(db, table_name: str) -> set[str]:
    """
    Return a set of the database adapter's reported column headings without caching or schema
    checks.

    Example:
        >>> columns = _table_columns(db, "stores")  # doctest: +SKIP


    :param db: Borrowed database exposing get_column_headings.
    :param table_name: Schema table whose reported column names are requested.
    :return: New set of column names; adapter/iteration errors propagate.
    """
    return set(db.get_column_headings(table_name))


def _ensure_or_create_unmanaged_store_row(db, *, root: pathlib.Path, store_name: str, store_kind: str = "on_disk_existing_unmanaged_drive"):
    """
    Prepare a local unmanaged backend and upsert its Store metadata by raw root text.

    Instantiate the fixed unmanaged backend before searching str(root); canonical file-URI rows are
    not searched here. Reuse the first match, reconstructing the backend with a persisted UUID when
    available or filling a missing UUID. Apply changed allowed fields and sync once. The requested
    store_kind is a persisted label independent of the concrete backend class.

    New rows use reported columns and wall-clock created/modified timestamps. No transaction,
    root-identity deduplication across URI spellings, manager attachment, or backend startup is
    added. Earlier row mutations or backend setup can precede a later failure.

    Example:
        >>> row, backend = _ensure_or_create_unmanaged_store_row(  # doctest: +SKIP
        ...     db, root=pathlib.Path("/books"), store_name="source",
        ... )


    :param db: Borrowed database supporting Store lookup and Row insertion/update.
    :param root: Local source root used verbatim as the Store lookup/persistence string.
    :param store_name: Display name used by the unmanaged backend and persisted payload.
    :param store_kind: Store-kind label written to admitted columns; does not change the instantiated backend class.
    :return: Pair of persisted Store row and unmanaged backend sharing the selected UUID.
    """
    backend = OnDiskUnmanagedStorageBackend(url=str(root), name=store_name)
    rows = db.search("stores", "store_root_uri", str(root))
    payload = {
        "store_uuid": str(backend.store_ref),
        "store_name": backend.configuration.store_name,
        "store_kind": store_kind,
        "store_access_protocol": "file",
        "store_root_uri": str(root),
        "store_operational_role": "live",
        "store_is_read_only": 1,
        "store_online_status": "online",
        "store_supports_random_read": 1,
        "store_supports_random_write": 0,
        "store_supports_delete": 0,
        "store_supports_folders": 1,
        "store_supports_checksums": 1,
    }
    if rows:
        row = rows[0]
        changed = False
        persisted_uuid = row["store_uuid"] if "store_uuid" in row.allowed_columns else None
        if persisted_uuid not in (None, ""):
            backend = OnDiskUnmanagedStorageBackend(
                url=str(root),
                name=store_name,
                uuid=str(persisted_uuid),
            )
        elif "store_uuid" in row.allowed_columns:
            row["store_uuid"] = str(backend.store_ref)
            changed = True
        payload["store_uuid"] = str(backend.store_ref)
        for key, value in payload.items():
            if key in row.allowed_columns and row[key] != value:
                row[key] = value
                changed = True
        if changed:
            row.sync()
        return row, backend
    store_columns = _table_columns(db, "stores")
    now = _now_ep_ms()
    payload["store_created_timestamp_ep_k"] = now
    payload["store_modified_timestamp_ep_k"] = now
    row_dict = {k: v for k, v in payload.items() if k in store_columns}
    return Row.from_idless_row_dict(db, row_dict=row_dict, table="stores"), backend


def _index_existing_disk_frbr(db, *, disk_path: pathlib.Path, store_name: str, ebook_extensions: Iterable[str] | None, progress_callback=None) -> UnmanagedDiskRegistrationReport:
    """
    Index a local tree directly into Digital Asset and Replica rows for the FRBR schema path.

    Resolve the root, ensure its Store row, emit start, normalize extensions, and map existing
    Replicas by storage key with the last match winning. Materialize/sort paths whose is_file
    succeeds, count all scanned entries, and emit scan for excluded suffixes. File symlinks follow
    ordinary stat/open behavior without a separate containment check.

    For candidates, stat and hash the current path, guess MIME by filename, then insert an Asset
    followed by its Replica or update the rows associated with the existing key. This reuses an
    existing Asset identity when source bytes change. The legacy get_file_hash helper returns
    SHA-512 hex followed by decimal byte length; that value is currently written to columns named
    hash_sha256/observed_hash_sha256. Those column names do not make the value a SHA-256 digest.
    Stat and hashing do not form a pinned byte snapshot.

    Update counters according to Replica row mutation, so an Asset-only update is not counted as an
    updated file; timestamp refresh can count even unchanged bytes. Missing files are not deleted,
    and no cross-row transaction or manager refresh is performed here. Filesystem, database, hash,
    and callback errors propagate after earlier effects rather than being appended to report.errors.
    Finish time and done callback occur only after normal traversal.

    Example:
        >>> report = _index_existing_disk_frbr(  # doctest: +SKIP
        ...     db, disk_path=pathlib.Path("/books"), store_name="source",
        ...     ebook_extensions=("epub", "mobi"),
        ... )


    :param db: Borrowed FRBR database with Store, digital_assets, and asset_replicas tables.
    :param disk_path: Local directory root resolved before backend creation and sorted recursive scanning.
    :param store_name: Display name used for the source Store and report.
    :param ebook_extensions: Optional accepted suffix iterable; None selects the project defaults.
    :param progress_callback: Optional truthy callable receiving event, live mutable report, and detail mapping synchronously; its errors propagate.
    :return: Finished UnmanagedDiskRegistrationReport after normal traversal, with counters describing incremental row effects.
    """
    root = disk_path.resolve()
    store_row, backend = _ensure_or_create_unmanaged_store_row(db, root=root, store_name=store_name)
    store_id = int(store_row.row_id if store_row.row_id is not None else store_row["store_id"])
    report = UnmanagedDiskRegistrationReport(store_row_id=store_id, store_root_uri=str(backend.root_path), store_name=store_row["store_name"] if "store_name" in store_row.allowed_columns else backend.configuration.store_name)
    if progress_callback:
        progress_callback("start", report, {"mode": "local-frbr", "store_id": store_id, "store_root_uri": str(root)})
    ext_filter = _normalize_ebook_extensions(ebook_extensions)
    replica_rows = db.search("asset_replicas", "asset_replica_store_id", store_id) if "asset_replicas" in set(db.get_tables()) else []
    existing_by_key = {}
    for row in replica_rows:
        key = row["asset_replica_storage_key"]
        if key not in (None, ""):
            existing_by_key[str(key)] = row
    da_cols = _table_columns(db, "digital_assets")
    ar_cols = _table_columns(db, "asset_replicas")
    for path in sorted(p for p in root.rglob('*') if p.is_file()):
        report.scanned_files += 1
        ext = path.suffix.lower().lstrip('.')
        if ext not in ext_filter:
            report.skipped_non_ebook_files += 1
            if progress_callback:
                progress_callback("scan", report, {"path": str(path), "is_ebook": False})
            continue
        report.ebook_candidates += 1
        stat = path.stat()
        now = _now_ep_ms()
        rel = path.relative_to(root).as_posix()
        sha256 = get_file_hash(str(path))
        import mimetypes
        mime_type, _ = mimetypes.guess_type(path.name)
        replica = existing_by_key.get(rel)
        if replica is None:
            da_payload = {
                "digital_asset_name": path.name,
                "digital_asset_base_name": path.stem,
                "digital_asset_extension": ext,
                "digital_asset_mime_type": mime_type,
                "digital_asset_media_category": "ebook",
                "digital_asset_size_bytes": int(stat.st_size),
                "digital_asset_hash_sha256": sha256,
                "digital_asset_integrity_status": "ok",
                "digital_asset_last_seen_timestamp_ep_k": now,
                "digital_asset_last_integrity_check_timestamp_ep_k": now,
                "digital_asset_acquired_timestamp_ep_k": now,
                "digital_asset_source": "on_disk_unmanaged_import",
                "digital_asset_original_name": path.name,
                "digital_asset_original_path": str(path),
                "digital_asset_processed": 0,
                "digital_asset_source_created_datestamp_ep_k": int(getattr(stat, "st_ctime", 0) * 1000) if getattr(stat, "st_ctime", None) is not None else None,
                "digital_asset_source_modified_datestamp_ep_k": int(getattr(stat, "st_mtime", 0) * 1000) if getattr(stat, "st_mtime", None) is not None else None,
            }
            da = Row.from_idless_row_dict(db, row_dict={k: v for k, v in da_payload.items() if k in da_cols}, table="digital_assets")
            ar_payload = {
                "asset_replica_digital_asset_id": int(da["digital_asset_id"]),
                "asset_replica_store_id": store_id,
                "asset_replica_storage_key": rel,
                "asset_replica_mode": "active",
                "asset_replica_name": path.name,
                "asset_replica_base_name": path.stem,
                "asset_replica_extension": ext,
                "asset_replica_presence_status": "present",
                "asset_replica_integrity_status": "ok",
                "asset_replica_last_seen_timestamp_ep_k": now,
                "asset_replica_last_integrity_check_timestamp_ep_k": now,
                "asset_replica_observed_size_bytes": int(stat.st_size),
                "asset_replica_observed_hash_sha256": sha256,
                "asset_replica_source_created_datestamp_ep_k": int(getattr(stat, "st_ctime", 0) * 1000) if getattr(stat, "st_ctime", None) is not None else None,
                "asset_replica_source_modified_datestamp_ep_k": int(getattr(stat, "st_mtime", 0) * 1000) if getattr(stat, "st_mtime", None) is not None else None,
            }
            replica = Row.from_idless_row_dict(db, row_dict={k: v for k, v in ar_payload.items() if k in ar_cols}, table="asset_replicas")
            existing_by_key[rel] = replica
            report.inserted_files += 1
        else:
            da_id = replica["asset_replica_digital_asset_id"]
            da = db.get_row_from_id("digital_assets", int(da_id)) if da_id not in (None, "") else None
            da_changed = False
            if da is not None:
                for key, value in {
                    "digital_asset_size_bytes": int(stat.st_size),
                    "digital_asset_hash_sha256": sha256,
                    "digital_asset_integrity_status": "ok",
                    "digital_asset_last_seen_timestamp_ep_k": now,
                    "digital_asset_last_integrity_check_timestamp_ep_k": now,
                    "digital_asset_original_path": str(path),
                    "digital_asset_source_modified_datestamp_ep_k": int(getattr(stat, "st_mtime", 0) * 1000) if getattr(stat, "st_mtime", None) is not None else None,
                }.items():
                    if key in da.allowed_columns and da[key] != value:
                        da[key] = value
                        da_changed = True
                if da_changed:
                    da.sync()
            rep_changed = False
            for key, value in {
                "asset_replica_presence_status": "present",
                "asset_replica_integrity_status": "ok",
                "asset_replica_last_seen_timestamp_ep_k": now,
                "asset_replica_last_integrity_check_timestamp_ep_k": now,
                "asset_replica_observed_size_bytes": int(stat.st_size),
                "asset_replica_observed_hash_sha256": sha256,
                "asset_replica_source_modified_datestamp_ep_k": int(getattr(stat, "st_mtime", 0) * 1000) if getattr(stat, "st_mtime", None) is not None else None,
            }.items():
                if key in replica.allowed_columns and replica[key] != value:
                    replica[key] = value
                    rep_changed = True
            if rep_changed:
                replica.sync()
                report.updated_files += 1
            else:
                report.unchanged_files += 1
        if progress_callback:
            progress_callback("scan", report, {"path": str(path), "is_ebook": True})
    report.finished_timestamp_ep_k = _now_ep_ms()
    if progress_callback:
        progress_callback("done", report, {"mode": "local-frbr", "store_id": store_id, "store_root_uri": str(root)})
    return report


class ConsoleReporter:
    """
    Print flushed operator text and independently tracked indexing/pack progress lines.

    This reporter writes carriage-return updates without checking whether the stream is a terminal.
    Padding uses Python string lengths, not terminal display-cell widths. Index and pack widths are
    separate, and no lock coordinates concurrent or interleaved writers. Stream errors propagate;
    the stream remains caller-owned.

    Example:
        >>> import io
        >>> output = io.StringIO()
        >>> ConsoleReporter(stream=output).info("ready")
        >>> output.getvalue().splitlines()
        ['ready']
    """

    def __init__(self, *, stream=None) -> None:
        """
        Choose a truthy stream or current stdout and reset both progress-width counters.

        No stream conformance check or ownership transfer occurs; a falsey supplied stream selects
        stdout.

        Example:
            >>> import io
            >>> stream = io.StringIO()
            >>> ConsoleReporter(stream=stream).stream is stream
            True


        :param stream: Optional borrowed text stream; falsey input uses sys.stdout at construction.
        :return: None after retaining the stream and setting index/pack remembered lengths to zero.
        """
        self.stream = stream or sys.stdout
        self._last_index_line_len = 0
        self._last_pack_line_len = 0

    def line(self, text: str = "") -> None:
        """
        Print one newline-terminated value and flush the borrowed stream.

        Delegate conversion and writing to print. Progress-width state is unchanged, so this does
        not finish an active carriage-return line.

        Example:
            >>> import io
            >>> stream = io.StringIO()
            >>> ConsoleReporter(stream=stream).line("ready")
            >>> stream.getvalue().splitlines()
            ['ready']


        :param text: Value printed with a trailing newline; empty text emits a blank line.
        :return: None after print and flush succeed.
        """
        print(text, file=self.stream, flush=True)

    def section(self, title: str) -> None:
        """
        Print a blank line, title, and equal-sign borders sized by title string length.

        Each line delegates separately and flushes immediately. A later print failure can follow
        already visible earlier lines; progress counters are not reset.

        Example:
            >>> import io
            >>> stream = io.StringIO()
            >>> ConsoleReporter(stream=stream).section("Go")
            >>> stream.getvalue().splitlines()
            ['', '==', 'Go', '==']


        :param title: Title text whose len determines both border lengths.
        :return: None after the four line writes return.
        """
        self.line()
        self.line("=" * len(title))
        self.line(title)
        self.line("=" * len(title))

    def info(self, text: str) -> None:
        """
        Delegate an informational value to the newline-and-flush line method.

        Example:
            >>> reporter.info("Database opened")  # doctest: +SKIP


        :param text: Informational text passed unchanged to line.
        :return: None after the delegated line write returns.
        """
        self.line(text)

    def index_progress(self, *, label: str, scanned: int, total: int | None, ebooks: int, inserted: int, updated: int, skipped: int) -> None:
        """
        Overwrite the current indexing display with a bar and observed row counters.

        Prefix a carriage return, format scanned/total through the progress helper, and append raw
        ebook/insert/update/skip counts. Pad to at least the prior indexing line length, print
        without a newline, and flush before remembering the new padded length. This neither
        validates counter consistency nor changes pack-line state.

        Example:
            >>> reporter.index_progress(  # doctest: +SKIP
            ...     label="Index 1", scanned=3, total=10, ebooks=2,
            ...     inserted=1, updated=1, skipped=1,
            ... )


        :param label: Human-readable prefix for the current source scan.
        :param scanned: Observed scan count passed to progress formatting.
        :param total: Optional pre-counted file total; unknown/nonpositive selects pulse rendering.
        :param ebooks: Reported accepted ebook candidates displayed without coercion.
        :param inserted: Reported newly inserted file/Replica count.
        :param updated: Reported updated file/Replica count.
        :param skipped: Reported rejected-extension count.
        :return: None after writing/flushing and updating remembered indexing width.
        """
        msg = f"\r{label}: {_render_progress(scanned, total)}  ebooks={ebooks} inserted={inserted} updated={updated} skipped={skipped}"
        padded = msg.ljust(max(len(msg), self._last_index_line_len))
        print(padded, end="", file=self.stream, flush=True)
        self._last_index_line_len = len(padded)

    def finish_index_progress(self) -> None:
        """
        Terminate an active indexing line once and reset its remembered width.

        A zero remembered length is a no-op. Reset follows the flushed newline, so an output error
        leaves the previous remembered state.

        Example:
            >>> reporter.finish_index_progress()  # doctest: +SKIP


        :return: None after an optional newline and indexing-width reset.
        """
        if self._last_index_line_len:
            print(file=self.stream, flush=True)
            self._last_index_line_len = 0

    def pack_progress(self, *, label: str, staged: int, total: int, status: str) -> None:
        """
        Overwrite the pack display with staged-source progress and status text.

        Prefix a carriage return, pad to the prior pack-line length, and print/flush without a
        newline before updating remembered width. Index-line state is independent; status is
        displayed as supplied without lifecycle validation.

        Example:
            >>> reporter.pack_progress(  # doctest: +SKIP
            ...     label="pack-0001", staged=2, total=5, status="running",
            ... )


        :param label: Pack display label used before the progress bar.
        :param staged: Reported staged-source count passed to progress formatting.
        :param total: Declared source total; nonpositive values select unknown-total rendering.
        :param status: Lifecycle label displayed without validation.
        :return: None after writing/flushing and remembering the padded pack-line length.
        """
        msg = f"\r{label}: {_render_progress(staged, total)}  status={status}"
        padded = msg.ljust(max(len(msg), self._last_pack_line_len))
        print(padded, end="", file=self.stream, flush=True)
        self._last_pack_line_len = len(padded)

    def finish_pack_progress(self) -> None:
        """
        Terminate an active pack line once and clear its remembered width after flushing.

        An inactive line is a no-op. Printing errors propagate before the counter is reset.

        Example:
            >>> reporter.finish_pack_progress()  # doctest: +SKIP


        :return: None after an optional newline and pack-width reset.
        """
        if self._last_pack_line_len:
            print(file=self.stream, flush=True)
            self._last_pack_line_len = 0


@dataclasses.dataclass(slots=True, frozen=True)
class IndexedStoreRun:
    """
    Retain the operator summary of indexing one input directory.

    This frozen record performs no validation, catalogue lookup, or counter reconciliation. Counts
    describe reported row operations, which can include timestamp refreshes rather than byte
    changes. Errors and skipped/link counts from the richer registration report are not fields of
    this summary.

    Example:
        >>> summary = IndexedStoreRun("/books", 1, "source", "/books", 4, 3, 2, 1, 0)
        >>> summary.ebook_candidates
        3


    :ivar input_path: Recorded input directory spelling.
    :ivar store_id: Numeric database Store row identity reported by indexing.
    :ivar store_name: Reported source Store display name.
    :ivar store_root_uri: Reported source root path/URI spelling.
    :ivar scanned_files: Count of observed file entries including excluded suffixes.
    :ivar ebook_candidates: Count accepted by the indexing extension policy.
    :ivar inserted_files: Reported successful new file/Replica insertions.
    :ivar updated_files: Reported changed existing file/Replica rows.
    :ivar unchanged_files: Reported existing rows requiring no counted update.
    """

    input_path: str
    store_id: int
    store_name: str
    store_root_uri: str
    scanned_files: int
    ebook_candidates: int
    inserted_files: int
    updated_files: int
    unchanged_files: int


@dataclasses.dataclass(slots=True, frozen=True)
class PackExecutionRun:
    """
    Retain the reported outcome of one built and registered backup pack.

    The frozen value does not inspect output bytes, validate IDs/counts, or confirm Store
    availability. It records source-size estimates rather than a measured compressed-image size.

    Example:
        >>> summary = PackExecutionRun(3, "pack", "/backups/pack.sqsh", 2, 100, UUID(int=1), 2)
        >>> summary.source_count
        2


    :ivar workflow_id: Repository ID assigned to the executed workflow.
    :ivar workflow_name: Planned pack name used for operator output.
    :ivar output_url: Local image pathname recorded as text, despite the URL field name.
    :ivar source_count: Declared pack source count.
    :ivar estimated_size_bytes: Planner estimate of source payload bytes.
    :ivar backup_store_ref: Stable UUID of the registered archive Store.
    :ivar presence_links_created: Registry-reported count of member-presence insertions.
    """

    workflow_id: int
    workflow_name: str
    output_url: str
    source_count: int
    estimated_size_bytes: int
    backup_store_ref: UUID
    presence_links_created: int


@dataclasses.dataclass(slots=True, frozen=True)
class PrototypeRunResult:
    """
    Collect successful indexing and pack summaries after a normal prototype run.

    Frozen fields prevent reassignment without deeply freezing caller-supplied collections or
    validating their contents. A failed run raises instead of returning this aggregate, even when
    earlier indexing or packs already produced persistent effects.

    Example:
        >>> PrototypeRunResult("catalogue.sqlite", (), ()).total_executed_packs
        0


    :ivar database_path: Recorded catalogue pathname.
    :ivar indexed_stores: Ordered source-indexing summaries accumulated during the run.
    :ivar executed_packs: Ordered pack summaries appended after registration succeeds.
    """

    database_path: str
    indexed_stores: tuple[IndexedStoreRun, ...]
    executed_packs: tuple[PackExecutionRun, ...]

    @property
    def total_indexed_stores(self) -> int:
        """
        Count retained indexing summaries without deduplicating Store identity or querying the
        catalogue.

        Example:
            >>> PrototypeRunResult("catalogue.sqlite", (), ()).total_indexed_stores
            0


        :return: Current len(indexed_stores).
        """
        return len(self.indexed_stores)

    @property
    def total_executed_packs(self) -> int:
        """
        Count retained pack summaries without inspecting image existence, uniqueness, or current
        readability.

        Example:
            >>> PrototypeRunResult("catalogue.sqlite", (), ()).total_executed_packs
            0


        :return: Current len(executed_packs).
        """
        return len(self.executed_packs)


class ExistingDriveSquashfsPrototype:
    """
    Coordinate local indexing, pack planning, checkpoint writes, and artifact Store registration.

    This synchronous operator prototype opens its own Library for each run and borrows a reporter
    and optional workflow factory. It chooses legacy-file or direct-FRBR indexing from the current
    schema, persists each workflow's effective initial declaration, and saves checkpoints after each
    returned unit. It does not resume existing workflow IDs or wrap the whole run in a transaction.

    Failures can leave indexed rows, workflow evidence, staged bytes, completed images, and earlier
    registrations. Successful registration concerns the archive Store and presence links; the
    prototype does not add whole-image Asset derivation provenance. Output/staging paths are not
    excluded automatically from input scans when directories overlap.

    Example:
        >>> prototype = ExistingDriveSquashfsPrototype(  # doctest: +SKIP
        ...     database_path="catalogue.sqlite", output_dir="packs",
        ...     target_pack_size_bytes=1024**3,
        ... )
        >>> result = prototype.run(["/media/books"])  # doctest: +SKIP
    """

    def __init__(self, *, database_path: str | pathlib.Path, output_dir: str | pathlib.Path, target_pack_size_bytes: int, max_files_per_pack: int | None = None, ebook_extensions: Iterable[str] | None = None, verify_after_build: bool = True, cleanup_staging_after_success: bool = False, staging_root: str | pathlib.Path | None = None, reporter: ConsoleReporter | None = None, workflow_factory: Callable[[BackupWorkflowDeclaration], Any] | None = None) -> None:
        """
        Normalize prototype settings and create the output directory immediately.

        Expand user syntax for paths without making them absolute. Output mkdir precedes numeric
        conversion, extension collection, and later run validation, so a rejected setting can leave
        that directory. Counts use int conversion; positivity is deferred to the planner.
        Collect/sort extension tokens, convert flags to bool, and choose a truthy reporter or a new
        stdout reporter. No catalogue is opened or workflow executed here.

        Example:
            >>> prototype = ExistingDriveSquashfsPrototype(  # doctest: +SKIP
            ...     database_path="catalogue.sqlite", output_dir="packs",
            ...     target_pack_size_bytes=100_000_000, max_files_per_pack=500,
            ...     ebook_extensions=("epub", "mobi"),
            ... )


        :param database_path: Catalogue path expanded at construction and opened later by Library.
        :param output_dir: Directory expanded and created immediately for published pack images.
        :param target_pack_size_bytes: Int-convertible planner target for summed source bytes; positivity checked during planning.
        :param max_files_per_pack: Optional int-convertible source-count target, passed to the planner.
        :param ebook_extensions: Optional extension iterable; None selects project defaults and an empty iterable remains empty.
        :param verify_after_build: Flag converted to bool and applied to effective pack declarations.
        :param cleanup_staging_after_success: Flag requesting workflow staging cleanup after successful publication.
        :param staging_root: Optional expanded staging base; None uses output_dir/.liuxin-staging.
        :param reporter: Optional borrowed ConsoleReporter-like object; falsey input selects a new stdout reporter.
        :param workflow_factory: Optional callable receiving the adjusted declaration; otherwise construct SquashfsBackupWorkflow with the run's manager.
        :return: None after output-directory creation and retained setting initialization.
        """
        self.database_path = pathlib.Path(database_path).expanduser()
        self.output_dir = pathlib.Path(output_dir).expanduser()
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.target_pack_size_bytes = int(target_pack_size_bytes)
        self.max_files_per_pack = None if max_files_per_pack is None else int(max_files_per_pack)
        self.ebook_extensions = tuple(sorted(_normalize_ebook_extensions(ebook_extensions)))
        self.verify_after_build = bool(verify_after_build)
        self.cleanup_staging_after_success = bool(cleanup_staging_after_success)
        self.staging_root = None if staging_root is None else pathlib.Path(staging_root).expanduser()
        self.reporter = reporter or ConsoleReporter()
        self.workflow_factory = workflow_factory

    def run(self, input_paths: Sequence[str | pathlib.Path]) -> PrototypeRunResult:
        """
        Index each input and synchronously build/register its proposed packs with saved checkpoints.

        Expand/resolve all input paths and require at least one existing directory before opening
        Library. Create the catalogue only when its path is absent; ensure an output Store row,
        refresh manager Stores, then process inputs in supplied order. Pre-count files for display
        and derive an ordinal-based Store name. A files table selects the existing legacy indexer;
        otherwise use the direct-FRBR fallback. Reports containing legacy-indexer errors are
        displayed but do not automatically stop subsequent planning.

        Resolve the indexed Store UUID and plan using configured size/count/extension limits. Add
        staging and verification settings to each declaration, construct a workflow, and persist its
        effective progress().declaration with DRAFT status before saving the initial checkpoint.
        Repeatedly call run_next, save returned state, and report progress until terminal. A
        non-COMPLETE state raises after its checkpoint has been saved. There is no loop time limit
        or recovery of prior workflow IDs.

        Request the terminal result, require an output reference, derive its local display/name
        path, and register the artifact with source links. Append a pack summary only after
        registration and reporting return. Library context cleanup runs on exit; later failures can
        follow completed writes, images, or earlier packs without returning a partial aggregate. No
        cross-step transaction or rollback is provided.

        Example:
            >>> result = prototype.run(["/media/books-a", "/media/books-b"])  # doctest: +SKIP
            >>> result.total_indexed_stores  # doctest: +SKIP
            2


        :param input_paths: Ordered nonempty sequence of local directory paths; duplicates are processed again rather than deduplicated.
        :return: PrototypeRunResult after all inputs/packs and final reporting succeed; earlier effects may remain when any step raises.
        """
        paths = [pathlib.Path(p).expanduser().resolve() for p in input_paths]
        if not paths:
            raise ValueError("Provide at least one input path.")
        for path in paths:
            if not path.exists():
                raise FileNotFoundError(str(path))
            if not path.is_dir():
                raise NotADirectoryError(str(path))
        self.reporter.section("Opening library database")
        create_db = not self.database_path.exists()
        self.reporter.info(f"Database: {self.database_path}")
        self.reporter.info(f"Create new DB: {'yes' if create_db else 'no'}")
        self.reporter.info(f"Output dir: {self.output_dir}")
        self.reporter.info(f"Target pack size: {_format_bytes(self.target_pack_size_bytes)}")
        if self.max_files_per_pack is not None:
            self.reporter.info(f"Max files per pack: {self.max_files_per_pack}")
        indexed_runs: list[IndexedStoreRun] = []
        executed_runs: list[PackExecutionRun] = []
        from LiuXin_alpha.library import Library
        with Library(database_path=self.database_path, create=create_db, backup=False, storage_startup_on_add=False) as lib:
            destination_store_ref = self._ensure_output_store_row(lib.db)
            lib.refresh_storage(clear_existing=True, startup_on_add=True)
            repo = BackupWorkflowRepository(lib.db)
            registry = BackupArtifactRegistry(lib.db, storage_manager=lib.storage)
            for ordinal, input_path in enumerate(paths, start=1):
                store_name = self._derive_store_name(input_path, ordinal)
                total_files = self._count_all_files(input_path)
                label = f"Index {ordinal}/{len(paths)} [{store_name}]"
                self.reporter.section(f"Indexing {input_path}")
                self.reporter.info(f"Store name: {store_name}")
                self.reporter.info(f"All files observed before index pass: {total_files}")
                def _progress(event: str, report, details: dict[str, object]) -> None:
                    """
                    Translate one indexer event into the current input's operator progress display.

                    The closure reads the current label, pre-counted total, and reporter. start/scan
                    render int-converted report counters; error finishes the line and prints
                    supplied error/path details; done renders once more and finishes. Unknown events
                    are ignored. No report mutation or exception guard is added here; the calling
                    indexer's callback policy determines whether errors escape.

                    Example:
                        During run(), the chosen indexer invokes this callback with start, scan, error,
                        or done and the live registration report for the current input directory.


                    :param event: Indexer event label controlling display or no-op behavior.
                    :param report: Live report supplying scanned/candidate/insert/update/skip counters.
                    :param details: Event metadata; error display reads its optional error and path entries.
                    :return: None after the selected reporter calls or an unrecognized-event no-op.
                    """
                    if event in {"scan", "start"}:
                        self.reporter.index_progress(label=label, scanned=int(report.scanned_files), total=total_files, ebooks=int(report.ebook_candidates), inserted=int(report.inserted_files), updated=int(report.updated_files), skipped=int(report.skipped_non_ebook_files))
                    elif event == "error":
                        self.reporter.finish_index_progress()
                        self.reporter.info(f"  ! indexing error: {details.get('error')} @ {details.get('path')}")
                    elif event == "done":
                        self.reporter.index_progress(label=label, scanned=int(report.scanned_files), total=total_files, ebooks=int(report.ebook_candidates), inserted=int(report.inserted_files), updated=int(report.updated_files), skipped=int(report.skipped_non_ebook_files))
                        self.reporter.finish_index_progress()
                tables_now = set(lib.db.get_tables())
                if "files" in tables_now:
                    report = register_existing_disk_as_unmanaged_store(lib.db, disk_path=input_path, store_name=store_name, ebook_extensions=self.ebook_extensions, progress_callback=_progress)
                else:
                    report = _index_existing_disk_frbr(lib.db, disk_path=input_path, store_name=store_name, ebook_extensions=self.ebook_extensions, progress_callback=_progress)
                self.reporter.info("Indexed store {} (id={}): ebooks={} inserted={} updated={} unchanged={} errors={}".format(report.store_name, report.store_row_id, report.ebook_candidates, report.inserted_files, report.updated_files, report.unchanged_files, len(report.errors)))
                indexed_runs.append(IndexedStoreRun(input_path=str(input_path), store_id=int(report.store_row_id), store_name=str(report.store_name), store_root_uri=str(report.store_root_uri), scanned_files=int(report.scanned_files), ebook_candidates=int(report.ebook_candidates), inserted_files=int(report.inserted_files), updated_files=int(report.updated_files), unchanged_files=int(report.unchanged_files)))
                source_row = lib.db.get_row_from_id("stores", int(report.store_row_id))
                if source_row is None or source_row["store_uuid"] in (None, ""):
                    raise RuntimeError("indexed source Store has no stable UUID")
                source_store_ref = UUID(str(source_row["store_uuid"]))
                planner = StoreBackupPlanner(lib.storage)
                planned = planner.plan_store_backup(
                    source_store_ref=source_store_ref,
                    destination_store_ref=destination_store_ref,
                    target_artifact_size_bytes=int(self.target_pack_size_bytes),
                    workflow_name_prefix=store_name,
                    output_key_prefix="",
                    max_sources_per_artifact=self.max_files_per_pack,
                    allowed_extensions=self.ebook_extensions,
                )
                self.reporter.info(f"Planned {len(planned)} pack(s) for {store_name}.")
                for pack in planned:
                    declaration = pack.workflow_declaration
                    staging_base = self.staging_root or (self.output_dir / ".liuxin-staging")
                    declaration = dataclasses.replace(
                        declaration,
                        verify_after_build=self.verify_after_build,
                        cleanup_staging_after_success=self.cleanup_staging_after_success,
                        staging_target=str(staging_base / declaration.workflow_name),
                    )
                    self.reporter.section(f"Building {declaration.workflow_name}")
                    self.reporter.info(f"Sources: {pack.source_count}  Estimated payload: {_format_bytes(pack.estimated_size_bytes)}")
                    workflow = (
                        self.workflow_factory(declaration)
                        if self.workflow_factory is not None
                        else SquashfsBackupWorkflow.from_declaration(
                            declaration,
                            storage_manager=lib.storage,
                        )
                    )
                    state = workflow.progress()
                    # Persist the workflow's effective declaration.  Concrete
                    # implementations may make implicit execution settings
                    # explicit (for SquashFS: compression, executable,
                    # deterministic mode, and the stable builder Store UUID).
                    # Saving the planner's pre-construction declaration makes
                    # the very first checkpoint look like changed intent.
                    workflow_id = repo.save_workflow_declaration(
                        state.declaration,
                        status=WorkflowStatus.DRAFT,
                    )
                    repo.save_checkpoint(workflow_id, state)
                    total_sources = len(state.declaration.sources)
                    while state.status not in {WorkflowStatus.COMPLETE, WorkflowStatus.FAILED, WorkflowStatus.CANCELLED}:
                        state = workflow.run_next()
                        repo.save_checkpoint(workflow_id, state)
                        self.reporter.pack_progress(label=declaration.workflow_name, staged=int(state.staged_source_count), total=total_sources, status=state.status.value)
                    self.reporter.finish_pack_progress()
                    if state.status is not WorkflowStatus.COMPLETE:
                        raise RuntimeError("Workflow {!r} failed with status {!r}: {}".format(declaration.workflow_name, state.status.value, state.last_error or "unknown error"))
                    result = workflow.run_to_completion()
                    output_reference = result.output_artifact_reference
                    if output_reference is None:
                        raise RuntimeError("completed workflow did not identify its output")
                    output_path = self._output_path(output_reference)
                    registered = registry.register_artifact(
                        workflow_id,
                        result,
                        store_name=output_path.stem,
                        link_sources=True,
                    )
                    self.reporter.info("Built {} -> {} (backup store ref={}, presence links={})".format(declaration.workflow_name, output_path, registered.backup_store_ref, registered.presence_links_created))
                    executed_runs.append(PackExecutionRun(workflow_id=workflow_id, workflow_name=declaration.workflow_name, output_url=str(output_path), source_count=int(pack.source_count), estimated_size_bytes=int(pack.estimated_size_bytes), backup_store_ref=registered.backup_store_ref, presence_links_created=int(registered.presence_links_created)))
        self.reporter.section("Run complete")
        self.reporter.info(f"Indexed stores: {len(indexed_runs)}")
        self.reporter.info(f"Built packs: {len(executed_runs)}")
        return PrototypeRunResult(database_path=str(self.database_path), indexed_stores=tuple(indexed_runs), executed_packs=tuple(executed_runs))

    def _ensure_output_store_row(self, db) -> UUID:
        """
        Reuse the first Store row for the resolved output file URI or insert filesystem
        configuration.

        A matching row is returned through its UUID without replacing its name, kind, or
        capabilities; only a missing UUID is generated and synced. New configuration is filtered to
        reported Store columns. This helper does not search historical raw-path spellings, check
        manager attachment, or open an enclosing transaction.

        Example:
            >>> store_ref = prototype._ensure_output_store_row(db)  # doctest: +SKIP


        :param db: Borrowed database receiving output Store lookup or insertion.
        :return: UUID of the reused/new output Store row; malformed persisted UUID or adapter errors propagate.
        """
        root_uri = self.output_dir.resolve().as_uri()
        existing = db.search("stores", "store_root_uri", root_uri)
        if existing:
            row = existing[0]
            if row["store_uuid"] in (None, ""):
                row["store_uuid"] = str(uuid4())
                row.sync()
            return UUID(str(row["store_uuid"]))
        store_ref = uuid4()
        allowed = set(db.get_column_headings("stores"))
        values = {
            "store_uuid": str(store_ref),
            "store_name": "backup-pack-output",
            "store_kind": "filesystem",
            "store_access_protocol": "file",
            "store_root_uri": root_uri,
            "store_operational_role": "backup",
            "store_is_read_only": 0,
            "store_online_status": "online",
            "store_supports_random_read": 1,
            "store_supports_random_write": 1,
            "store_supports_delete": 1,
            "store_supports_folders": 1,
        }
        Row.from_idless_row_dict(
            db,
            row_dict={key: value for key, value in values.items() if key in allowed},
            table="stores",
        )
        return store_ref

    def _output_path(self, reference) -> pathlib.Path:
        """
        Project a result reference into the local pathname used for display and Store naming.

        For a Location, join its POSIX key beneath output_dir and resolve, ignoring its Store UUID.
        Other values become expanded/resolved Paths without file-URI parsing. There is no existence
        or post-resolution containment check here; actual registry resolution remains a separate
        later operation.

        Example:
            >>> path = prototype._output_path(result.output_artifact_reference)  # doctest: +SKIP


        :param reference: Workflow output Location or local path-like reference to project.
        :return: Resolved local Path derived from the output directory/key or supplied local spelling.
        """
        from LiuXin_alpha.storage.api import Location

        if isinstance(reference, Location):
            return self.output_dir.joinpath(
                *pathlib.PurePosixPath(reference.key).parts
            ).resolve()
        return pathlib.Path(reference).expanduser().resolve()

    @staticmethod
    def _derive_store_name(path: pathlib.Path, ordinal: int) -> str:
        """
        Combine an input ordinal with a sanitized basename token and stable helper hash.

        Prefer path.name, then drive, then a slash-replaced path spelling. safe_path_to_name
        supplies its default sanitization/hash; strip surrounding underscores and use
        input_<ordinal> only if the resulting token is empty. No root lookup or uniqueness check
        occurs, and ordinal formatting does not validate positivity.

        Example:
            >>> ExistingDriveSquashfsPrototype._derive_store_name(pathlib.Path("/books"), 2).startswith("existing_disk_002_books-")
            True


        :param path: Resolved input path whose basename supplies the human-readable token.
        :param ordinal: Input position formatted with at least three decimal digits.
        :return: existing_disk_<ordinal>_<sanitized-token> display name.
        """
        base = path.name or path.drive or path.as_posix().replace("/", "_")
        token = safe_path_to_name(base).strip("_") or f"input_{ordinal:03d}"
        return f"existing_disk_{ordinal:03d}_{token}"

    @staticmethod
    def _count_all_files(root: pathlib.Path) -> int:
        """
        Count recursive entries whose current is_file check succeeds for progress estimation.

        No suffix filtering, path deduplication, output/staging exclusion, or file-version snapshot
        is added. File symlinks follow ordinary pathlib checks; enumeration failures propagate and
        the later indexing count can differ as the tree changes.

        Example:
            >>> total = ExistingDriveSquashfsPrototype._count_all_files(pathlib.Path("/books"))  # doctest: +SKIP


        :param root: Directory Path traversed recursively with rglob.
        :return: Count of file entries observed during this traversal.
        """
        return sum(1 for path in root.rglob('*') if path.is_file())


__all__ = ["ConsoleReporter", "ExistingDriveSquashfsPrototype", "IndexedStoreRun", "PackExecutionRun", "PrototypeRunResult"]
