"""Chatty prototype pipeline for indexing existing drives and building SquashFS packs."""

from __future__ import annotations

import dataclasses
import pathlib
import sys
import time
from collections.abc import Callable, Iterable, Sequence
from typing import Any, TYPE_CHECKING

from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.storage.api.backup_api import BackupWorkflowSpec, BackupWorkflowStatus
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
    return int(time.time() * 1000)


def _format_bytes(value: int) -> str:
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
    if ebook_extensions is None:
        from LiuXin_alpha.constants.file_extensions import BOOK_EXTENSIONS
        ebook_extensions = BOOK_EXTENSIONS
    return {str(x).lower().lstrip('.') for x in ebook_extensions if str(x).strip()}


def _table_columns(db, table_name: str) -> set[str]:
    return set(db.get_column_headings(table_name))


def _ensure_or_create_unmanaged_store_row(db, *, root: pathlib.Path, store_name: str, store_kind: str = "on_disk_existing_unmanaged_drive"):
    backend = OnDiskUnmanagedStorageBackend(url=str(root), name=store_name)
    rows = db.search("stores", "store_root_uri", str(root))
    payload = {
        "store_name": backend.name,
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
    root = disk_path.resolve()
    store_row, backend = _ensure_or_create_unmanaged_store_row(db, root=root, store_name=store_name)
    store_id = int(store_row.row_id if store_row.row_id is not None else store_row["store_id"])
    report = UnmanagedDiskRegistrationReport(store_row_id=store_id, store_root_uri=str(backend.root_path), store_name=store_row["store_name"] if "store_name" in store_row.allowed_columns else backend.name)
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
    def __init__(self, *, stream=None) -> None:
        self.stream = stream or sys.stdout
        self._last_index_line_len = 0
        self._last_pack_line_len = 0

    def line(self, text: str = "") -> None:
        print(text, file=self.stream, flush=True)

    def section(self, title: str) -> None:
        self.line()
        self.line("=" * len(title))
        self.line(title)
        self.line("=" * len(title))

    def info(self, text: str) -> None:
        self.line(text)

    def index_progress(self, *, label: str, scanned: int, total: int | None, ebooks: int, inserted: int, updated: int, skipped: int) -> None:
        msg = f"\r{label}: {_render_progress(scanned, total)}  ebooks={ebooks} inserted={inserted} updated={updated} skipped={skipped}"
        padded = msg.ljust(max(len(msg), self._last_index_line_len))
        print(padded, end="", file=self.stream, flush=True)
        self._last_index_line_len = len(padded)

    def finish_index_progress(self) -> None:
        if self._last_index_line_len:
            print(file=self.stream, flush=True)
            self._last_index_line_len = 0

    def pack_progress(self, *, label: str, staged: int, total: int, status: str) -> None:
        msg = f"\r{label}: {_render_progress(staged, total)}  status={status}"
        padded = msg.ljust(max(len(msg), self._last_pack_line_len))
        print(padded, end="", file=self.stream, flush=True)
        self._last_pack_line_len = len(padded)

    def finish_pack_progress(self) -> None:
        if self._last_pack_line_len:
            print(file=self.stream, flush=True)
            self._last_pack_line_len = 0


@dataclasses.dataclass(slots=True, frozen=True)
class IndexedStoreRun:
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
    workflow_id: int
    workflow_name: str
    output_url: str
    source_count: int
    estimated_size_bytes: int
    backup_store_id: int
    presence_links_created: int


@dataclasses.dataclass(slots=True, frozen=True)
class PrototypeRunResult:
    database_path: str
    indexed_stores: tuple[IndexedStoreRun, ...]
    executed_packs: tuple[PackExecutionRun, ...]

    @property
    def total_indexed_stores(self) -> int:
        return len(self.indexed_stores)

    @property
    def total_executed_packs(self) -> int:
        return len(self.executed_packs)


class ExistingDriveSquashfsPrototype:
    def __init__(self, *, database_path: str | pathlib.Path, output_dir: str | pathlib.Path, target_pack_size_bytes: int, max_files_per_pack: int | None = None, ebook_extensions: Iterable[str] | None = None, verify_after_build: bool = True, cleanup_staging_after_success: bool = False, staging_root: str | pathlib.Path | None = None, reporter: ConsoleReporter | None = None, workflow_factory: Callable[[BackupWorkflowSpec], Any] | None = None) -> None:
        self.database_path = pathlib.Path(database_path).expanduser()
        self.output_dir = pathlib.Path(output_dir).expanduser()
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.target_pack_size_bytes = int(target_pack_size_bytes)
        self.max_files_per_pack = None if max_files_per_pack is None else int(max_files_per_pack)
        self.ebook_extensions = None if ebook_extensions is None else tuple(str(x) for x in ebook_extensions)
        self.verify_after_build = bool(verify_after_build)
        self.cleanup_staging_after_success = bool(cleanup_staging_after_success)
        self.staging_root = None if staging_root is None else pathlib.Path(staging_root).expanduser()
        self.reporter = reporter or ConsoleReporter()
        self.workflow_factory = workflow_factory or self._build_workflow_from_spec

    def run(self, input_paths: Sequence[str | pathlib.Path]) -> PrototypeRunResult:
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
            planner = StoreBackupPlanner(lib.db)
            repo = BackupWorkflowRepository(lib.db)
            registry = BackupArtifactRegistry(lib.db)
            for ordinal, input_path in enumerate(paths, start=1):
                store_name = self._derive_store_name(input_path, ordinal)
                total_files = self._count_all_files(input_path)
                label = f"Index {ordinal}/{len(paths)} [{store_name}]"
                self.reporter.section(f"Indexing {input_path}")
                self.reporter.info(f"Store name: {store_name}")
                self.reporter.info(f"All files observed before index pass: {total_files}")
                def _progress(event: str, report, details: dict[str, object]) -> None:
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
                planned = planner.plan_squashfs_packs_for_store(source_store_id=int(report.store_row_id), output_dir=str(self.output_dir), target_pack_size_bytes=int(self.target_pack_size_bytes), workflow_name_prefix=store_name, max_files_per_pack=self.max_files_per_pack, allowed_extensions=self.ebook_extensions)
                self.reporter.info(f"Planned {len(planned)} pack(s) for {store_name}.")
                for pack in planned:
                    spec = dataclasses.replace(pack.workflow_spec, verify_after_build=self.verify_after_build, cleanup_staging_after_success=self.cleanup_staging_after_success, staging_root=(None if self.staging_root is None else str(self.staging_root / pack.workflow_spec.workflow_name)))
                    self.reporter.section(f"Building {spec.workflow_name}")
                    self.reporter.info(f"Sources: {pack.source_count}  Estimated payload: {_format_bytes(pack.estimated_size_bytes)}")
                    workflow_row = repo.save_workflow_spec(spec, status=BackupWorkflowStatus.DRAFT)
                    workflow_id = int(workflow_row.backup_workflow_id)
                    workflow = self.workflow_factory(spec)
                    state = workflow.progress()
                    repo.save_resume_state(workflow_id, state)
                    total_sources = len(spec.sources)
                    while state.status not in {BackupWorkflowStatus.COMPLETE, BackupWorkflowStatus.FAILED, BackupWorkflowStatus.CANCELLED}:
                        state = workflow.run_next()
                        repo.save_resume_state(workflow_id, state)
                        self.reporter.pack_progress(label=spec.workflow_name, staged=int(state.staged_source_count), total=total_sources, status=state.status.value)
                    self.reporter.finish_pack_progress()
                    if state.status is not BackupWorkflowStatus.COMPLETE:
                        raise RuntimeError("Workflow {!r} failed with status {!r}: {}".format(spec.workflow_name, state.status.value, state.last_error or "unknown error"))
                    registered = registry.register_workflow_output_as_store(workflow_id, artifact_url=state.output_artifact_url, store_name=pathlib.Path(spec.output_url).stem, link_sources=True)
                    self.reporter.info("Built {} -> {} (backup store id={}, presence links={})".format(spec.workflow_name, state.output_artifact_url, registered.backup_store_id, registered.presence_links_created))
                    executed_runs.append(PackExecutionRun(workflow_id=workflow_id, workflow_name=spec.workflow_name, output_url=str(state.output_artifact_url), source_count=int(pack.source_count), estimated_size_bytes=int(pack.estimated_size_bytes), backup_store_id=int(registered.backup_store_id), presence_links_created=int(registered.presence_links_created)))
        self.reporter.section("Run complete")
        self.reporter.info(f"Indexed stores: {len(indexed_runs)}")
        self.reporter.info(f"Built packs: {len(executed_runs)}")
        return PrototypeRunResult(database_path=str(self.database_path), indexed_stores=tuple(indexed_runs), executed_packs=tuple(executed_runs))

    def _build_workflow_from_spec(self, spec: BackupWorkflowSpec):
        return SquashfsBackupWorkflow.from_spec(spec)

    @staticmethod
    def _derive_store_name(path: pathlib.Path, ordinal: int) -> str:
        base = path.name or path.drive or path.as_posix().replace("/", "_")
        token = safe_path_to_name(base).strip("_") or f"input_{ordinal:03d}"
        return f"existing_disk_{ordinal:03d}_{token}"

    @staticmethod
    def _count_all_files(root: pathlib.Path) -> int:
        return sum(1 for path in root.rglob('*') if path.is_file())


__all__ = ["ConsoleReporter", "ExistingDriveSquashfsPrototype", "IndexedStoreRun", "PackExecutionRun", "PrototypeRunResult"]
