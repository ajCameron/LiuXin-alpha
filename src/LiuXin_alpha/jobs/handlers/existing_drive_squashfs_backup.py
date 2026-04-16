"""Job handler for the existing-drives-to-SquashFS prototype pipeline."""

from __future__ import annotations

import dataclasses
import json
import pathlib

from typing import Any, Iterable

from LiuXin_alpha.jobs.handler_api import JobHandlerAPI, JobRunContext
from LiuXin_alpha.jobs.models import JobProgressUpdate
from LiuXin_alpha.storage.backup.prototype_pipeline import ConsoleReporter, ExistingDriveSquashfsPrototype


@dataclasses.dataclass(slots=True, frozen=True)
class ExistingDriveSquashfsBackupJobPayload:
    input_paths: tuple[str, ...]
    database_path: str
    output_dir: str
    target_pack_size_bytes: int
    max_files_per_pack: int | None = None
    ebook_extensions: tuple[str, ...] | None = None
    verify_after_build: bool = True
    cleanup_staging_after_success: bool = False
    staging_root: str | None = None
    delete_originals: bool = False

    @classmethod
    def from_json(cls, payload_json: str) -> "ExistingDriveSquashfsBackupJobPayload":
        data = json.loads(payload_json)
        if not isinstance(data, dict):
            raise TypeError("Job payload must decode to a JSON object")
        input_paths = tuple(str(x) for x in data.get("input_paths", ()) if str(x).strip())
        if not input_paths:
            raise ValueError("input_paths is required")
        database_path = str(data.get("database_path", "")).strip()
        output_dir = str(data.get("output_dir", "")).strip()
        if not database_path or not output_dir:
            raise ValueError("database_path and output_dir are required")
        return cls(
            input_paths=input_paths,
            database_path=database_path,
            output_dir=output_dir,
            target_pack_size_bytes=int(data.get("target_pack_size_bytes") or 0),
            max_files_per_pack=(int(data["max_files_per_pack"]) if data.get("max_files_per_pack") is not None else None),
            ebook_extensions=(tuple(str(x) for x in data.get("ebook_extensions", ())) if data.get("ebook_extensions") is not None else None),
            verify_after_build=bool(data.get("verify_after_build", True)),
            cleanup_staging_after_success=bool(data.get("cleanup_staging_after_success", False)),
            staging_root=(str(data["staging_root"]).strip() if data.get("staging_root") else None),
            delete_originals=bool(data.get("delete_originals", False)),
        )

    def to_json(self) -> str:
        return json.dumps(dataclasses.asdict(self), sort_keys=True)


class _JobReporter(ConsoleReporter):
    def __init__(self, run_context: JobRunContext) -> None:
        super().__init__()
        self.run_context = run_context

    def line(self, text: str = "") -> None:
        self.run_context.log(text)

    def index_progress(self, *, label: str, scanned: int, total: int | None, ebooks: int, inserted: int, updated: int, skipped: int) -> None:
        super().index_progress(label=label, scanned=scanned, total=total, ebooks=ebooks, inserted=inserted, updated=updated, skipped=skipped)
        self.run_context.update_progress(
            JobProgressUpdate(
                progress_current=scanned,
                progress_total=total,
                progress_unit="files",
                progress_message=f"{label}: scanned={scanned} ebooks={ebooks} inserted={inserted} updated={updated} skipped={skipped}",
            )
        )

    def pack_progress(self, *, label: str, staged: int, total: int, status: str) -> None:
        super().pack_progress(label=label, staged=staged, total=total, status=status)
        self.run_context.update_progress(
            JobProgressUpdate(
                progress_current=staged,
                progress_total=total,
                progress_unit="sources",
                progress_message=f"{label}: {status}",
            )
        )
        self.run_context.heartbeat(f"{label}: {status}")


class ExistingDriveSquashfsBackupJobHandler(JobHandlerAPI):
    job_kind = "existing_drives_to_squashfs_backup"

    def validate_payload(self, payload_json: str) -> None:
        payload = ExistingDriveSquashfsBackupJobPayload.from_json(payload_json)
        if payload.target_pack_size_bytes <= 0:
            raise ValueError("target_pack_size_bytes must be > 0")
        if payload.delete_originals:
            raise ValueError("delete_originals remains gated off for now")

    def run(self, *, payload_json: str, run_context: JobRunContext) -> dict[str, Any]:
        payload = ExistingDriveSquashfsBackupJobPayload.from_json(payload_json)
        reporter = _JobReporter(run_context)
        prototype = ExistingDriveSquashfsPrototype(
            database_path=pathlib.Path(payload.database_path),
            output_dir=pathlib.Path(payload.output_dir),
            target_pack_size_bytes=int(payload.target_pack_size_bytes),
            max_files_per_pack=payload.max_files_per_pack,
            ebook_extensions=payload.ebook_extensions,
            verify_after_build=payload.verify_after_build,
            cleanup_staging_after_success=payload.cleanup_staging_after_success,
            staging_root=payload.staging_root,
            reporter=reporter,
        )
        result = prototype.run(payload.input_paths)
        return {
            "database_path": result.database_path,
            "total_indexed_stores": result.total_indexed_stores,
            "total_executed_packs": result.total_executed_packs,
            "indexed_stores": [dataclasses.asdict(one) for one in result.indexed_stores],
            "executed_packs": [dataclasses.asdict(one) for one in result.executed_packs],
        }


__all__ = ["ExistingDriveSquashfsBackupJobHandler", "ExistingDriveSquashfsBackupJobPayload"]
