"""SquashFS backup workflow implementation.

This is an example of the intended backup-workflow shape:
- designation of sources is explicit
- staging/sealing is tracked separately from store plugin mechanics
- resume state is first-class and serializable
"""

from __future__ import annotations

import pathlib
import shutil
from collections.abc import Callable

from LiuXin_alpha.storage.api.workflow_apis.backup_api import BackupWorkflowAPI
from LiuXin_alpha.storage.api.workflow_apis.backup_api import (
    BackupSourceKind,
    BackupSourceResult,
    BackupSourceSpec,
    BackupWorkflowKind,
    BackupWorkflowResult,
    BackupWorkflowResumeState,
    BackupWorkflowSpec,
    BackupWorkflowStatus,
    BackupWorkflowStepKind,
)
from LiuXin_alpha.storage.api.location_api import StoreLocationMixinAPI
from LiuXin_alpha.storage.store_backend_plugins.squashfs_build import SquashfsBuildStorageBackend


class SquashfsBackupWorkflow(BackupWorkflowAPI):
    """Resumable workflow that stages designated sources then seals SquashFS."""

    workflow_kind = BackupWorkflowKind.SQUASHFS_PACK.value

    def __init__(
        self,
        output_url: str,
        *,
        workflow_name: str | None = None,
        verify_after_build: bool = True,
        cleanup_staging_after_success: bool = False,
        staging_root: str | None = None,
        mksquashfs_exe: str = "mksquashfs",
        compression: str = "zstd",
        deterministic: bool = False,
        location_loader: Callable[[str], StoreLocationMixinAPI] | None = None,
    ) -> None:
        self._output_url = str(output_url)
        self._workflow_name = workflow_name or pathlib.Path(self._output_url).stem or "squashfs_backup"
        self._verify_after_build = bool(verify_after_build)
        self._cleanup_staging_after_success = bool(cleanup_staging_after_success)
        self._location_loader = location_loader
        self._builder = SquashfsBuildStorageBackend(
            url=self._output_url,
            name=self._workflow_name,
            staging_root=staging_root,
            mksquashfs_exe=mksquashfs_exe,
            compression=compression,
            deterministic=deterministic,
        )
        self._sources: list[BackupSourceSpec] = []
        self._source_results: list[BackupSourceResult] = []
        self._status = BackupWorkflowStatus.DRAFT
        self._next_source_index = 0
        self._completed_steps: list[BackupWorkflowStepKind] = []
        self._output_artifact_url: str | None = None
        self._last_error: str | None = None

    @property
    def workflow_name(self) -> str:
        return self._workflow_name

    def _normalized_archive_path(self, archive_path: str | None, fallback: str) -> str:
        chosen = archive_path if archive_path is not None else fallback
        text = str(chosen).replace("\\", "/").lstrip("/")
        if not text:
            raise ValueError("Backup archive paths must not be empty.")
        if any(part == ".." for part in text.split("/")):
            raise ValueError("Backup archive paths must not contain '..'.")
        return text

    def _options(self) -> tuple[tuple[str, str], ...]:
        return (
            ("mksquashfs_exe", self._builder._mksquashfs_exe),
            ("compression", self._builder._compression),
            ("deterministic", "1" if self._builder._deterministic else "0"),
        )

    def build_spec(self) -> BackupWorkflowSpec:
        return BackupWorkflowSpec(
            workflow_name=self.workflow_name,
            workflow_kind=BackupWorkflowKind.SQUASHFS_PACK,
            output_url=self._output_url,
            sources=tuple(self._sources),
            verify_after_build=self._verify_after_build,
            cleanup_staging_after_success=self._cleanup_staging_after_success,
            staging_root=str(self._builder.staging_root),
            options=self._options(),
        )

    def progress(self) -> BackupWorkflowResumeState:
        return BackupWorkflowResumeState(
            spec=self.build_spec(),
            status=self._status,
            next_source_index=self._next_source_index,
            staged_source_count=sum(1 for item in self._source_results if item.ok),
            source_results=tuple(self._source_results),
            completed_steps=tuple(self._completed_steps),
            output_artifact_url=self._output_artifact_url,
            last_error=self._last_error,
        )

    def designate_local_path(
        self,
        source_path: str,
        *,
        archive_path: str | None = None,
    ) -> BackupSourceSpec:
        source = pathlib.Path(source_path).expanduser().resolve()
        spec = BackupSourceSpec(
            source_kind=BackupSourceKind.LOCAL_PATH,
            source_identifier=str(source),
            archive_path=self._normalized_archive_path(archive_path, fallback=source.name),
            expected_size=source.stat().st_size if source.exists() and source.is_file() else None,
        )
        self._sources.append(spec)
        return spec

    def designate_location(
        self,
        source_location: StoreLocationMixinAPI,
        *,
        archive_path: str | None = None,
    ) -> BackupSourceSpec:
        spec = BackupSourceSpec(
            source_kind=BackupSourceKind.STORE_LOCATION,
            source_identifier=source_location.file_url,
            archive_path=self._normalized_archive_path(
                archive_path,
                fallback="/".join(source_location.parts) if source_location.parts else source_location.name,
            ),
            expected_size=source_location.size,
        )
        self._sources.append(spec)
        return spec

    def _ensure_unique_archive_paths(self) -> None:
        seen: dict[str, str] = {}
        for source in self._sources:
            archive_path = source.archive_path or ""
            if archive_path in seen:
                raise ValueError(
                    "Backup workflow has duplicate archive path {!r} from both {!r} and {!r}.".format(
                        archive_path,
                        seen[archive_path],
                        source.source_identifier,
                    )
                )
            seen[archive_path] = source.source_identifier

    def _stage_one(self, source: BackupSourceSpec, source_index: int) -> BackupSourceResult:
        archive_path = source.archive_path or ""
        if source.source_kind is BackupSourceKind.LOCAL_PATH:
            staged = self._builder.designate_file(source.source_identifier, archive_path=archive_path)
        elif source.source_kind is BackupSourceKind.STORE_LOCATION:
            if self._location_loader is None:
                raise RuntimeError(
                    "Cannot resume or stage store-location backup sources without a location_loader."
                )
            src_location = self._location_loader(source.source_identifier)
            staged = self._builder.write_bytes(src_location.as_bytes(), location=archive_path)
        else:
            raise ValueError("Unknown backup source kind: {!r}".format(source.source_kind))
        return BackupSourceResult(
            source_index=source_index,
            source_identifier=source.source_identifier,
            archive_path=archive_path,
            staged_location_url=staged.file_url,
            ok=True,
            error=None,
        )

    def _seal(self) -> None:
        built = self._builder.seal(force=False, quiet=True)
        self._output_artifact_url = built.url
        if BackupWorkflowStepKind.SEAL_ARTIFACT not in self._completed_steps:
            self._completed_steps.append(BackupWorkflowStepKind.SEAL_ARTIFACT)
        if self._verify_after_build:
            built.self_test()
            if BackupWorkflowStepKind.VERIFY_ARTIFACT not in self._completed_steps:
                self._completed_steps.append(BackupWorkflowStepKind.VERIFY_ARTIFACT)
        if self._cleanup_staging_after_success:
            shutil.rmtree(self._builder.staging_root, ignore_errors=True)
            if BackupWorkflowStepKind.CLEANUP not in self._completed_steps:
                self._completed_steps.append(BackupWorkflowStepKind.CLEANUP)

    def run_next(self) -> BackupWorkflowResumeState:
        if self._status in {BackupWorkflowStatus.COMPLETE, BackupWorkflowStatus.CANCELLED}:
            return self.progress()

        try:
            self._ensure_unique_archive_paths()
            self._status = BackupWorkflowStatus.RUNNING
            self._last_error = None
            if self._next_source_index < len(self._sources):
                result = self._stage_one(self._sources[self._next_source_index], self._next_source_index)
                self._source_results.append(result)
                self._next_source_index += 1
                if self._next_source_index >= len(self._sources):
                    if BackupWorkflowStepKind.STAGE_SOURCES not in self._completed_steps:
                        self._completed_steps.append(BackupWorkflowStepKind.STAGE_SOURCES)
                return self.progress()
            self._seal()
            self._status = BackupWorkflowStatus.COMPLETE
        except Exception as exc:
            self._status = BackupWorkflowStatus.FAILED
            self._last_error = str(exc)
        return self.progress()

    def run_to_completion(self) -> BackupWorkflowResult:
        while self.progress().status not in {
            BackupWorkflowStatus.COMPLETE,
            BackupWorkflowStatus.FAILED,
            BackupWorkflowStatus.CANCELLED,
        }:
            self.run_next()
        state = self.progress()
        return BackupWorkflowResult(
            spec=state.spec,
            status=state.status,
            output_artifact_url=state.output_artifact_url,
            source_results=state.source_results,
            completed_steps=state.completed_steps,
            last_error=state.last_error,
            resume_state=state,
        )

    def cancel(self) -> BackupWorkflowResumeState:
        self._status = BackupWorkflowStatus.CANCELLED
        return self.progress()

    @classmethod
    def from_spec(
        cls,
        spec: BackupWorkflowSpec,
        *,
        location_loader: Callable[[str], StoreLocationMixinAPI] | None = None,
    ) -> "SquashfsBackupWorkflow":
        if spec.workflow_kind is not BackupWorkflowKind.SQUASHFS_PACK:
            raise ValueError(
                "SquashfsBackupWorkflow cannot build workflow kind {!r}.".format(spec.workflow_kind)
            )
        options = spec.option_map()
        workflow = cls(
            spec.output_url,
            workflow_name=spec.workflow_name,
            verify_after_build=spec.verify_after_build,
            cleanup_staging_after_success=spec.cleanup_staging_after_success,
            staging_root=spec.staging_root,
            mksquashfs_exe=options.get("mksquashfs_exe", "mksquashfs"),
            compression=options.get("compression", "zstd"),
            deterministic=options.get("deterministic", "0") == "1",
            location_loader=location_loader,
        )
        workflow._sources = list(spec.sources)
        return workflow

    @classmethod
    def from_resume_state(
        cls,
        resume_state: BackupWorkflowResumeState,
        *,
        location_loader: Callable[[str], StoreLocationMixinAPI] | None = None,
    ) -> "SquashfsBackupWorkflow":
        spec = resume_state.spec
        if spec.workflow_kind is not BackupWorkflowKind.SQUASHFS_PACK:
            raise ValueError(
                "SquashfsBackupWorkflow cannot resume workflow kind {!r}.".format(spec.workflow_kind)
            )
        options = spec.option_map()
        workflow = cls(
            spec.output_url,
            workflow_name=spec.workflow_name,
            verify_after_build=spec.verify_after_build,
            cleanup_staging_after_success=spec.cleanup_staging_after_success,
            staging_root=spec.staging_root,
            mksquashfs_exe=options.get("mksquashfs_exe", "mksquashfs"),
            compression=options.get("compression", "zstd"),
            deterministic=options.get("deterministic", "0") == "1",
            location_loader=location_loader,
        )
        workflow._sources = list(spec.sources)
        workflow._status = resume_state.status
        workflow._next_source_index = resume_state.next_source_index
        workflow._source_results = list(resume_state.source_results)
        workflow._completed_steps = list(resume_state.completed_steps)
        workflow._output_artifact_url = resume_state.output_artifact_url
        workflow._last_error = resume_state.last_error
        return workflow
