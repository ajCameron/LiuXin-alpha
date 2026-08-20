"""Resumable, transactionally staged SquashFS backup workflow."""

from __future__ import annotations

import dataclasses
import hashlib
import pathlib
import shutil

from typing import TYPE_CHECKING
from uuid import UUID, uuid4

from LiuXin_alpha.storage.api import (
    BackupSourceDeclaration,
    BackupSourceKind,
    BackupSourceStagingReport,
    BackupWorkflowAPI,
    BackupWorkflowCheckpoint,
    BackupWorkflowDeclaration,
    BackupWorkflowKind,
    BackupWorkflowResult,
    BackupWorkflowStepKind,
    Digest,
    Location,
    StoreAlreadyExists,
    StoreIntegrityError,
    StorePreconditionFailed,
    StoreUnsupportedOperation,
    WorkflowStatus,
)
from LiuXin_alpha.storage.store_backend_plugins.squashfs_build import (
    SquashfsBuildStorageBackend,
)
from LiuXin_alpha.storage.store_backend_plugins.squashfs_readonly import (
    SquashfsReadOnlyStorageBackend,
)

if TYPE_CHECKING:
    from LiuXin_alpha.storage.api import StorageManagerAPI


class SquashfsBackupWorkflow(BackupWorkflowAPI):
    """Stage verified sources, seal atomically, and expose durable checkpoints."""

    def __init__(
        self,
        output_target: str | Location,
        *,
        workflow_name: str | None = None,
        verify_after_build: bool = True,
        cleanup_staging_after_success: bool = False,
        staging_target: str | None = None,
        mksquashfs_exe: str = "mksquashfs",
        compression: str = "zstd",
        deterministic: bool = False,
        storage_manager: StorageManagerAPI | None = None,
        _builder_store_uuid: UUID | None = None,
    ) -> None:
        self._output_target = output_target
        self._storage_manager = storage_manager
        self._workflow_name = workflow_name or _default_workflow_name(output_target)
        self._verify_after_build = bool(verify_after_build)
        self._cleanup_staging_after_success = bool(cleanup_staging_after_success)
        builder_store_uuid = _builder_store_uuid or uuid4()
        build_output = _local_build_output(
            output_target,
            staging_target,
            builder_store_uuid=builder_store_uuid,
        )
        self._builder = SquashfsBuildStorageBackend(
            url=str(build_output),
            name=self._workflow_name,
            uuid=builder_store_uuid,
            staging_root=staging_target,
            mksquashfs_exe=mksquashfs_exe,
            compression=compression,
            deterministic=deterministic,
        )
        self._sources: list[BackupSourceDeclaration] = []
        self._source_reports: list[BackupSourceStagingReport] = []
        self._status = WorkflowStatus.DRAFT
        self._workflow_id: int | None = None
        self._next_source_index = 0
        self._completed_steps: list[BackupWorkflowStepKind] = []
        self._output_artifact_reference: str | Location | None = None
        self._last_error: str | None = None

    @property
    def workflow_kind(self) -> BackupWorkflowKind:
        return BackupWorkflowKind.SQUASHFS_PACK

    @property
    def workflow_name(self) -> str:
        return self._workflow_name

    def build_declaration(self) -> BackupWorkflowDeclaration:
        return BackupWorkflowDeclaration(
            workflow_name=self.workflow_name,
            workflow_kind=self.workflow_kind,
            output_target=self._output_target,
            sources=tuple(self._sources),
            verify_after_build=self._verify_after_build,
            cleanup_staging_after_success=self._cleanup_staging_after_success,
            staging_target=str(self._builder.staging_root),
            options=(
                ("mksquashfs_exe", self._builder._mksquashfs_exe),
                ("compression", self._builder._compression),
                ("deterministic", "1" if self._builder._deterministic else "0"),
                ("builder_store_uuid", str(self._builder.store_ref)),
            ),
        )

    def progress(self) -> BackupWorkflowCheckpoint:
        return BackupWorkflowCheckpoint(
            declaration=self.build_declaration(),
            status=self._status,
            workflow_id=self._workflow_id,
            next_source_index=self._next_source_index,
            staged_source_count=sum(report.ok for report in self._source_reports),
            source_reports=tuple(self._source_reports),
            completed_steps=tuple(self._completed_steps),
            output_artifact_reference=self._output_artifact_reference,
            last_error=self._last_error,
        )

    def designate_local_path(
        self,
        source_path: str,
        *,
        archive_path: str | None = None,
    ) -> BackupSourceDeclaration:
        self._require_draft()
        source = pathlib.Path(source_path).expanduser().resolve(strict=False)
        expected_size = source.stat().st_size if source.is_file() else None
        declaration = BackupSourceDeclaration(
            source_kind=BackupSourceKind.LOCAL_PATH,
            source_identifier=str(source),
            archive_path=archive_path or source.name,
            expected_size=expected_size,
        )
        self._append_source(declaration)
        return declaration

    def designate_location(
        self,
        source_location: Location,
        *,
        archive_path: str | None = None,
    ) -> BackupSourceDeclaration:
        self._require_draft()
        if not isinstance(source_location, Location):
            raise TypeError("source_location must be a Location.")
        expected_size = None
        expected_digest = None
        if self._storage_manager is not None:
            info = self._storage_manager.stat(source_location)
            expected_size = info.size
            expected_digest = info.digest
        declaration = BackupSourceDeclaration(
            source_kind=BackupSourceKind.STORE_LOCATION,
            source_identifier=source_location,
            archive_path=archive_path or f"source-{len(self._sources):06d}",
            expected_size=expected_size,
            expected_digest=expected_digest,
            source_store_ref=source_location.store_ref,
        )
        self._append_source(declaration)
        return declaration

    def run_next(self) -> BackupWorkflowCheckpoint:
        if self._status in {WorkflowStatus.COMPLETE, WorkflowStatus.CANCELLED}:
            return self.progress()
        try:
            self._status = WorkflowStatus.RUNNING
            self._last_error = None
            if self._next_source_index < len(self._sources):
                report = self._stage_one(
                    self._sources[self._next_source_index],
                    self._next_source_index,
                )
                self._source_reports.append(report)
                self._next_source_index += 1
                if self._next_source_index == len(self._sources):
                    self._mark_complete(BackupWorkflowStepKind.STAGE_SOURCES)
                return self.progress()
            self._seal_and_publish()
            self._status = WorkflowStatus.COMPLETE
        except Exception as error:
            self._status = WorkflowStatus.FAILED
            self._last_error = f"{type(error).__name__}: {error}"
        return self.progress()

    def run_to_completion(self) -> BackupWorkflowResult:
        while self._status not in {
            WorkflowStatus.COMPLETE,
            WorkflowStatus.FAILED,
            WorkflowStatus.CANCELLED,
        }:
            self.run_next()
        checkpoint = self.progress()
        return BackupWorkflowResult(
            declaration=checkpoint.declaration,
            status=checkpoint.status,
            workflow_id=checkpoint.workflow_id,
            output_artifact_reference=checkpoint.output_artifact_reference,
            source_reports=checkpoint.source_reports,
            completed_steps=checkpoint.completed_steps,
            last_error=checkpoint.last_error,
            final_checkpoint=checkpoint,
        )

    def cancel(self) -> BackupWorkflowCheckpoint:
        if self._status is WorkflowStatus.COMPLETE:
            raise StorePreconditionFailed("a completed workflow cannot be cancelled.")
        self._status = WorkflowStatus.CANCELLED
        self._last_error = None
        return self.progress()

    @classmethod
    def from_declaration(
        cls,
        declaration: BackupWorkflowDeclaration,
        *,
        storage_manager: StorageManagerAPI | None = None,
    ) -> SquashfsBackupWorkflow:
        if declaration.workflow_kind is not BackupWorkflowKind.SQUASHFS_PACK:
            raise ValueError("declaration is not a SquashFS pack workflow.")
        if isinstance(declaration.staging_target, Location):
            raise StoreUnsupportedOperation(
                "SquashFS build staging currently requires a local path."
            )
        options = declaration.option_map()
        workflow = cls(
            declaration.output_target,
            workflow_name=declaration.workflow_name,
            verify_after_build=declaration.verify_after_build,
            cleanup_staging_after_success=declaration.cleanup_staging_after_success,
            staging_target=declaration.staging_target,
            mksquashfs_exe=options.get("mksquashfs_exe", "mksquashfs"),
            compression=options.get("compression", "zstd"),
            deterministic=options.get("deterministic", "0") == "1",
            storage_manager=storage_manager,
            _builder_store_uuid=(
                None
                if "builder_store_uuid" not in options
                else UUID(options["builder_store_uuid"])
            ),
        )
        workflow._sources = list(declaration.sources)
        return workflow

    @classmethod
    def from_checkpoint(
        cls,
        checkpoint: BackupWorkflowCheckpoint,
        *,
        storage_manager: StorageManagerAPI | None = None,
    ) -> SquashfsBackupWorkflow:
        if not checkpoint.status.resumable:
            raise StorePreconditionFailed("workflow checkpoint is not resumable.")
        workflow = cls.from_declaration(
            checkpoint.declaration,
            storage_manager=storage_manager,
        )
        workflow._workflow_id = checkpoint.workflow_id
        workflow._status = checkpoint.status
        workflow._next_source_index = checkpoint.next_source_index
        workflow._source_reports = list(checkpoint.source_reports)
        workflow._completed_steps = list(checkpoint.completed_steps)
        workflow._output_artifact_reference = checkpoint.output_artifact_reference
        workflow._last_error = checkpoint.last_error
        return workflow

    def _append_source(self, source: BackupSourceDeclaration) -> None:
        if any(
            existing.archive_path == source.archive_path
            for existing in self._sources
        ):
            raise ValueError(f"duplicate backup archive path: {source.archive_path!r}.")
        self._sources.append(source)

    def _stage_one(
        self,
        source: BackupSourceDeclaration,
        source_index: int,
    ) -> BackupSourceStagingReport:
        assert source.archive_path is not None
        destination = self._builder.locate(source.archive_path)
        existing = self._builder.try_stat(destination)
        if existing is not None:
            self._verify_staged(existing.location, source)
            return BackupSourceStagingReport(
                source_index,
                source.source_identifier,
                source.archive_path,
                staged_location=existing.location,
                bytes_staged=existing.size,
                digest_verified=source.expected_digest is not None,
            )

        if source.source_kind is BackupSourceKind.LOCAL_PATH:
            path = pathlib.Path(str(source.source_identifier))
            if not path.is_file():
                raise FileNotFoundError(str(path))
            info = self._builder.store_file(
                path,
                location=destination,
                expected_size=source.expected_size,
                expected_digest=source.expected_digest,
            )
        else:
            if self._storage_manager is None:
                raise StorePreconditionFailed(
                    "store-location sources require a storage manager."
                )
            location = source.location
            assert location is not None
            current = self._storage_manager.stat(location)
            if source.expected_size is not None and current.size != source.expected_size:
                raise StoreIntegrityError("backup source size changed before staging.")
            with self._storage_manager.get(location) as input_stream:
                info = self._builder.store_stream(
                    input_stream,
                    location=destination,
                    expected_size=current.size,
                    expected_digest=source.expected_digest or current.digest,
                )
        return BackupSourceStagingReport(
            source_index,
            source.source_identifier,
            source.archive_path,
            staged_location=info.location,
            bytes_staged=info.size,
            digest_verified=source.expected_digest is not None,
        )

    def _verify_staged(
        self,
        staged_location: Location,
        source: BackupSourceDeclaration,
    ) -> None:
        info = self._builder.stat(staged_location)
        if source.expected_size is not None and info.size != source.expected_size:
            raise StoreIntegrityError("existing staged source has the wrong size.")
        if source.expected_digest is not None:
            observed = self._builder.compute_digest(
                staged_location,
                source.expected_digest.algorithm,
            )
            if observed != source.expected_digest:
                raise StoreIntegrityError("existing staged source has the wrong digest.")

    def _seal_and_publish(self) -> None:
        local_archive = self._builder.archive_path
        if local_archive.is_file():
            if BackupWorkflowStepKind.SEAL_ARTIFACT not in self._completed_steps:
                raise StoreAlreadyExists(
                    "backup output already exists without a checkpoint proving "
                    "that this workflow sealed it."
                )
            built = SquashfsReadOnlyStorageBackend(
                str(local_archive),
                name=f"{self.workflow_name} (resumed)",
            )
        else:
            built = self._builder.seal(force=False, quiet=True)
        self._mark_complete(BackupWorkflowStepKind.SEAL_ARTIFACT)

        if self._verify_after_build:
            status = built.self_test()
            if not status.available:
                raise StoreIntegrityError(
                    status.message or "sealed SquashFS artifact is unreadable."
                )
            self._mark_complete(BackupWorkflowStepKind.VERIFY_ARTIFACT)

        if isinstance(self._output_target, Location):
            if self._storage_manager is None:
                raise StorePreconditionFailed(
                    "Location output targets require a storage manager."
                )
            digest = _file_digest(local_archive)
            try:
                with local_archive.open("rb") as source:
                    self._storage_manager.put(
                        self._output_target,
                        source,
                        expected_size=local_archive.stat().st_size,
                        expected_digest=digest,
                    )
            except StoreAlreadyExists:
                target_store = self._storage_manager.get_store(
                    self._output_target.store_ref
                )
                if target_store.compute_digest(
                    self._output_target,
                    digest.algorithm,
                ) != digest:
                    raise
            self._output_artifact_reference = self._output_target
        else:
            self._output_artifact_reference = str(local_archive)

        if self._cleanup_staging_after_success:
            # A local output is deliberately outside staging. A Location output
            # has already been committed before cleanup begins.
            shutil.rmtree(self._builder.staging_root, ignore_errors=True)
            self._mark_complete(BackupWorkflowStepKind.CLEANUP)

    def _mark_complete(self, step: BackupWorkflowStepKind) -> None:
        if step not in self._completed_steps:
            self._completed_steps.append(step)

    def _require_draft(self) -> None:
        if self._status is not WorkflowStatus.DRAFT:
            raise StorePreconditionFailed(
                "backup sources are immutable after execution starts."
            )


def _default_workflow_name(output_target: str | Location) -> str:
    if isinstance(output_target, Location):
        return "squashfs-backup"
    path = pathlib.Path(output_target)
    return path.stem or path.name or "squashfs-backup"


def _local_build_output(
    output_target: str | Location,
    staging_target: str | None,
    *,
    builder_store_uuid: UUID,
) -> pathlib.Path:
    if isinstance(output_target, str):
        return pathlib.Path(output_target).expanduser().resolve(strict=False)
    if staging_target is None:
        raise StorePreconditionFailed(
            "Location output targets require a persistent local staging_target."
        )
    return (
        pathlib.Path(staging_target).expanduser().resolve(strict=False).parent
        / f".liuxin-backup-output-{builder_store_uuid.hex}.squashfs"
    )


def _file_digest(path: pathlib.Path) -> Digest:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return Digest("sha256", digest.hexdigest())


__all__ = ["SquashfsBackupWorkflow"]
