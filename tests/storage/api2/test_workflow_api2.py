"""Contracts for the second-generation storage workflow layer."""

from __future__ import annotations

import dataclasses

from collections.abc import Iterator
from uuid import UUID

import pytest

import LiuXin_alpha.storage.api as api
from LiuXin_alpha.storage import utils as storage_utils


PRIMARY_STORE_UUID = UUID("00000000-0000-0000-0000-000000000001")
ARCHIVE_STORE_UUID = UUID("00000000-0000-0000-0000-000000000002")


class _MemoryBackupWorkflow(api.BackupWorkflowAPI):
    def __init__(
        self,
        declaration: api.BackupWorkflowDeclaration,
        *,
        checkpoint: api.BackupWorkflowCheckpoint | None = None,
    ) -> None:
        self._declaration = declaration
        self._checkpoint = checkpoint or api.BackupWorkflowCheckpoint(
            declaration,
            api.WorkflowStatus.DRAFT,
        )

    @property
    def workflow_kind(self) -> api.BackupWorkflowKind:
        return self._declaration.workflow_kind

    @property
    def workflow_name(self) -> str:
        return self._declaration.workflow_name

    def build_declaration(self) -> api.BackupWorkflowDeclaration:
        return self._declaration

    def progress(self) -> api.BackupWorkflowCheckpoint:
        return self._checkpoint

    def _add_source(self, source: api.BackupSourceDeclaration) -> api.BackupSourceDeclaration:
        if self._checkpoint.status is not api.WorkflowStatus.DRAFT:
            raise api.StorePreconditionFailed("sources are immutable after execution starts")
        self._declaration = dataclasses.replace(
            self._declaration,
            sources=(*self._declaration.sources, source),
        )
        self._checkpoint = dataclasses.replace(
            self._checkpoint,
            declaration=self._declaration,
        )
        return source

    def designate_local_path(
        self,
        source_path: str,
        *,
        archive_path: str | None = None,
    ) -> api.BackupSourceDeclaration:
        chosen_path = archive_path or f"source-{len(self._declaration.sources)}"
        return self._add_source(
            api.BackupSourceDeclaration(
                api.BackupSourceKind.LOCAL_PATH,
                source_path,
                archive_path=chosen_path,
            )
        )

    def designate_location(
        self,
        source_location: api.Location,
        *,
        archive_path: str | None = None,
    ) -> api.BackupSourceDeclaration:
        chosen_path = archive_path or f"source-{len(self._declaration.sources)}"
        return self._add_source(
            api.BackupSourceDeclaration(
                api.BackupSourceKind.STORE_LOCATION,
                source_location,
                archive_path=chosen_path,
            )
        )

    def run_next(self) -> api.BackupWorkflowCheckpoint:
        if self._checkpoint.status.terminal:
            return self._checkpoint

        index = self._checkpoint.next_source_index
        if index < len(self._declaration.sources):
            source = self._declaration.sources[index]
            report = api.BackupSourceStagingReport(
                source_index=index,
                source_identifier=source.source_identifier,
                archive_path=source.archive_path or f"source-{index}",
                bytes_staged=source.expected_size,
                digest_verified=source.expected_digest is not None,
            )
            completed_steps = self._checkpoint.completed_steps
            if index + 1 == len(self._declaration.sources):
                completed_steps = (*completed_steps, api.BackupWorkflowStepKind.STAGE_SOURCES)
            self._checkpoint = dataclasses.replace(
                self._checkpoint,
                status=api.WorkflowStatus.RUNNING,
                next_source_index=index + 1,
                staged_source_count=self._checkpoint.staged_source_count + 1,
                source_reports=(*self._checkpoint.source_reports, report),
                completed_steps=completed_steps,
            )
            return self._checkpoint

        steps = (
            *self._checkpoint.completed_steps,
            api.BackupWorkflowStepKind.SEAL_ARTIFACT,
        )
        if self._declaration.verify_after_build:
            steps = (*steps, api.BackupWorkflowStepKind.VERIFY_ARTIFACT)
        self._checkpoint = dataclasses.replace(
            self._checkpoint,
            status=api.WorkflowStatus.COMPLETE,
            completed_steps=steps,
            output_artifact_reference=self._declaration.output_target,
        )
        return self._checkpoint

    def run_to_completion(self) -> api.BackupWorkflowResult:
        while not self._checkpoint.status.terminal:
            self.run_next()
        return api.BackupWorkflowResult(
            declaration=self._declaration,
            status=self._checkpoint.status,
            workflow_id=self._checkpoint.workflow_id,
            output_artifact_reference=self._checkpoint.output_artifact_reference,
            source_reports=self._checkpoint.source_reports,
            completed_steps=self._checkpoint.completed_steps,
            last_error=self._checkpoint.last_error,
            final_checkpoint=self._checkpoint,
        )

    def cancel(self) -> api.BackupWorkflowCheckpoint:
        self._checkpoint = dataclasses.replace(
            self._checkpoint,
            status=api.WorkflowStatus.CANCELLED,
        )
        return self._checkpoint

    @classmethod
    def from_declaration(
        cls,
        declaration: api.BackupWorkflowDeclaration,
        *,
        storage_manager=None,
    ) -> _MemoryBackupWorkflow:
        return cls(declaration)

    @classmethod
    def from_checkpoint(
        cls,
        checkpoint: api.BackupWorkflowCheckpoint,
        *,
        storage_manager=None,
    ) -> _MemoryBackupWorkflow:
        if not checkpoint.status.resumable:
            raise api.StorePreconditionFailed("workflow state is not resumable")
        return cls(checkpoint.declaration, checkpoint=checkpoint)


class _MemoryWorkflowRepository(api.BackupWorkflowRepositoryAPI):
    def __init__(self) -> None:
        self.declarations: dict[int, api.BackupWorkflowDeclaration] = {}
        self.checkpoints: dict[int, api.BackupWorkflowCheckpoint] = {}
        self.statuses: dict[int, api.WorkflowStatus] = {}
        self.results: dict[int, api.BackupWorkflowResult] = {}
        self.presence: set[tuple[int, str, str]] = set()

    def save_workflow_declaration(
        self,
        declaration,
        *,
        workflow_id=None,
        status=api.WorkflowStatus.DRAFT,
    ):
        chosen_id = workflow_id or (max(self.declarations, default=0) + 1)
        self.declarations[chosen_id] = declaration
        self.statuses[chosen_id] = status
        return chosen_id

    def load_workflow_declaration(self, workflow_id):
        return self.declarations[workflow_id]

    def iter_workflow_declarations(self, *, status=None):
        for workflow_id in sorted(self.declarations):
            if status is None or self.statuses[workflow_id] is status:
                yield workflow_id, self.declarations[workflow_id]

    def save_checkpoint(self, workflow_id, checkpoint) -> None:
        self.checkpoints[workflow_id] = dataclasses.replace(
            checkpoint,
            workflow_id=workflow_id,
        )
        self.statuses[workflow_id] = checkpoint.status

    def load_checkpoint(self, workflow_id):
        if workflow_id in self.checkpoints:
            return self.checkpoints[workflow_id]
        return api.BackupWorkflowCheckpoint(
            self.declarations[workflow_id],
            self.statuses[workflow_id],
            workflow_id=workflow_id,
        )

    def record_result(self, workflow_id, result) -> None:
        self.results[workflow_id] = result
        self.statuses[workflow_id] = result.status

    def record_backup_presence(
        self,
        workflow_id,
        registration,
        source,
        *,
        archive_path,
        protected=True,
        immutable=True,
    ) -> bool:
        key = workflow_id, str(source.source_identifier), archive_path
        if key in self.presence:
            return False
        self.presence.add(key)
        return True

    def delete_workflow(self, workflow_id, *, require_terminal=True) -> bool:
        if workflow_id not in self.declarations:
            return False
        if require_terminal and not self.statuses[workflow_id].terminal:
            raise api.StorePreconditionFailed("workflow is not terminal")
        self.declarations.pop(workflow_id)
        self.checkpoints.pop(workflow_id, None)
        self.statuses.pop(workflow_id)
        self.results.pop(workflow_id, None)
        return True


def test_workflow_package_is_segregated_explicit_and_layered() -> None:
    from LiuXin_alpha.storage.api import workflow_api
    from LiuXin_alpha.storage.api.workflow_api.backup_api.artifact_api import (
        BackupArtifactRegistryAPI,
    )
    from LiuXin_alpha.storage.api.workflow_api.backup_api.planner_api import (
        BackupPlannerAPI,
    )
    from LiuXin_alpha.storage.api.workflow_api.backup_api.repository_api import (
        BackupWorkflowRepositoryAPI,
    )
    from LiuXin_alpha.storage.api.workflow_api.backup_api.workflow_api import (
        BackupWorkflowAPI,
    )

    assert workflow_api.BackupWorkflowAPI is BackupWorkflowAPI is api.BackupWorkflowAPI
    assert workflow_api.BackupPlannerAPI is BackupPlannerAPI is api.BackupPlannerAPI
    assert workflow_api.BackupArtifactRegistryAPI is BackupArtifactRegistryAPI
    assert workflow_api.BackupWorkflowRepositoryAPI is BackupWorkflowRepositoryAPI
    assert issubclass(api.BackupWorkflowAPI, api.StorageWorkflowAPI)
    assert len(workflow_api.__all__) == len(set(workflow_api.__all__))
    assert all(hasattr(workflow_api, name) for name in workflow_api.__all__)
    assert {
        "build_declaration",
        "cancel",
        "designate_local_path",
        "designate_location",
        "from_checkpoint",
        "from_declaration",
        "progress",
        "run_next",
        "run_to_completion",
        "workflow_kind",
        "workflow_name",
    } == api.BackupWorkflowAPI.__abstractmethods__
    with pytest.raises(TypeError):
        api.BackupWorkflowAPI()


def test_backup_models_validate_paths_sources_checkpoints_and_results() -> None:
    location = api.Location(PRIMARY_STORE_UUID, "objects/42")
    source = api.BackupSourceDeclaration(
        api.BackupSourceKind.STORE_LOCATION,
        location,
        archive_path=r"/books\\novel.epub",
        expected_size=4,
    )
    declaration = api.BackupWorkflowDeclaration(
        "nightly",
        api.BackupWorkflowKind.SQUASHFS_PACK,
        api.Location(ARCHIVE_STORE_UUID, "packs/nightly.sqsh"),
        sources=(source,),
        options=(("compression", "zstd"),),
    )

    assert source.archive_path == "books/novel.epub"
    assert source.location == location
    assert source.source_store_ref == PRIMARY_STORE_UUID
    assert declaration.option_map() == {"compression": "zstd"}
    assert "BackupWorkflowStatus" not in api.__all__
    assert not api.WorkflowStatus.DRAFT.terminal
    assert api.WorkflowStatus.FAILED.resumable

    with pytest.raises(TypeError, match="Location"):
        api.BackupSourceDeclaration(api.BackupSourceKind.STORE_LOCATION, "not-a-location")
    with pytest.raises(ValueError, match=r"\.\."):
        storage_utils.normalize_archive_path("../escape")
    with pytest.raises(ValueError, match="unique"):
        api.BackupWorkflowDeclaration(
            "duplicate",
            api.BackupWorkflowKind.SQUASHFS_PACK,
            "out.sqsh",
            sources=(source, source),
        )
    with pytest.raises(ValueError, match="terminal"):
        api.BackupWorkflowResult(declaration, api.WorkflowStatus.RUNNING)


def test_backup_workflow_runs_checkpoints_resumes_and_completes() -> None:
    declaration = api.BackupWorkflowDeclaration(
        "nightly",
        api.BackupWorkflowKind.SQUASHFS_PACK,
        api.Location(ARCHIVE_STORE_UUID, "packs/nightly.sqsh"),
    )
    workflow = _MemoryBackupWorkflow.from_declaration(declaration)
    workflow.designate_local_path("/books/a.epub", archive_path="books/a.epub")
    workflow.designate_location(
        api.Location(PRIMARY_STORE_UUID, "objects/42"),
        archive_path="books/b.epub",
    )

    first = workflow.run_next()
    assert first.status is api.WorkflowStatus.RUNNING
    assert first.next_source_index == 1
    assert first.remaining_source_count == 1

    resumed = _MemoryBackupWorkflow.from_checkpoint(first)
    result = resumed.run_to_completion()
    assert result.successful
    assert result.output_artifact_reference == declaration.output_target
    assert len(result.source_reports) == 2
    assert api.BackupWorkflowStepKind.STAGE_SOURCES in result.completed_steps
    assert api.BackupWorkflowStepKind.SEAL_ARTIFACT in result.completed_steps
    assert api.BackupWorkflowStepKind.VERIFY_ARTIFACT in result.completed_steps
    assert resumed.terminal


def test_backup_workflow_cancel_and_repository_are_durable_and_idempotent() -> None:
    declaration = api.BackupWorkflowDeclaration(
        "nightly",
        api.BackupWorkflowKind.SQUASHFS_PACK,
        "nightly.sqsh",
    )
    workflow = _MemoryBackupWorkflow.from_declaration(declaration)
    cancelled = workflow.cancel()
    assert cancelled.status is api.WorkflowStatus.CANCELLED
    assert workflow.terminal

    repository = _MemoryWorkflowRepository()
    workflow_id = repository.save_workflow_declaration(declaration)
    repository.save_checkpoint(workflow_id, cancelled)
    loaded = repository.load_checkpoint(workflow_id)
    assert loaded.workflow_id == workflow_id
    assert loaded.status is api.WorkflowStatus.CANCELLED
    assert list(repository.iter_workflow_declarations(status=api.WorkflowStatus.CANCELLED)) == [
        (workflow_id, declaration),
    ]

    registration = api.BackupArtifactRegistration(
        workflow_id,
        ARCHIVE_STORE_UUID,
        "archive-store",
        "nightly.sqsh",
    )
    source = api.BackupSourceDeclaration(api.BackupSourceKind.LOCAL_PATH, "/books/a")
    assert repository.record_backup_presence(
        workflow_id,
        registration,
        source,
        archive_path="books/a",
    )
    assert not repository.record_backup_presence(
        workflow_id,
        registration,
        source,
        archive_path="books/a",
    )
    assert repository.delete_workflow(workflow_id)
    assert not repository.delete_workflow(workflow_id)


def test_backup_planning_and_registration_facades_remain_separate() -> None:
    assert api.BackupPlannerAPI.__abstractmethods__ == {"plan_store_backup"}
    assert api.BackupArtifactRegistryAPI.__abstractmethods__ == {
        "get_artifact_registration",
        "iter_artifact_registrations",
        "register_artifact",
    }
    assert api.BackupWorkflowRepositoryAPI.__abstractmethods__ == {
        "delete_workflow",
        "iter_workflow_declarations",
        "load_checkpoint",
        "load_workflow_declaration",
        "record_backup_presence",
        "record_result",
        "save_checkpoint",
        "save_workflow_declaration",
    }
