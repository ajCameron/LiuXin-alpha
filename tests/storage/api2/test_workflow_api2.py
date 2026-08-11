"""Contracts for the second-generation storage workflow layer."""

from __future__ import annotations

import dataclasses

from collections.abc import Iterator

import pytest

import LiuXin_alpha.storage.api2 as api2


class _MemoryBackupWorkflow(api2.BackupWorkflowAPI):
    def __init__(
        self,
        spec: api2.BackupWorkflowSpec,
        *,
        state: api2.BackupWorkflowResumeState | None = None,
    ) -> None:
        self._spec = spec
        self._state = state or api2.BackupWorkflowResumeState(
            spec,
            api2.WorkflowStatus.DRAFT,
        )

    @property
    def workflow_kind(self) -> api2.BackupWorkflowKind:
        return self._spec.workflow_kind

    @property
    def workflow_name(self) -> str:
        return self._spec.workflow_name

    def build_spec(self) -> api2.BackupWorkflowSpec:
        return self._spec

    def progress(self) -> api2.BackupWorkflowResumeState:
        return self._state

    def _add_source(self, source: api2.BackupSourceSpec) -> api2.BackupSourceSpec:
        if self._state.status is not api2.WorkflowStatus.DRAFT:
            raise api2.StorePreconditionFailed("sources are immutable after execution starts")
        self._spec = dataclasses.replace(
            self._spec,
            sources=(*self._spec.sources, source),
        )
        self._state = dataclasses.replace(self._state, spec=self._spec)
        return source

    def designate_local_path(
        self,
        source_path: str,
        *,
        archive_path: str | None = None,
    ) -> api2.BackupSourceSpec:
        chosen_path = archive_path or f"source-{len(self._spec.sources)}"
        return self._add_source(
            api2.BackupSourceSpec(
                api2.BackupSourceKind.LOCAL_PATH,
                source_path,
                archive_path=chosen_path,
            )
        )

    def designate_location(
        self,
        source_location: api2.Location,
        *,
        archive_path: str | None = None,
    ) -> api2.BackupSourceSpec:
        chosen_path = archive_path or f"source-{len(self._spec.sources)}"
        return self._add_source(
            api2.BackupSourceSpec(
                api2.BackupSourceKind.STORE_LOCATION,
                source_location,
                archive_path=chosen_path,
            )
        )

    def run_next(self) -> api2.BackupWorkflowResumeState:
        if self._state.status.terminal:
            return self._state

        index = self._state.next_source_index
        if index < len(self._spec.sources):
            source = self._spec.sources[index]
            result = api2.BackupSourceResult(
                source_index=index,
                source_identifier=source.source_identifier,
                archive_path=source.archive_path or f"source-{index}",
                bytes_staged=source.expected_size,
                digest_verified=source.expected_digest is not None,
            )
            completed_steps = self._state.completed_steps
            if index + 1 == len(self._spec.sources):
                completed_steps = (*completed_steps, api2.BackupWorkflowStepKind.STAGE_SOURCES)
            self._state = dataclasses.replace(
                self._state,
                status=api2.WorkflowStatus.RUNNING,
                next_source_index=index + 1,
                staged_source_count=self._state.staged_source_count + 1,
                source_results=(*self._state.source_results, result),
                completed_steps=completed_steps,
            )
            return self._state

        steps = (*self._state.completed_steps, api2.BackupWorkflowStepKind.SEAL_ARTIFACT)
        if self._spec.verify_after_build:
            steps = (*steps, api2.BackupWorkflowStepKind.VERIFY_ARTIFACT)
        self._state = dataclasses.replace(
            self._state,
            status=api2.WorkflowStatus.COMPLETE,
            completed_steps=steps,
            output_artifact=self._spec.output_target,
        )
        return self._state

    def run_to_completion(self) -> api2.BackupWorkflowResult:
        while not self._state.status.terminal:
            self.run_next()
        return api2.BackupWorkflowResult(
            spec=self._spec,
            status=self._state.status,
            workflow_id=self._state.workflow_id,
            output_artifact=self._state.output_artifact,
            source_results=self._state.source_results,
            completed_steps=self._state.completed_steps,
            last_error=self._state.last_error,
            resume_state=self._state,
        )

    def cancel(self) -> api2.BackupWorkflowResumeState:
        self._state = dataclasses.replace(
            self._state,
            status=api2.WorkflowStatus.CANCELLED,
        )
        return self._state

    @classmethod
    def from_spec(
        cls,
        spec: api2.BackupWorkflowSpec,
        *,
        storage_manager=None,
    ) -> _MemoryBackupWorkflow:
        return cls(spec)

    @classmethod
    def from_resume_state(
        cls,
        resume_state: api2.BackupWorkflowResumeState,
        *,
        storage_manager=None,
    ) -> _MemoryBackupWorkflow:
        if not resume_state.status.resumable:
            raise api2.StorePreconditionFailed("workflow state is not resumable")
        return cls(resume_state.spec, state=resume_state)


class _MemoryWorkflowRepository(api2.BackupWorkflowRepositoryAPI):
    def __init__(self) -> None:
        self.specs: dict[int, api2.BackupWorkflowSpec] = {}
        self.states: dict[int, api2.BackupWorkflowResumeState] = {}
        self.statuses: dict[int, api2.WorkflowStatus] = {}
        self.results: dict[int, api2.BackupWorkflowResult] = {}
        self.presence: set[tuple[int, str, str]] = set()

    def save_workflow_spec(
        self,
        spec,
        *,
        workflow_id=None,
        status=api2.WorkflowStatus.DRAFT,
    ):
        chosen_id = workflow_id or (max(self.specs, default=0) + 1)
        self.specs[chosen_id] = spec
        self.statuses[chosen_id] = status
        return chosen_id

    def load_workflow_spec(self, workflow_id):
        return self.specs[workflow_id]

    def iter_workflow_specs(self, *, status=None):
        for workflow_id in sorted(self.specs):
            if status is None or self.statuses[workflow_id] is status:
                yield workflow_id, self.specs[workflow_id]

    def save_resume_state(self, workflow_id, state) -> None:
        self.states[workflow_id] = dataclasses.replace(state, workflow_id=workflow_id)
        self.statuses[workflow_id] = state.status

    def load_resume_state(self, workflow_id):
        if workflow_id in self.states:
            return self.states[workflow_id]
        return api2.BackupWorkflowResumeState(
            self.specs[workflow_id],
            self.statuses[workflow_id],
            workflow_id=workflow_id,
        )

    def record_result(self, workflow_id, result) -> None:
        self.results[workflow_id] = result
        self.statuses[workflow_id] = result.status

    def record_backup_presence(
        self,
        workflow_id,
        artifact,
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
        if workflow_id not in self.specs:
            return False
        if require_terminal and not self.statuses[workflow_id].terminal:
            raise api2.StorePreconditionFailed("workflow is not terminal")
        self.specs.pop(workflow_id)
        self.states.pop(workflow_id, None)
        self.statuses.pop(workflow_id)
        self.results.pop(workflow_id, None)
        return True


def test_workflow_package_is_segregated_explicit_and_layered() -> None:
    from LiuXin_alpha.storage.api2 import workflow_api
    from LiuXin_alpha.storage.api2.workflow_api.backup_api.artifact_api import (
        BackupArtifactRegistryAPI,
    )
    from LiuXin_alpha.storage.api2.workflow_api.backup_api.planner_api import (
        BackupPlannerAPI,
    )
    from LiuXin_alpha.storage.api2.workflow_api.backup_api.repository_api import (
        BackupWorkflowRepositoryAPI,
    )
    from LiuXin_alpha.storage.api2.workflow_api.backup_api.workflow_api import (
        BackupWorkflowAPI,
    )

    assert workflow_api.BackupWorkflowAPI is BackupWorkflowAPI is api2.BackupWorkflowAPI
    assert workflow_api.BackupPlannerAPI is BackupPlannerAPI is api2.BackupPlannerAPI
    assert workflow_api.BackupArtifactRegistryAPI is BackupArtifactRegistryAPI
    assert workflow_api.BackupWorkflowRepositoryAPI is BackupWorkflowRepositoryAPI
    assert issubclass(api2.BackupWorkflowAPI, api2.StorageWorkflowAPI)
    assert len(workflow_api.__all__) == len(set(workflow_api.__all__))
    assert all(hasattr(workflow_api, name) for name in workflow_api.__all__)
    assert {
        "build_spec",
        "cancel",
        "designate_local_path",
        "designate_location",
        "from_resume_state",
        "from_spec",
        "progress",
        "run_next",
        "run_to_completion",
        "workflow_kind",
        "workflow_name",
    } == api2.BackupWorkflowAPI.__abstractmethods__
    with pytest.raises(TypeError):
        api2.BackupWorkflowAPI()


def test_backup_models_validate_paths_sources_checkpoints_and_results() -> None:
    location = api2.Location("primary", "objects/42")
    source = api2.BackupSourceSpec(
        api2.BackupSourceKind.STORE_LOCATION,
        location,
        archive_path=r"/books\\novel.epub",
        expected_size=4,
    )
    spec = api2.BackupWorkflowSpec(
        "nightly",
        api2.BackupWorkflowKind.SQUASHFS_PACK,
        api2.Location("archive", "packs/nightly.sqsh"),
        sources=(source,),
        options=(("compression", "zstd"),),
    )

    assert source.archive_path == "books/novel.epub"
    assert source.location == location
    assert source.source_store_ref == "primary"
    assert spec.option_map() == {"compression": "zstd"}
    assert api2.BackupWorkflowStatus is api2.WorkflowStatus
    assert not api2.WorkflowStatus.DRAFT.terminal
    assert api2.WorkflowStatus.FAILED.resumable

    with pytest.raises(TypeError, match="Location"):
        api2.BackupSourceSpec(api2.BackupSourceKind.STORE_LOCATION, "not-a-location")
    with pytest.raises(ValueError, match=r"\.\."):
        api2.normalize_archive_path("../escape")
    with pytest.raises(ValueError, match="unique"):
        api2.BackupWorkflowSpec(
            "duplicate",
            api2.BackupWorkflowKind.SQUASHFS_PACK,
            "out.sqsh",
            sources=(source, source),
        )
    with pytest.raises(ValueError, match="terminal"):
        api2.BackupWorkflowResult(spec, api2.WorkflowStatus.RUNNING)


def test_backup_workflow_runs_checkpoints_resumes_and_completes() -> None:
    spec = api2.BackupWorkflowSpec(
        "nightly",
        api2.BackupWorkflowKind.SQUASHFS_PACK,
        api2.Location("archive", "packs/nightly.sqsh"),
    )
    workflow = _MemoryBackupWorkflow.from_spec(spec)
    workflow.designate_local_path("/books/a.epub", archive_path="books/a.epub")
    workflow.designate_location(
        api2.Location("primary", "objects/42"),
        archive_path="books/b.epub",
    )

    first = workflow.run_next()
    assert first.status is api2.WorkflowStatus.RUNNING
    assert first.next_source_index == 1
    assert first.remaining_source_count == 1

    resumed = _MemoryBackupWorkflow.from_resume_state(first)
    result = resumed.run_to_completion()
    assert result.successful
    assert result.output_artifact == spec.output_target
    assert len(result.source_results) == 2
    assert api2.BackupWorkflowStepKind.STAGE_SOURCES in result.completed_steps
    assert api2.BackupWorkflowStepKind.SEAL_ARTIFACT in result.completed_steps
    assert api2.BackupWorkflowStepKind.VERIFY_ARTIFACT in result.completed_steps
    assert resumed.terminal


def test_backup_workflow_cancel_and_repository_are_durable_and_idempotent() -> None:
    spec = api2.BackupWorkflowSpec(
        "nightly",
        api2.BackupWorkflowKind.SQUASHFS_PACK,
        "nightly.sqsh",
    )
    workflow = _MemoryBackupWorkflow.from_spec(spec)
    cancelled = workflow.cancel()
    assert cancelled.status is api2.WorkflowStatus.CANCELLED
    assert workflow.terminal

    repository = _MemoryWorkflowRepository()
    workflow_id = repository.save_workflow_spec(spec)
    repository.save_resume_state(workflow_id, cancelled)
    loaded = repository.load_resume_state(workflow_id)
    assert loaded.workflow_id == workflow_id
    assert loaded.status is api2.WorkflowStatus.CANCELLED
    assert list(repository.iter_workflow_specs(status=api2.WorkflowStatus.CANCELLED)) == [
        (workflow_id, spec),
    ]

    artifact = api2.RegisteredBackupArtifact(
        workflow_id,
        "archive-store",
        "archive-store",
        "nightly.sqsh",
    )
    source = api2.BackupSourceSpec(api2.BackupSourceKind.LOCAL_PATH, "/books/a")
    assert repository.record_backup_presence(
        workflow_id,
        artifact,
        source,
        archive_path="books/a",
    )
    assert not repository.record_backup_presence(
        workflow_id,
        artifact,
        source,
        archive_path="books/a",
    )
    assert repository.delete_workflow(workflow_id)
    assert not repository.delete_workflow(workflow_id)


def test_backup_planning_and_registration_facades_remain_separate() -> None:
    assert api2.BackupPlannerAPI.__abstractmethods__ == {"plan_store_backup"}
    assert api2.BackupArtifactRegistryAPI.__abstractmethods__ == {
        "get_registered_artifact",
        "iter_registered_artifacts",
        "register_artifact",
    }
    assert api2.BackupWorkflowRepositoryAPI.__abstractmethods__ == {
        "delete_workflow",
        "iter_workflow_specs",
        "load_resume_state",
        "load_workflow_spec",
        "record_backup_presence",
        "record_result",
        "save_resume_state",
        "save_workflow_spec",
    }
