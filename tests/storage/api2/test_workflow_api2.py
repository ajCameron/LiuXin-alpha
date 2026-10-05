"""
Check workflow API composition, value validation, and simulated state transitions.

The memory workflow and repository are deliberately local test doubles. They
retain references and synthesize staging/verification labels without reading
sources, creating archives, or persisting across processes. Export and abstract
method checks establish declared interface shape rather than backend behavior.
"""

from __future__ import annotations

import dataclasses

from collections.abc import Iterator
from uuid import UUID

import pytest

import LiuXin_alpha.storage.api as api
import LiuXin_alpha.storage.utils.workflow as storage_utils


PRIMARY_STORE_UUID = UUID("00000000-0000-0000-0000-000000000001")
ARCHIVE_STORE_UUID = UUID("00000000-0000-0000-0000-000000000002")


class _MemoryBackupWorkflow(api.BackupWorkflowAPI):
    """
    Simulate backup checkpoint transitions without reading sources or constructing an artifact.
    Declarations/checkpoints are retained frozen values; progress replaces the checkpoint reference.
    Staging reports copy expected size/digest-presence facts instead of measuring bytes. There is no
    persistence, backend access, resource cleanup, or locking in this API test double.

    Example:
        >>> workflow = _MemoryBackupWorkflow(api.BackupWorkflowDeclaration(
        ...     "nightly", api.BackupWorkflowKind.SQUASHFS_PACK, "out.sqsh",
        ... ))
        >>> workflow.progress().status is api.WorkflowStatus.DRAFT
        True
    """
    def __init__(
        self,
        declaration: api.BackupWorkflowDeclaration,
        *,
        checkpoint: api.BackupWorkflowCheckpoint | None = None,
    ) -> None:
        """
        Retain the declaration and a truthy supplied checkpoint, otherwise create a DRAFT checkpoint
        for that declaration. No check reconciles a supplied checkpoint's declaration with the
        separate declaration argument.

        Example:
            >>> workflow = _MemoryBackupWorkflow(declaration)  # doctest: +SKIP


        :param declaration: Backup intent retained directly for name, kind, sources, and completion.
        :param checkpoint: Optional checkpoint retained directly when truthy; absence creates draft progress.
        :return: None after assigning the two retained references.
        """
        self._declaration = declaration
        self._checkpoint = checkpoint or api.BackupWorkflowCheckpoint(
            declaration,
            api.WorkflowStatus.DRAFT,
        )

    @property
    def workflow_kind(self) -> api.BackupWorkflowKind:
        """
        Read workflow_kind from the retained declaration without inspecting checkpoint state or an
        output artifact.

        Example:
            >>> kind = workflow.workflow_kind  # doctest: +SKIP


        :return: The declaration's BackupWorkflowKind value.
        """
        return self._declaration.workflow_kind

    @property
    def workflow_name(self) -> str:
        """
        Read workflow_name from the retained declaration without generating or normalizing another
        name.

        Example:
            >>> name = workflow.workflow_name  # doctest: +SKIP


        :return: The declaration's workflow-name string.
        """
        return self._declaration.workflow_name

    def build_declaration(self) -> api.BackupWorkflowDeclaration:
        """
        Return the current declaration reference, including any sources appended by designation. No
        defensive copy, persistence, or backend discovery occurs.

        Example:
            >>> intent = workflow.build_declaration()  # doctest: +SKIP


        :return: The retained BackupWorkflowDeclaration object.
        """
        return self._declaration

    def progress(self) -> api.BackupWorkflowCheckpoint:
        """
        Return the current checkpoint reference without advancing the simulation or inspecting
        external state.

        Example:
            >>> checkpoint = workflow.progress()  # doctest: +SKIP


        :return: The retained BackupWorkflowCheckpoint object.
        """
        return self._checkpoint

    def _add_source(self, source: api.BackupSourceDeclaration) -> api.BackupSourceDeclaration:
        """
        Require exact DRAFT status, append the source through declaration replacement, then replace
        the checkpoint's declaration. Model validation may reject duplicate paths; a later
        checkpoint replacement error can follow an already replaced declaration.

        Example:
            >>> added = workflow._add_source(source)  # doctest: +SKIP


        :param source: Source declaration appended without probing or reading its identifier.
        :return: The same source object after both replacements; non-draft state raises StorePreconditionFailed and model errors propagate.
        """
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
        """
        Build a LOCAL_PATH source declaration and append it through the draft-only helper. A falsey
        archive_path becomes source-N using the current source count. The local path is retained as
        intent without filesystem access.

        Example:
            >>> source = workflow.designate_local_path("/books/a.epub", archive_path="books/a.epub")  # doctest: +SKIP


        :param source_path: Local source-path text passed into the source declaration.
        :param archive_path: Truthy archive member path, or None/empty text to use the count-based source-N default.
        :return: Appended BackupSourceDeclaration; model validation and non-draft errors propagate.
        """
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
        """
        Build a STORE_LOCATION source declaration and append it through the draft-only helper. A
        falsey archive_path becomes source-N. Source-model validation checks the Location type; no
        Store lookup or read occurs.

        Example:
            >>> source = workflow.designate_location(location, archive_path="books/a.epub")  # doctest: +SKIP


        :param source_location: Opaque Location retained as the source identifier.
        :param archive_path: Truthy archive member path or a falsey value selecting source-N from the current source count.
        :return: Appended BackupSourceDeclaration; validation or non-draft errors propagate.
        """
        chosen_path = archive_path or f"source-{len(self._declaration.sources)}"
        return self._add_source(
            api.BackupSourceDeclaration(
                api.BackupSourceKind.STORE_LOCATION,
                source_location,
                archive_path=chosen_path,
            )
        )

    def run_next(self) -> api.BackupWorkflowCheckpoint:
        """
        Advance one synthetic source report or mark the simulated artifact complete. Return the
        existing checkpoint unchanged when its status is terminal. Otherwise stage the next declared
        source by copying expected_size and setting digest_verified solely from expected_digest
        presence. Increment progress and add STAGE_SOURCES when the last source is consumed.

        Once no source remains, append SEAL_ARTIFACT and optionally VERIFY_ARTIFACT, set COMPLETE,
        and copy the output target into the artifact reference. An empty-source workflow reaches
        this phase without a STAGE_SOURCES marker. No actual staging, digest verification, sealing,
        or output file exists.

        Example:
            >>> checkpoint = workflow.run_next()  # doctest: +SKIP


        :return: The retained or newly replaced checkpoint; model/shape errors propagate.
        """
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
        """
        Call run_next until status is terminal, then assemble a result from retained
        declaration/checkpoint fields. An already failed or cancelled terminal checkpoint returns
        that status without retrying or resetting progress; no artifact is built.

        Example:
            >>> result = workflow.run_to_completion()  # doctest: +SKIP


        :return: BackupWorkflowResult describing simulated terminal state, including the final checkpoint reference.
        """
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
        """
        Replace checkpoint status with CANCELLED while retaining its other fields. This can relabel
        an already terminal checkpoint and does not undo reports, remove artifact references, or
        cancel external work.

        Example:
            >>> checkpoint = workflow.cancel()  # doctest: +SKIP


        :return: The newly retained CANCELLED checkpoint; model errors propagate.
        """
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
        """
        Construct cls from the declaration using default draft progress. The optional
        storage_manager is accepted for interface compatibility but ignored; no service is bound.

        Example:
            >>> workflow = _MemoryBackupWorkflow.from_declaration(declaration)  # doctest: +SKIP


        :param declaration: Backup intent passed unchanged to cls.
        :param storage_manager: Ignored manager argument; this double does not route Store operations.
        :return: New workflow initialized from the declaration.
        """
        return cls(declaration)

    @classmethod
    def from_checkpoint(
        cls,
        checkpoint: api.BackupWorkflowCheckpoint,
        *,
        storage_manager=None,
    ) -> _MemoryBackupWorkflow:
        """
        Require a status whose resumable property is true, then construct cls with the checkpoint's
        declaration and the same checkpoint reference. Status is not changed, so a resumable
        terminal failure remains terminal in this simulation.

        Example:
            >>> resumed = _MemoryBackupWorkflow.from_checkpoint(checkpoint)  # doctest: +SKIP


        :param checkpoint: Progress value whose resumable status is checked before construction.
        :param storage_manager: Ignored manager argument retained for interface compatibility.
        :return: New workflow retaining the supplied checkpoint, or StorePreconditionFailed for non-resumable status.
        """
        if not checkpoint.status.resumable:
            raise api.StorePreconditionFailed("workflow state is not resumable")
        return cls(checkpoint.declaration, checkpoint=checkpoint)


class _MemoryWorkflowRepository(api.BackupWorkflowRepositoryAPI):
    """
    Hold workflow test values in process-local dictionaries and a presence-key set. Each instance
    starts empty. Saving retains values without serialization, locking, transactions, or durable
    storage. Presence keys ignore registration and protection flags and survive workflow deletion.
    These deliberately narrow semantics exercise the public repository interface only.

    Example:
        >>> repository = _MemoryWorkflowRepository()
        >>> list(repository.iter_workflow_declarations())
        []
    """
    def __init__(self) -> None:
        """
        Create fresh declaration, checkpoint, status, and result dictionaries plus an empty presence
        set. No database, artifact registry, or shared state is attached.

        Example:
            >>> repository = _MemoryWorkflowRepository()
            >>> repository.presence
            set()


        :return: None after initializing five independent containers.
        """
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
        """
        Choose a truthy supplied ID or max existing declaration ID plus one, then retain declaration
        and status under it. Existing values are overwritten; ID shape, status type, and consistency
        with prior checkpoints/results are not checked.

        Example:
            >>> workflow_id = repository.save_workflow_declaration(declaration)  # doctest: +SKIP


        :param declaration: Declaration object retained without copying or serialization.
        :param workflow_id: Optional truthy mapping key; None or zero chooses the next computed ID.
        :param status: Status reference written alongside the declaration, defaulting to DRAFT.
        :return: Chosen ID/key; mixed or incompatible existing ID values can fail max/addition.
        """
        chosen_id = workflow_id or (max(self.declarations, default=0) + 1)
        self.declarations[chosen_id] = declaration
        self.statuses[chosen_id] = status
        return chosen_id

    def load_workflow_declaration(self, workflow_id):
        """
        Index the declaration dictionary and return its exact retained value without copying or
        consulting status.

        Example:
            >>> declaration = repository.load_workflow_declaration(workflow_id)  # doctest: +SKIP


        :param workflow_id: Declaration dictionary key to look up.
        :return: Stored declaration object; a missing key raises KeyError.
        """
        return self.declarations[workflow_id]

    def iter_workflow_declarations(self, *, status=None):
        """
        Sort declaration keys when iteration first advances, then read current status/value entries
        for each key. A supplied status filters by object identity. Later value changes are visible
        and removals can raise; this is not a coherent snapshot.

        Example:
            >>> rows = list(repository.iter_workflow_declarations(status=api.WorkflowStatus.DRAFT))  # doctest: +SKIP


        :param status: Optional status object compared with is; None includes every captured key.
        :return: Iterator of (workflow ID, retained declaration) pairs in sorted key order; lookup/sort failures propagate on advancement.
        """
        for workflow_id in sorted(self.declarations):
            if status is None or self.statuses[workflow_id] is status:
                yield workflow_id, self.declarations[workflow_id]

    def save_checkpoint(self, workflow_id, checkpoint) -> None:
        """
        Clone the checkpoint with the supplied workflow ID, store it, and copy its status into the
        status dictionary. No existing declaration is required and no declaration/result consistency
        is checked.

        Example:
            >>> repository.save_checkpoint(workflow_id, checkpoint)  # doctest: +SKIP


        :param workflow_id: Dictionary key also assigned to the replacement checkpoint.
        :param checkpoint: Progress value cloned through dataclasses.replace.
        :return: None after checkpoint/status writes; model validation errors occur before assignment.
        """
        self.checkpoints[workflow_id] = dataclasses.replace(
            checkpoint,
            workflow_id=workflow_id,
        )
        self.statuses[workflow_id] = checkpoint.status

    def load_checkpoint(self, workflow_id):
        """
        Return an existing checkpoint directly, otherwise synthesize one from the stored declaration
        and status without saving it. Missing fallback declaration/status entries raise KeyError.

        Example:
            >>> checkpoint = repository.load_checkpoint(workflow_id)  # doctest: +SKIP


        :param workflow_id: Key selecting saved progress or fallback declaration/status.
        :return: Retained checkpoint or a new unsaved fallback checkpoint; model/lookup errors propagate.
        """
        if workflow_id in self.checkpoints:
            return self.checkpoints[workflow_id]
        return api.BackupWorkflowCheckpoint(
            self.declarations[workflow_id],
            self.statuses[workflow_id],
            workflow_id=workflow_id,
        )

    def record_result(self, workflow_id, result) -> None:
        """
        Retain the exact result under the supplied key and update status from result.status. Do not
        rewrite its internal workflow ID, require a declaration, or update a saved checkpoint.

        Example:
            >>> repository.record_result(workflow_id, result)  # doctest: +SKIP


        :param workflow_id: Dictionary key used independently of the result's own workflow ID.
        :param result: Result reference retained without copying or serialization.
        :return: None after result/status assignment; malformed result attributes can fail after the first write.
        """
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
        """
        Deduplicate the tuple of workflow ID, str(source.source_identifier), and archive_path in the
        presence set. Ignore registration, protected, and immutable; add no path normalization,
        Store lookup, or actual protection record.

        Example:
            >>> added = repository.record_backup_presence(workflow_id, registration, source, archive_path="books/a")  # doctest: +SKIP


        :param workflow_id: First component of the presence identity, without declaration lookup.
        :param registration: Ignored artifact-registration argument.
        :param source: Source whose identifier is stringified into the presence key.
        :param archive_path: Exact hashable archive path used as the third key component.
        :param protected: Ignored protection flag retained for interface compatibility.
        :param immutable: Ignored immutability flag retained for interface compatibility.
        :return: True for a newly inserted key, False for a duplicate; key conversion/hashing errors propagate.
        """
        key = workflow_id, str(source.source_identifier), archive_path
        if key in self.presence:
            return False
        self.presence.add(key)
        return True

    def delete_workflow(self, workflow_id, *, require_terminal=True) -> bool:
        """
        Return False for an absent declaration; otherwise optionally require terminal stored status
        and remove declaration/checkpoint/status/result entries. Presence keys remain. The sequence
        has no rollback, so inconsistent dictionaries can fail after partial removal.

        Example:
            >>> removed = repository.delete_workflow(workflow_id)  # doctest: +SKIP


        :param workflow_id: Key whose declaration presence decides whether deletion is attempted.
        :param require_terminal: Whether to reject nonterminal status before removing workflow dictionaries.
        :return: True after removal or False for absent declaration; nonterminal status raises StorePreconditionFailed and inconsistent state can raise KeyError.
        """
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
    """
    Check workflow export identity, unique/resolvable __all__, BackupWorkflowAPI ancestry, and its
    exact abstract-method set. Instantiating that incomplete abstract API must raise TypeError; no
    workflow runs here.

    Example:
        >>> test_workflow_package_is_segregated_explicit_and_layered()  # doctest: +SKIP


    :return: None after the interface/export assertions pass.
    """
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
    """
    Check source path normalization, Location projections, sorted options, status predicates, and
    selected validation errors. Wrong source types, parent traversal, duplicate member paths, and
    nonterminal results reject without any byte I/O.

    Example:
        >>> test_backup_models_validate_paths_sources_checkpoints_and_results()  # doctest: +SKIP


    :return: None after the value-model assertions pass.
    """
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
        options=(("deterministic", "1"), ("compression", "zstd")),
    )

    assert source.archive_path == "books/novel.epub"
    assert source.location == location
    assert source.source_store_ref == PRIMARY_STORE_UUID
    assert declaration.options == (
        ("compression", "zstd"),
        ("deterministic", "1"),
    )
    assert declaration.option_map() == {
        "compression": "zstd",
        "deterministic": "1",
    }
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
    """
    Designate two synthetic sources, advance one checkpoint, construct a second memory workflow from
    it, and run to terminal progress. Assert counts, artifact reference, and stage/seal/verify
    labels; these labels do not represent an actual archive or digest verification.

    Example:
        >>> test_backup_workflow_runs_checkpoints_resumes_and_completes()  # doctest: +SKIP


    :return: None after the simulated transition assertions pass.
    """
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
    """
    Cancel a memory workflow, save/load its checkpoint, filter declarations, deduplicate a presence
    key, and delete the workflow twice. All values remain inside one repository instance; despite
    the historical name, this tests no durable database or process restart.

    Example:
        >>> test_backup_workflow_cancel_and_repository_are_durable_and_idempotent()  # doctest: +SKIP


    :return: None after cancellation and in-memory repository assertions pass.
    """
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
    """
    Require the exact abstract-method sets for planner, artifact registry, and workflow repository
    APIs. This protects declared interface responsibilities without constructing concrete
    implementations.

    Example:
        >>> test_backup_planning_and_registration_facades_remain_separate()  # doctest: +SKIP


    :return: None after the three abstract interface assertions pass.
    """
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
