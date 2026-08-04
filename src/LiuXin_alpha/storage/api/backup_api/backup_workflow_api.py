"""Backup workflow contracts.

Backup workflows are orchestration objects that sit *above* raw store plugins.
They coordinate designation of source files/locations, staging, sealing, resume
state, and final backup artifacts. They should not be confused with raw storage
plugins or with the top-level storage manager.

Examples:
    A concrete workflow designates sources before it is run::

        workflow.designate_local_path("/books/book.epub", archive_path="book.epub")
        result = workflow.run_to_completion()
"""

from __future__ import annotations

import abc
from typing import Callable, Self

from LiuXin_alpha.storage.api.backup_api.backup_workflow_models import (
    BackupSourceSpec,
    BackupWorkflowResult,
    BackupWorkflowResumeState,
    BackupWorkflowSpec,
)
from LiuXin_alpha.storage.api.location_api import StoreLocationMixinAPI


class BackupWorkflowAPI(abc.ABC):
    """Contract for one resumable backup/export workflow.

    A workflow owns the *process* of creating a backup artifact. Raw store
    plugins own byte access. Store containers own configured stores. The
    storage manager chooses stores. Backup workflows sit beside those concerns
    and can therefore track planning, progress, and resume state explicitly.

    Examples:
        Checkpoint-friendly callers may advance one step at a time::

            state = workflow.run_next()
            saved_state = workflow.progress()
    """

    @property
    @abc.abstractmethod
    def workflow_kind(self) -> str:
        """Return the stable workflow implementation kind.

        Examples:
            Persist the kind alongside resume state::

                kind = workflow.workflow_kind
        """
        ...

    @property
    @abc.abstractmethod
    def workflow_name(self) -> str:
        """Return this workflow instance's human-readable name.

        Examples:
            Display the name in a job list::

                label = workflow.workflow_name
        """
        ...

    @abc.abstractmethod
    def build_spec(self) -> BackupWorkflowSpec:
        """Return the workflow's immutable intent/configuration object.

        Examples:
            Serialize the specification before execution::

                spec = workflow.build_spec()
        """
        ...

    @abc.abstractmethod
    def progress(self) -> BackupWorkflowResumeState:
        """Return the current durable checkpoint state.

        Examples:
            Save progress after each scheduled step::

                repository.save_resume_state(workflow_id, workflow.progress())
        """
        ...

    @abc.abstractmethod
    def designate_local_path(
        self,
        source_path: str,
        *,
        archive_path: str | None = None,
    ) -> BackupSourceSpec:
        """Add a local filesystem path to the backup source set.

        Examples:
            Rename a source inside the output artifact::

                source = workflow.designate_local_path(
                    "/incoming/a.epub", archive_path="books/a.epub"
                )
        """
        ...

    @abc.abstractmethod
    def designate_location(
        self,
        source_location: StoreLocationMixinAPI,
        *,
        archive_path: str | None = None,
    ) -> BackupSourceSpec:
        """Add a storage location to the backup source set.

        Examples:
            Preserve a managed file under a chosen archive path::

                source = workflow.designate_location(
                    location, archive_path="books/a.epub"
                )
        """
        ...

    @abc.abstractmethod
    def run_next(self) -> BackupWorkflowResumeState:
        """Advance one resumable workflow step.

        Examples:
            Run a single unit from a cooperative job loop::

                state = workflow.run_next()
        """
        ...

    @abc.abstractmethod
    def run_to_completion(self) -> BackupWorkflowResult:
        """Run remaining steps and return the terminal result.

        Examples:
            Execute a small export synchronously::

                result = workflow.run_to_completion()
                assert result.output_artifact_url is not None
        """
        ...

    @abc.abstractmethod
    def cancel(self) -> BackupWorkflowResumeState:
        """Mark the workflow cancelled and return its final checkpoint.

        Examples:
            Preserve cancellation state for later inspection::

                cancelled_state = workflow.cancel()
        """
        ...

    @classmethod
    @abc.abstractmethod
    def from_resume_state(
        cls,
        resume_state: BackupWorkflowResumeState,
        *,
        location_loader: Callable[[str], StoreLocationMixinAPI] | None = None,
    ) -> Self:
        """Reconstruct a concrete workflow from a durable checkpoint.

        Examples:
            Resume a workflow whose sources are storage URLs::

                workflow = ConcreteWorkflow.from_resume_state(
                    state, location_loader=manager.locate_file
                )
        """
        ...
