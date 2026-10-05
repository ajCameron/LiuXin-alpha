"""
Define the generic checkpointable workflow boundary above storage managers.

The abstract operations separate durable intent, current state, one-unit execution,
completion, and cancellation. Only terminal has a shared implementation. Persistence,
resource lifetime, retry policy, and physical rollback remain concrete responsibilities.
"""

from __future__ import annotations

import abc

from typing import Generic, TypeVar

from LiuXin_alpha.storage.api.workflow_api.models import WorkflowStateAPI


DeclarationT = TypeVar("DeclarationT")
StateT = TypeVar("StateT", bound=WorkflowStateAPI)
ResultT = TypeVar("ResultT")


class StorageWorkflowAPI(Generic[DeclarationT, StateT, ResultT], abc.ABC):
    """
    Define intent, checkpoint inspection, and execution for a concrete storage workflow.

    DeclarationT describes intended work, StateT exposes WorkflowStatus, and ResultT is the
    implementation's terminal outcome. Subclasses supply every operation except terminal, which
    reads progress().status. The ABC neither stores checkpoints nor opens a repository transaction:
    callers or concrete orchestration arrange persistence and retain resources needed for
    reconstruction.

    A terminal status stops ordinary completion loops; FAILED may still support an explicit retry.
    The interface does not prescribe a scheduler, locking, rollback, or a uniform
    exception-to-checkpoint policy.

    Example:
        >>> def checkpoint(workflow: StorageWorkflowAPI) -> WorkflowStateAPI:
        ...     return workflow.progress()
    """

    @property
    @abc.abstractmethod
    def workflow_kind(self) -> str:
        """
        Identify the implementation family used to interpret a declaration and checkpoint.

        This describes the kind of workflow, not its individual execution ID. Concrete
        implementations may return a string-valued enum.

        Example:
            >>> kind = workflow.workflow_kind  # doctest: +SKIP


        :return: Stable family identifier supplied by the concrete workflow.
        """
        ...

    @property
    @abc.abstractmethod
    def workflow_name(self) -> str:
        """
        Expose the human-readable name of this workflow instance.

        A display name is not necessarily unique and does not replace a repository-assigned
        WorkflowID.

        Example:
            >>> name = workflow.workflow_name  # doctest: +SKIP


        :return: Name associated with this instance and its durable intent.
        """
        ...

    @abc.abstractmethod
    def build_declaration(self) -> DeclarationT:
        """
        Describe the current intended work in a value suitable for persistence.

        This method produces intent without executing it or saving it to a repository. Whether
        designation remains editable depends on the concrete workflow lifecycle.

        Example:
            >>> declaration = workflow.build_declaration()  # doctest: +SKIP


        :return: Implementation-specific declaration containing targets, sources, and execution settings.
        """
        ...

    @abc.abstractmethod
    def progress(self) -> StateT:
        """
        Describe the current execution position without advancing the workflow.

        The checkpoint is a value for inspection or persistence; returning it does not prove that it
        has been saved or that referenced staging data still exists.

        Example:
            >>> state = workflow.progress()  # doctest: +SKIP


        :return: Current state value exposing at least a WorkflowStatus through status.
        """
        ...

    @property
    def terminal(self) -> bool:
        """
        Read whether the current checkpoint stops ordinary execution.

        Calls progress() once and returns its status.terminal value without caching, mutation, or
        exception handling. FAILED is terminal even though WorkflowStatus also marks it resumable.

        Example:
            >>> done = workflow.terminal  # doctest: +SKIP


        :return: True for FAILED, COMPLETE, or CANCELLED; false for DRAFT or RUNNING.
        """
        return self.progress().status.terminal

    @abc.abstractmethod
    def run_next(self) -> StateT:
        """
        Attempt the next implementation-defined resumable unit and return its checkpoint.

        A unit may stage one source or perform several finalization operations. This is not a time
        limit or an atomic transaction boundary. Concrete workflows define terminal-state behavior
        and whether operational errors raise or become FAILED checkpoints; retrying a failure may
        require this explicit entry point.

        Example:
            >>> state = workflow.run_next()  # doctest: +SKIP


        :return: Checkpoint after the attempted unit, including any failure represented as state.
        """
        ...

    @abc.abstractmethod
    def run_to_completion(self) -> ResultT:
        """
        Drive ordinary execution until a terminal outcome can be returned.

        Terminal includes failure and cancellation, so a return does not imply success. The SquashFS
        implementation stops immediately when already FAILED; constructing it from a failed
        checkpoint does not itself retry the failed unit.

        Example:
            >>> result = workflow.run_to_completion()  # doctest: +SKIP


        :return: Concrete result describing completion, failure, or cancellation and any retained output.
        """
        ...

    @abc.abstractmethod
    def cancel(self) -> StateT:
        """
        Request that future workflow work stop and expose the resulting checkpoint.

        Cancellation does not promise to interrupt a concurrent operation, delete staged data,
        remove published bytes, or undo catalogue writes. A concrete implementation may reject
        cancellation after completion.

        Example:
            >>> state = workflow.cancel()  # doctest: +SKIP


        :return: Checkpoint recording cancellation when the implementation accepts the request.
        """
        ...


__all__ = ["StorageWorkflowAPI"]
