"""Generic resumable storage-workflow contract."""

from __future__ import annotations

import abc

from typing import Generic, TypeVar

from LiuXin_alpha.storage.api.workflow_api.models import WorkflowStateAPI


SpecT = TypeVar("SpecT")
StateT = TypeVar("StateT", bound=WorkflowStateAPI)
ResultT = TypeVar("ResultT")


class StorageWorkflowAPI(Generic[SpecT, StateT, ResultT], abc.ABC):
    """Base contract for one explicit, checkpointable storage workflow.

    Workflows orchestrate managers and stores.  They do not implement driver
    mechanics, store routing, or database row access themselves.

    Example:
        >>> def checkpoint(workflow: StorageWorkflowAPI) -> WorkflowStateAPI:
        ...     return workflow.progress()
    """

    @property
    @abc.abstractmethod
    def workflow_kind(self) -> str:
        """Return the stable implementation or workflow-family identifier.

        Example:
            >>> kind = workflow.workflow_kind  # doctest: +SKIP
        """
        ...

    @property
    @abc.abstractmethod
    def workflow_name(self) -> str:
        """Return this workflow instance's human-readable name.

        Example:
            >>> name = workflow.workflow_name  # doctest: +SKIP
        """
        ...

    @abc.abstractmethod
    def build_spec(self) -> SpecT:
        """Return immutable durable intent for this workflow.

        Example:
            >>> spec = workflow.build_spec()  # doctest: +SKIP
        """
        ...

    @abc.abstractmethod
    def progress(self) -> StateT:
        """Return the current durable checkpoint state.

        Example:
            >>> state = workflow.progress()  # doctest: +SKIP
        """
        ...

    @property
    def terminal(self) -> bool:
        """Return whether the current checkpoint is terminal.

        Example:
            >>> done = workflow.terminal  # doctest: +SKIP
        """
        return self.progress().status.terminal

    @abc.abstractmethod
    def run_next(self) -> StateT:
        """Advance at most one resumable unit and return the new checkpoint.

        Example:
            >>> state = workflow.run_next()  # doctest: +SKIP
        """
        ...

    @abc.abstractmethod
    def run_to_completion(self) -> ResultT:
        """Run remaining units and return a terminal result.

        Example:
            >>> result = workflow.run_to_completion()  # doctest: +SKIP
        """
        ...

    @abc.abstractmethod
    def cancel(self) -> StateT:
        """Cancel future work and return the terminal checkpoint.

        Example:
            >>> state = workflow.cancel()  # doctest: +SKIP
        """
        ...


__all__ = ["StorageWorkflowAPI"]
