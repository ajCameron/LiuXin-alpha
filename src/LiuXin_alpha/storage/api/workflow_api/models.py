"""Values shared by resumable storage workflows."""

from __future__ import annotations

from enum import StrEnum
from typing import Protocol, TypeAlias, runtime_checkable


WorkflowID: TypeAlias = int


class WorkflowStatus(StrEnum):
    """Durable lifecycle state for one workflow execution.

    Example:
        >>> WorkflowStatus.COMPLETE.terminal
        True
        >>> WorkflowStatus.FAILED.resumable
        True
    """

    DRAFT = "draft"
    RUNNING = "running"
    FAILED = "failed"
    COMPLETE = "complete"
    CANCELLED = "cancelled"

    @property
    def terminal(self) -> bool:
        """Return whether ordinary execution must stop at this state.

        Example:
            >>> WorkflowStatus.CANCELLED.terminal
            True
        """
        return self in {
            WorkflowStatus.FAILED,
            WorkflowStatus.COMPLETE,
            WorkflowStatus.CANCELLED,
        }

    @property
    def resumable(self) -> bool:
        """Return whether a checkpoint may be reconstructed and continued.

        Failed workflows are resumable after their cause is corrected.

        Example:
            >>> WorkflowStatus.FAILED.resumable
            True
            >>> WorkflowStatus.CANCELLED.resumable
            False
        """
        return self in {
            WorkflowStatus.DRAFT,
            WorkflowStatus.RUNNING,
            WorkflowStatus.FAILED,
        }


@runtime_checkable
class WorkflowStateAPI(Protocol):
    """Structural status view required by generic workflow helpers.

    Example:
        >>> def is_done(state: WorkflowStateAPI) -> bool:
        ...     return state.status.terminal
    """

    @property
    def status(self) -> WorkflowStatus:
        """Return the durable lifecycle state represented by this checkpoint.

        Example:
            >>> status = state.status  # doctest: +SKIP
        """
        ...


__all__ = ["WorkflowID", "WorkflowStateAPI", "WorkflowStatus"]
