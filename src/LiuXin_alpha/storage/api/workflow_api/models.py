"""
Share workflow lifecycle classification and the minimal checkpoint protocol.

WorkflowID aliases int without runtime validation. WorkflowStatus deliberately treats
FAILED as both terminal and resumable; the protocol supplies only a status view.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Protocol, TypeAlias, runtime_checkable


WorkflowID: TypeAlias = int


class WorkflowStatus(StrEnum):
    """
    Name lifecycle states shared by workflow checkpoints and results.

    DRAFT and RUNNING allow ordinary work. FAILED, COMPLETE, and CANCELLED are terminal, while
    DRAFT, RUNNING, and FAILED are resumable. These predicates classify values only: they do not
    validate transitions, persist state, repair failures, or guarantee a retry succeeds. WorkflowID
    is an int alias rather than a validated identifier type.

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
        """
        Classify failure, completion, and cancellation as stopping states for ordinary execution.

        Example:
            >>> WorkflowStatus.CANCELLED.terminal
            True


        :return: True for FAILED, COMPLETE, and CANCELLED; false for DRAFT and RUNNING.
        """
        return self in {
            WorkflowStatus.FAILED,
            WorkflowStatus.COMPLETE,
            WorkflowStatus.CANCELLED,
        }

    @property
    def resumable(self) -> bool:
        """
        Classify checkpoints that may be reconstructed for further execution.

        FAILED remains eligible after its cause is addressed. Eligibility does not verify retained
        staging bytes or reset the failure state of a reconstructed implementation.

        Example:
            >>> WorkflowStatus.FAILED.resumable
            True
            >>> WorkflowStatus.CANCELLED.resumable
            False


        :return: True for DRAFT, RUNNING, and FAILED; false for COMPLETE and CANCELLED.
        """
        return self in {
            WorkflowStatus.DRAFT,
            WorkflowStatus.RUNNING,
            WorkflowStatus.FAILED,
        }


@runtime_checkable
class WorkflowStateAPI(Protocol):
    """
    Describe the status property consumed by generic workflow helpers.

    This runtime-checkable protocol requires a status member for static structural typing. A runtime
    isinstance check tests member presence, not the value's type, lifecycle consistency, or
    persistence. Implementations may expose status as a property or a compatible data attribute.

    Example:
        >>> def is_done(state: WorkflowStateAPI) -> bool:
        ...     return state.status.terminal
    """

    @property
    def status(self) -> WorkflowStatus:
        """
        Return the lifecycle classification represented by this checkpoint value.

        Example:
            >>> status = state.status  # doctest: +SKIP


        :return: WorkflowStatus used by terminal and resumable decisions; accessing it does not advance work.
        """
        ...


__all__ = ["WorkflowID", "WorkflowStateAPI", "WorkflowStatus"]
