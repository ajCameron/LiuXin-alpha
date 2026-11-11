"""Immutable, value-free query observations shared by the wrapper API and hooks."""

from collections.abc import Callable
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class QueryObservation:
    """Operation duration and result count, excluding SQL, bindings and row values.

    rows_read is unknown for writes and failures, never an affected-row estimate.
    Duration includes execution/fetching and excludes observer work.
    """

    operation: str
    duration_seconds: float
    rows_read: int | None
    exception_type: str | None = None


type QueryObserver = Callable[[QueryObservation], None]
