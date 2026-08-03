"""Bounded WEMI graph retrieval contract."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from ..common import EntityId, WemiGraph


@runtime_checkable
class WemiGraphRetrieverAPI(Protocol):
    """Return complete Work descendants within explicit transport limits.

    The result reports truncated levels so callers never mistake a bounded
    response for the Work's complete bibliographic graph.
    """

    def for_work(
        self,
        work_id: EntityId,
        *,
        max_expressions: int = 100,
        max_manifestations: int = 500,
        max_items: int = 1000,
    ) -> WemiGraph:
        """Return all selected descendants and structural graph edges."""


__all__ = ["WemiGraphRetrieverAPI"]
