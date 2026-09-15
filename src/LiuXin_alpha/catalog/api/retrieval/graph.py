"""
Define bounded Work graph retrieval and truncation reporting.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from ..common import EntityId, WemiGraph


@runtime_checkable
class WemiGraphRetrieverAPI(Protocol):
    """
    Describe descendant selection within explicit output limits.

    Example:
        Inspect ``graph.truncated_levels`` to distinguish a bounded selection from
        a complete traversal; the limits do not cap rows read from storage.
    """

    def for_work(
        self,
        work_id: EntityId,
        *,
        max_expressions: int = 100,
        max_manifestations: int = 500,
        max_items: int = 1000,
    ) -> WemiGraph:
        """
        Read a Work and descendants selected by per-level result limits.

        Limits bound output, not database reads: each visited parent materializes its
        children before slicing. Expressions keep repository order; Manifestations and
        Items keep their first row per ID. Edges to discarded children are removed,
        but distinct parent relationships remain. Trimming a level marks descendants
        as truncated too, even without visiting omitted branches. Reads do not share
        a snapshot transaction. Zero limits are allowed.

        Example:
            With ``max_expressions=0``, an existing Work still appears; if it has
            Expressions, Expression, Manifestation and Item are all marked truncated.


        :param work_id: Existing Work ID; required after all limits are validated.
        :param max_expressions: Maximum Expression rows returned; nonnegative integer excluding bool.
        :param max_manifestations: Maximum distinct Manifestation rows returned; nonnegative integer excluding bool.
        :param max_items: Maximum distinct Item rows returned; nonnegative integer excluding bool.
        :return: Graph with retained edges and truncated levels in WEMI order.
        :raises TypeError: Any limit is not an integer or is bool.
        :raises ValueError: Any limit is negative.
        """


__all__ = ["WemiGraphRetrieverAPI"]
