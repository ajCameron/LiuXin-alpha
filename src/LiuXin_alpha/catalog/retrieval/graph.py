"""
Retrieve Work descendants with explicit result bounds and truncation metadata.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from ..api.common import EntityId, RowMapping, WemiGraph, WemiLevel


class WemiGraphRetriever:
    """
    Traverse selected descendants while retaining their structural edges.

    Example:
        Request a graph with ``max_items=10`` to limit returned Items; inspect
        ``truncated_levels`` before treating it as complete.
    """

    def __init__(self, repositories: Any) -> None:
        """
        Retain the supplied repositories without querying or validating them.

        Example:
            Construct the service once when composing a Catalog; later calls use the same group.


        :param repositories: Repository group used by later reads.
        :return: None; stores the borrowed reference.
        """

        self.repositories = repositories

    @staticmethod
    def _limit(name: str, value: int) -> int:
        """
        Validate one nonnegative integer result bound.

        Example:
            >>> WemiGraphRetriever._limit("max_items", 0)
            0


        :param name: Limit name used in error messages.
        :param value: Value to validate; bool is explicitly rejected.
        :return: The unchanged valid integer.
        :raises TypeError: Value is not an integer or is bool.
        :raises ValueError: Value is negative.
        """

        if not isinstance(value, int) or isinstance(value, bool):
            raise TypeError(f"{name} must be an integer")
        if value < 0:
            raise ValueError(f"{name} cannot be negative")
        return value

    @staticmethod
    def _deduplicate(
        rows: Iterable[RowMapping],
        id_column: str,
    ) -> tuple[RowMapping, ...]:
        """
        Keep the first row for each ID, preserving encounter order.

        Missing IDs share the key None and collapse into one row. Unhashable IDs raise TypeError.

        Example:
            >>> WemiGraphRetriever._deduplicate([{"id": 1}, {"id": 1}], "id")
            ({'id': 1},)


        :param rows: Rows to consume without copying their mappings.
        :param id_column: Column whose hashable value identifies a row.
        :return: Tuple of retained original mappings.
        """

        result: list[RowMapping] = []
        seen: set[object] = set()
        for row in rows:
            row_id = row.get(id_column)
            if row_id in seen:
                continue
            seen.add(row_id)
            result.append(row)
        return tuple(result)

    @staticmethod
    def _edge(
        *,
        parent_level: WemiLevel,
        parent_id: EntityId,
        child_level: WemiLevel,
        child_id: EntityId,
        metadata: Mapping[str, object] | None = None,
    ) -> RowMapping:
        """
        Build a structural edge with a shallow copy of its metadata.

        Example:
            A Manifestation-to-Item edge uses ``metadata={"storage": "foreign_key"}``.


        :param parent_level: Parent WEMI level; not validated here.
        :param parent_id: Parent ID; not validated here.
        :param child_level: Child WEMI level; not validated here.
        :param child_id: Child ID; not validated here.
        :param metadata: Optional mapping; None becomes an empty dictionary.
        :return: New edge dictionary with parent/child levels, IDs and metadata.
        """

        return {
            "parent_level": parent_level,
            "parent_id": parent_id,
            "child_level": child_level,
            "child_id": child_id,
            "metadata": dict(metadata or {}),
        }

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

        max_expressions = self._limit("max_expressions", max_expressions)
        max_manifestations = self._limit(
            "max_manifestations",
            max_manifestations,
        )
        max_items = self._limit("max_items", max_items)
        work = self.repositories.works.require(work_id)
        truncated: set[WemiLevel] = set()

        all_expressions = tuple(
            self.repositories.expressions.list_for_work(work_id)
        )
        expressions = all_expressions[:max_expressions]
        if len(all_expressions) > len(expressions):
            truncated.update(("expression", "manifestation", "item"))

        expression_edges: list[RowMapping] = []
        manifestation_rows: list[RowMapping] = []
        manifestation_edges: list[RowMapping] = []
        for expression in expressions:
            expression_id = int(expression["expression_id"])
            link = expression.get("_catalog_link")
            expression_edges.append(
                self._edge(
                    parent_level="work",
                    parent_id=work_id,
                    child_level="expression",
                    child_id=expression_id,
                    metadata=link if isinstance(link, Mapping) else None,
                )
            )
            for manifestation in (
                self.repositories.manifestations.list_for_expression(
                    expression_id
                )
            ):
                manifestation_rows.append(manifestation)
                manifestation_id = int(manifestation["manifestation_id"])
                link = manifestation.get("_catalog_link")
                manifestation_edges.append(
                    self._edge(
                        parent_level="expression",
                        parent_id=expression_id,
                        child_level="manifestation",
                        child_id=manifestation_id,
                        metadata=link if isinstance(link, Mapping) else None,
                    )
                )

        all_manifestations = self._deduplicate(
            manifestation_rows,
            "manifestation_id",
        )
        manifestations = all_manifestations[:max_manifestations]
        selected_manifestation_ids = {
            row["manifestation_id"] for row in manifestations
        }
        manifestation_edges = [
            edge
            for edge in manifestation_edges
            if edge["child_id"] in selected_manifestation_ids
        ]
        if len(all_manifestations) > len(manifestations):
            truncated.update(("manifestation", "item"))

        item_rows: list[RowMapping] = []
        item_edges: list[RowMapping] = []
        for manifestation in manifestations:
            manifestation_id = int(manifestation["manifestation_id"])
            for item in self.repositories.items.list_for_manifestation(
                manifestation_id
            ):
                item_rows.append(item)
                item_edges.append(
                    self._edge(
                        parent_level="manifestation",
                        parent_id=manifestation_id,
                        child_level="item",
                        child_id=int(item["item_id"]),
                        metadata={"storage": "foreign_key"},
                    )
                )
        all_items = self._deduplicate(item_rows, "item_id")
        items = all_items[:max_items]
        selected_item_ids = {row["item_id"] for row in items}
        item_edges = [
            edge for edge in item_edges if edge["child_id"] in selected_item_ids
        ]
        if len(all_items) > len(items):
            truncated.add("item")

        level_order: tuple[WemiLevel, ...] = (
            "work",
            "expression",
            "manifestation",
            "item",
        )
        return WemiGraph(
            work=work,
            expressions=tuple(expressions),
            manifestations=tuple(manifestations),
            items=tuple(items),
            links=tuple(
                (*expression_edges, *manifestation_edges, *item_edges)
            ),
            truncated_levels=tuple(
                level for level in level_order if level in truncated
            ),
        )


__all__ = ["WemiGraphRetriever"]
