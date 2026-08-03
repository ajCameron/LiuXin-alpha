"""Bounded full-descendant WEMI graph retrieval."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from ..api.common import EntityId, RowMapping, WemiGraph, WemiLevel


class WemiGraphRetriever:
    """Read a Work and every descendant selected by explicit result limits."""

    def __init__(self, repositories: Any) -> None:
        self.repositories = repositories

    @staticmethod
    def _limit(name: str, value: int) -> int:
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
        """Return a bounded full descendant graph rooted at one Work."""

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
