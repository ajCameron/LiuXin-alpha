"""Coherent WEMI bundle retrieval."""

from __future__ import annotations

from typing import Any, Iterable, Mapping

from ..api.common import DatabaseHandle, EntityId, RowMapping, WemiBundle, WemiLevel


class BundleRetriever:
    """Read coherent WEMI slices for catalog consumers."""

    def __init__(self, db: DatabaseHandle, repositories: Any) -> None:
        self.db = db
        self.repositories = repositories

    def for_item(self, item_id: EntityId) -> WemiBundle:
        """Return the WEMI path containing one Item."""

        item = self.repositories.items.require(item_id)
        manifestation = self.repositories.items.manifestation_for_item(item_id)
        expression = None
        work = None
        if manifestation is not None:
            expression = self._first(
                self.repositories.manifestations.list_expressions(
                    manifestation["manifestation_id"]
                )
            )
        if expression is not None:
            work = self._first(
                self.repositories.expressions.list_works(expression["expression_id"])
            )
        return self._assemble(
            work=work,
            expression=expression,
            manifestation=manifestation,
            item=item,
        )

    def for_manifestation(self, manifestation_id: EntityId) -> WemiBundle:
        """Return one deterministic WEMI path through a Manifestation."""

        manifestation = self.repositories.manifestations.require(manifestation_id)
        expression = self._first(
            self.repositories.manifestations.list_expressions(manifestation_id)
        )
        work = None
        if expression is not None:
            work = self._first(
                self.repositories.expressions.list_works(expression["expression_id"])
            )
        item = self._first(self.repositories.items.list_for_manifestation(manifestation_id))
        return self._assemble(
            work=work,
            expression=expression,
            manifestation=manifestation,
            item=item,
        )

    def for_expression(self, expression_id: EntityId) -> WemiBundle:
        """Return one deterministic WEMI path through an Expression."""

        expression = self.repositories.expressions.require(expression_id)
        work = self._first(self.repositories.expressions.list_works(expression_id))
        manifestation = self._first(
            self.repositories.manifestations.list_for_expression(expression_id)
        )
        item = None
        if manifestation is not None:
            item = self._first(
                self.repositories.items.list_for_manifestation(
                    manifestation["manifestation_id"]
                )
            )
        return self._assemble(
            work=work,
            expression=expression,
            manifestation=manifestation,
            item=item,
        )

    def for_work(self, work_id: EntityId) -> WemiBundle:
        """Return one deterministic WEMI path through a Work."""

        work = self.repositories.works.require(work_id)
        expression = self._first(self.repositories.expressions.list_for_work(work_id))
        manifestation = None
        item = None
        if expression is not None:
            manifestation = self._first(
                self.repositories.manifestations.list_for_expression(
                    expression["expression_id"]
                )
            )
        if manifestation is not None:
            item = self._first(
                self.repositories.items.list_for_manifestation(
                    manifestation["manifestation_id"]
                )
            )
        return self._assemble(
            work=work,
            expression=expression,
            manifestation=manifestation,
            item=item,
        )

    @staticmethod
    def _first(rows: Iterable[RowMapping]) -> RowMapping | None:
        return next(iter(rows), None)

    @staticmethod
    def _deduplicate(rows: Iterable[RowMapping], id_column: str) -> tuple[RowMapping, ...]:
        result: list[RowMapping] = []
        seen: set[object] = set()
        for row in rows:
            row_id = row.get(id_column)
            if row_id in seen:
                continue
            seen.add(row_id)
            result.append(row)
        return tuple(result)

    def _assemble(
        self,
        *,
        work: RowMapping | None,
        expression: RowMapping | None,
        manifestation: RowMapping | None,
        item: RowMapping | None,
    ) -> WemiBundle:
        levels: tuple[tuple[WemiLevel, RowMapping | None], ...] = (
            ("work", work),
            ("expression", expression),
            ("manifestation", manifestation),
            ("item", item),
        )
        agents: list[RowMapping] = []
        identifiers: list[RowMapping] = []
        titles: list[RowMapping] = []
        notes: list[RowMapping] = []
        links: list[Mapping[str, object]] = []
        for level, row in levels:
            if row is None:
                continue
            entity_id = row[f"{level}_id"]
            agents.extend(
                self.repositories.agents.list_for_wemi(
                    level=level,
                    entity_id=entity_id,
                )
            )
            identifiers.extend(
                self.repositories.identifiers.list_for_wemi(
                    level=level,
                    entity_id=entity_id,
                )
            )
            titles.extend(
                self.repositories.titles.list_for_wemi(
                    level=level,
                    entity_id=entity_id,
                )
            )
            notes.extend(
                self.repositories.notes.list_for_wemi(
                    level=level,
                    entity_id=entity_id,
                )
            )
            link = row.get("_catalog_link")
            if isinstance(link, Mapping):
                links.append({"level": level, **link})
        return WemiBundle(
            work=work,
            expression=expression,
            manifestation=manifestation,
            item=item,
            agents=self._deduplicate(agents, "agent_id"),
            identifiers=self._deduplicate(
                identifiers,
                "entity_identifier_id",
            ),
            titles=tuple(titles),
            notes=self._deduplicate(notes, "note_id"),
            links=tuple(links),
        )
