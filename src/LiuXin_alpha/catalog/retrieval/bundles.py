"""
Assemble one selected WEMI path and its attached metadata.
"""

from __future__ import annotations

from typing import Any, Iterable, Mapping

from ..api.common import DatabaseHandle, EntityId, RowMapping, WemiBundle, WemiLevel


class BundleRetriever:
    """
    Combine repository reads into a path with optional WEMI levels.

    Each branch follows the first repository-ordered relationship. This is neither
    a full descendant graph nor a transactional snapshot.

    Example:
        Read ``catalog.retrieval.bundles.for_item(item_id)`` to collect the Item's path and attachments.
    """

    def __init__(self, db: DatabaseHandle, repositories: Any) -> None:
        """
        Retain the database and repository group for subsequent reads.

        Example:
            A service constructed for a Catalog shares that Catalog's database and repositories.


        :param db: Borrowed database handle; not opened or closed here.
        :param repositories: Repository group used by later reads.
        :return: None; retains both references.
        """

        self.db = db
        self.repositories = repositories

    def for_item(self, item_id: EntityId) -> WemiBundle:
        """
        Read one WEMI path through an existing Item.

        Follow the Item's Manifestation, then the first linked Expression and Work. Repository ordering determines each first choice. Missing relationships leave
        levels as None. Metadata collection and path reads do not share a snapshot transaction.
        Missing owners and database errors propagate.

        Example:
            For an existing Item, call ``catalog.retrieval.bundles.for_item(item_id)``;
            check optional ancestor and descendant rows before reading their columns.


        :param item_id: Existing Item ID; validation is delegated to its repository.
        :return: Bundle containing the requested row, available path rows, and their attached metadata.
        """

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
        """
        Read one WEMI path through an existing Manifestation.

        Choose the first linked Expression, its first Work, and this Manifestation's first Item. Repository ordering determines each first choice. Missing relationships leave
        levels as None. Metadata collection and path reads do not share a snapshot transaction.
        Missing owners and database errors propagate.

        Example:
            For an existing Manifestation, call ``catalog.retrieval.bundles.for_manifestation(manifestation_id)``;
            check optional ancestor and descendant rows before reading their columns.


        :param manifestation_id: Existing Manifestation ID; validation is delegated to its repository.
        :return: Bundle containing the requested row, available path rows, and their attached metadata.
        """

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
        """
        Read one WEMI path through an existing Expression.

        Choose the first linked Work and Manifestation, then that Manifestation's first Item. Repository ordering determines each first choice. Missing relationships leave
        levels as None. Metadata collection and path reads do not share a snapshot transaction.
        Missing owners and database errors propagate.

        Example:
            For an existing Expression, call ``catalog.retrieval.bundles.for_expression(expression_id)``;
            check optional ancestor and descendant rows before reading their columns.


        :param expression_id: Existing Expression ID; validation is delegated to its repository.
        :return: Bundle containing the requested row, available path rows, and their attached metadata.
        """

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
        """
        Read one WEMI path through an existing Work.

        Choose the first Expression, its first Manifestation, and that Manifestation's first Item. Repository ordering determines each first choice. Missing relationships leave
        levels as None. Metadata collection and path reads do not share a snapshot transaction.
        Missing owners and database errors propagate.

        Example:
            For an existing Work, call ``catalog.retrieval.bundles.for_work(work_id)``;
            check optional ancestor and descendant rows before reading their columns.


        :param work_id: Existing Work ID; validation is delegated to its repository.
        :return: Bundle containing the requested row, available path rows, and their attached metadata.
        """

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
        """
        Take one row from an iterable without consuming its remainder.

        Example:
            >>> BundleRetriever._first([]) is None
            True


        :param rows: Rows in their existing order.
        :return: First row, or None when empty.
        """

        return next(iter(rows), None)

    @staticmethod
    def _deduplicate(rows: Iterable[RowMapping], id_column: str) -> tuple[RowMapping, ...]:
        """
        Keep the first row for each ID, preserving encounter order.

        Missing IDs share the key None and collapse into one row. Unhashable IDs raise TypeError.

        Example:
            >>> BundleRetriever._deduplicate([{"id": 1}, {"id": 1}], "id")
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

    def _assemble(
        self,
        *,
        work: RowMapping | None,
        expression: RowMapping | None,
        manifestation: RowMapping | None,
        item: RowMapping | None,
    ) -> WemiBundle:
        """
        Collect attachments for the populated rows of a selected WEMI path.

        Visit Work, Expression, Manifestation, then Item. Deduplicate Agents, curated
        identifiers and Notes by ID, retaining the first mapping; retain all titles.
        Only path-row _catalog_link mappings become links, with a level default that
        the mapping may override. These are not all attachment edges. Reads may fail
        partway through; no encompassing transaction is opened.

        Example:
            Passing four None rows yields a bundle with no path rows or attachments.


        :param work: Work row, or None.
        :param expression: Expression row, or None.
        :param manifestation: Manifestation row, or None.
        :param item: Item row, or None.
        :return: Bundle retaining path mappings and attachment tuples.
        """

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
