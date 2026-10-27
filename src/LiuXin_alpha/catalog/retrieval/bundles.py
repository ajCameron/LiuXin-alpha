"""
Assemble one selected WEMI path and its attached metadata.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from ..api.common import DatabaseHandle, EntityId, RowMapping, WemiBundle, WemiLevel
from .selection import WemiPath


class BundleRetriever:
    """
    Combine repository reads into a path with optional WEMI levels.

    All branches use Catalog's WEM path selection policy, independently of repository
    list ordering. Complete paths precede partial paths. This returns a preferred
    path, not a full descendant graph or a transactional snapshot.

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
        """Read the preferred ancestor path through the requested Item.

        Keep the Item fixed. Rank all available ancestor paths by the Catalog WEM
        selection policy; retain partial ancestors if no complete path exists.
        Reads and metadata assembly do not share a snapshot transaction.

        Example:
            An Item embodying a novel and its introduction selects one path;
            hierarchy/graph reads still expose both Works.

        :param item_id: Existing Item ID; missing-owner errors propagate.
        :return: Selected path and its attached metadata, including the requested Item.
        """
        item = self.repositories.items.require(item_id)
        manifestation = self.repositories.items.manifestation_for_item(item_id)
        path = (
            WemiPath()
            if manifestation is None
            else self._path_for_manifestation(manifestation)
        )
        return self._assemble_path(path, item=item)

    def for_manifestation(self, manifestation_id: EntityId) -> WemiBundle:
        """Read the preferred ancestor path through the requested Manifestation.

        Rank complete paths first, then available partial paths using the same
        WEM selection key. Choose the lowest Item ID on this Manifestation.
        Other Works embodied in it remain available through hierarchy/graph reads.

        Example:
            A shared edition returns one preferred Work without removing its
            introduction's separate Work from the catalogue.

        :param manifestation_id: Existing Manifestation ID; missing-owner errors propagate.
        :return: Selected path and its attached metadata, with optional ancestors/Item.
        """
        manifestation = self.repositories.manifestations.require(manifestation_id)
        return self._assemble_path(self._path_for_manifestation(manifestation))

    def for_expression(self, expression_id: EntityId) -> WemiBundle:
        """Read the preferred path through the requested Expression.

        Rank its linked Works and Manifestations using the Catalog WEM policy,
        then choose the lowest Item ID on the selected Manifestation. Missing
        relationships remain None; selection does not cross a missing edge.

        Example:
            An untranslated Expression with no edition still returns its Work
            and Expression rather than disappearing from a complete-ray view.

        :param expression_id: Existing Expression ID; missing-owner errors propagate.
        :return: Selected path and its metadata; reads are not a snapshot transaction.
        """
        expression = self.repositories.expressions.require(expression_id)
        works = self.repositories.expressions.list_works(expression_id)
        manifestations = self.repositories.manifestations.list_for_expression(
            expression_id
        )
        paths = (
            WemiPath(work, expression, manifestation, work, manifestation)
            for work in works or (None,)
            for manifestation in manifestations or (None,)
        )
        return self._assemble_path(min(paths, key=WemiPath.selection_key))

    def for_work(self, work_id: EntityId) -> WemiBundle:
        """Read the preferred path for a Work using SQL primary-ray semantics.

        Rank complete WEM paths by primary flags, ascending priorities and IDs.
        A preferred Expression without an edition cannot hide another complete
        path. If none is complete, retain the fullest available partial path.
        The lowest Item ID on the selected Manifestation wins; Item availability
        never changes bibliographic preference.

        Example:
            ``for_work(work_id)`` and ``wemi_primary_rays_v`` select the same
            Expression and Manifestation whenever a complete ray exists.

        :param work_id: Existing Work ID; missing-owner errors propagate.
        :return: Selected path and its metadata; reads are not a snapshot transaction.
        """
        work = self.repositories.works.require(work_id)
        return self._assemble_path(
            min(self._paths_for_work(work), key=WemiPath.selection_key)
        )

    def _paths_for_work(self, work: RowMapping) -> Iterable[WemiPath]:
        """Enumerate available descendants, retaining a missing level explicitly."""
        expressions = self.repositories.expressions.list_for_work(work["work_id"])
        for expression in expressions or (None,):
            manifestations = (
                ()
                if expression is None
                else self.repositories.manifestations.list_for_expression(
                    expression["expression_id"]
                )
            )
            for manifestation in manifestations or (None,):
                yield WemiPath(
                    work, expression, manifestation, expression, manifestation
                )

    def _path_for_manifestation(self, manifestation: RowMapping) -> WemiPath:
        """Select among all ancestors of this Manifestation without following siblings."""

        def paths() -> Iterable[WemiPath]:
            expressions = self.repositories.manifestations.list_expressions(
                manifestation["manifestation_id"]
            )
            for expression in expressions or (None,):
                works = (
                    ()
                    if expression is None
                    else self.repositories.expressions.list_works(
                        expression["expression_id"]
                    )
                )
                for work in works or (None,):
                    yield WemiPath(work, expression, manifestation, work, expression)

        return min(paths(), key=WemiPath.selection_key)

    def _assemble_path(
        self, path: WemiPath, *, item: RowMapping | None = None
    ) -> WemiBundle:
        """Add the lowest Item ID unless the caller supplied its requested Item."""
        if item is None and path.manifestation is not None:
            item = min(
                self.repositories.items.list_for_manifestation(
                    path.manifestation["manifestation_id"]
                ),
                key=lambda row: row["item_id"],
                default=None,
            )
        return self._assemble(
            work=path.work,
            expression=path.expression,
            manifestation=path.manifestation,
            item=item,
        )

    @staticmethod
    def _deduplicate(
        rows: Iterable[RowMapping], id_column: str
    ) -> tuple[RowMapping, ...]:
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
