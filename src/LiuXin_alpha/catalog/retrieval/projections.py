"""
Derive semantic titles and Item summaries from Catalog repositories.
"""

from __future__ import annotations

from typing import Any

from ..api.common import DatabaseHandle, EntityId, WemiLevel


class ProjectionService:
    """
    Provide display-neutral values with explicit title fallbacks.

    Example:
        A surface can format the returned Item summary for HTML or terminal output;
        this service supplies metadata values without either rendering format.
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

    def display_title(self, *, level: WemiLevel, entity_id: EntityId) -> str:
        """
        Choose the first nonblank string title and strip its outer whitespace.

        A Work tries title, canonical title, then sort title. Other levels read a
        bundle: Expression tries override and subtitle, while Manifestation and Item
        try Manifestation subtitle first, followed by Expression titles. Both then
        try Work title and canonical title, omitting Work sort title. Nonstrings are
        ignored. Unknown levels fail during route lookup. Bundle metadata reads can
        also fail even when only a display title is wanted.

        Example:
            An Expression with a blank override falls back to its subtitle, then the
            selected Work's title or canonical title.


        :param level: WEMI level used to choose the repository or bundle route.
        :param entity_id: Existing entity ID; missing-owner errors propagate.
        :return: Selected title, or ``Untitled <level> <id>``.
        :raises AttributeError: The non-Work level has no bundle retrieval method.
        """

        candidates: tuple[object, ...]
        if level == "work":
            row = self.repositories.works.require(entity_id)
            candidates = (
                row.get("work_title"),
                row.get("work_canonical_title"),
                row.get("work_sort_title"),
            )
        else:
            from .bundles import BundleRetriever

            retriever = getattr(
                BundleRetriever(self.db, self.repositories),
                f"for_{level}",
            )
            bundle = retriever(entity_id)
            candidates = self._bundle_title_candidates(bundle, level)
        title = next(
            (
                value.strip()
                for value in candidates
                if isinstance(value, str) and value.strip()
            ),
            None,
        )
        return title or f"Untitled {level} {entity_id}"

    def item_summary(self, item_id: EntityId) -> dict[str, object]:
        """
        Collect an Item's path IDs, display title and compact attachment values.

        Missing ancestors and columns yield None. Agents are a tuple of canonical
        names; identifiers are tuples of stored scheme/value pairs, possibly with
        None entries. Display-title selection performs a second bundle read, so the
        summary is not a single database snapshot. Database errors propagate.

        Example:
            Use ``summary["title"]`` for semantic display text and ``summary["agents"]``
            for names; interface-specific formatting remains the caller's responsibility.


        :param item_id: Existing Item ID for both bundle and title reads.
        :return: Dictionary with item_id, work_id, expression_id, manifestation_id, title,
            location, lifecycle_status, agents and identifiers.
        """

        bundle = self.repositories.items.get_metadata_bundle(item_id)
        item = bundle.item or {}
        return {
            "item_id": item_id,
            "work_id": None if bundle.work is None else bundle.work.get("work_id"),
            "expression_id": (
                None if bundle.expression is None else bundle.expression.get("expression_id")
            ),
            "manifestation_id": (
                None
                if bundle.manifestation is None
                else bundle.manifestation.get("manifestation_id")
            ),
            "title": self.display_title(level="item", entity_id=item_id),
            "location": item.get("item_location"),
            "lifecycle_status": item.get("item_lifecycle_status"),
            "agents": tuple(
                row.get("agent_canonical_name") for row in bundle.agents
            ),
            "identifiers": tuple(
                (
                    row.get("entity_identifier_scheme"),
                    row.get("entity_identifier_value"),
                )
                for row in bundle.identifiers
            ),
        }

    @staticmethod
    def _bundle_title_candidates(bundle: object, level: WemiLevel) -> tuple[object, ...]:
        """
        Order title columns from optional bundle rows without filtering values.

        Missing or false-valued row attributes act as empty mappings. This helper
        does not validate levels, strip text or include Work sort titles.

        Example:
            >>> ProjectionService._bundle_title_candidates(object(), "expression")
            (None, None, None, None)


        :param bundle: Object with optional work, expression and manifestation mapping attributes.
        :param level: Expression omits Manifestation titles; every other value includes them.
        :return: Raw candidate tuple, including None, empty and nonstring values.
        """

        work = getattr(bundle, "work", None) or {}
        expression = getattr(bundle, "expression", None) or {}
        manifestation = getattr(bundle, "manifestation", None) or {}
        if level == "expression":
            return (
                expression.get("expression_title_override"),
                expression.get("expression_subtitle"),
                work.get("work_title"),
                work.get("work_canonical_title"),
            )
        return (
            manifestation.get("manifestation_subtitle"),
            expression.get("expression_title_override"),
            expression.get("expression_subtitle"),
            work.get("work_title"),
            work.get("work_canonical_title"),
        )
