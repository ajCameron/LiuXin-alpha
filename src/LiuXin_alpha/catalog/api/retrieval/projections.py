"""
Define derived Catalog titles and compact Item summaries.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from ..common import EntityId, WemiLevel

@runtime_checkable
class ProjectionAPI(Protocol):
    """
    Describe semantic projection values without prescribing UI formatting.

    Example:
        A cache can retain ``item_summary(item_id)`` as metadata; refresh it when
        its source rows change because this API provides no snapshot token.
    """

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
