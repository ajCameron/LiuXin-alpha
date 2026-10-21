"""
Contracts for selecting a single WEMI path with metadata.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from ..common import EntityId, WemiBundle


@runtime_checkable
class BundleRetrieverAPI(Protocol):
    """
    Describe path retrieval with optional ancestors and descendants.

    Catalog's preferred WEM policy selects a complete path first, or the fullest
    available partial path. Primary flags, ascending priorities and IDs rank
    candidates independently of repository list order. Multiple reads do not
    guarantee snapshot consistency; use graph retrieval for bounded descendants.

    Example:
        A ``BundleRetrieverAPI`` consumer must check ``bundle.work is not None``
        before accessing an Item bundle's Work.
    """

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
