"""WEMI bundle retrieval API."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from ..common import EntityId, WemiBundle


@runtime_checkable
class BundleRetrieverAPI(Protocol):
    """Read coherent WEMI slices for workflows, caches, or interfaces.

    An Item is followed upward. Broader roots choose one deterministic path:
    the first priority/ID-ordered relationship at each lower level. Use
    repository ``list_*`` methods when every descendant is required.

    Example::

        bundle = catalog.retrieval.bundles.for_item(item_id)
        print(bundle.work["work_title"])
    """

    def for_item(self, item_id: EntityId) -> WemiBundle:
        """
        Return a WEMI bundle rooted at an item.

        :param item_id: Existing Item ID.
        :return: Complete upward WEMI path and metadata attached along it.
        """

    def for_manifestation(self, manifestation_id: EntityId) -> WemiBundle:
        """
        Return a WEMI bundle rooted at a manifestation.

        :param manifestation_id: Existing Manifestation ID.
        :return: One deterministic Work/Expression/Manifestation/Item path plus
            metadata attached to its populated rows.
        """

    def for_expression(self, expression_id: EntityId) -> WemiBundle:
        """
        Return a WEMI bundle rooted at an expression.

        :param expression_id: Existing Expression ID.
        :return: One deterministic Work/Expression/Manifestation/Item path plus
            metadata attached to its populated rows.
        """

    def for_work(self, work_id: EntityId) -> WemiBundle:
        """
        Return a WEMI bundle rooted at a work.

        :param work_id: Existing Work ID.
        :return: One deterministic Work/Expression/Manifestation/Item path plus
            metadata attached to its populated rows.
        """
