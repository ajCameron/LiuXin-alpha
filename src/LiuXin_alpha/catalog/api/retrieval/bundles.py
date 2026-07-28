"""WEMI bundle retrieval API."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from ..common import EntityId, WemiBundle


@runtime_checkable
class BundleRetrieverAPI(Protocol):
    """Read coherent WEMI slices for downstream workflows or surfaces."""

    def for_item(self, item_id: EntityId) -> WemiBundle:
        """
        Return a WEMI bundle rooted at an item.

        :param item_id:
        :return:
        """

    def for_manifestation(self, manifestation_id: EntityId) -> WemiBundle:
        """
        Return a WEMI bundle rooted at a manifestation.

        :param manifestation_id:
        :return:
        """

    def for_expression(self, expression_id: EntityId) -> WemiBundle:
        """
        Return a WEMI bundle rooted at an expression.

        :param expression_id:
        :return:
        """

    def for_work(self, work_id: EntityId) -> WemiBundle:
        """
        Return a WEMI bundle rooted at a work.

        :param work_id:
        :return:
        """
