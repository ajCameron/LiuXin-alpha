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

    First repository-ordered relationships select the path. Multiple reads do not
    guarantee snapshot consistency; use graph retrieval for bounded descendants.

    Example:
        A ``BundleRetrieverAPI`` consumer must check ``bundle.work is not None``
        before accessing an Item bundle's Work.
    """

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
