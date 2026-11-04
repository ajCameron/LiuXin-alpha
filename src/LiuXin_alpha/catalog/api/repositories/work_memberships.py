"""Declare the shared Work membership contract for Tag and Series repositories."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from typing import Protocol, runtime_checkable

from ..common import EntityId, RowMapping
from .exact_entity import ExactEntityRepositoryAPI


@runtime_checkable
class WorkMembershipRepositoryAPI(ExactEntityRepositoryAPI, Protocol):
    """Combine exact entity CRUD with explicit, transactional Work memberships.

    Used by catalog.tags and catalog.series. Entity IDs identify rows of that
    repository; owner IDs always identify Works. Existing link metadata is
    retained, and membership replacement never deletes entity rows. This
    structural protocol does not enforce signatures or transaction behavior.

    Example:
        catalog.tags.replace_for_work(work_id=work_id, entity_ids=(tag_id,))
    """

    def link_to_work(self, *, entity_id: EntityId, work_id: EntityId) -> None:
        """Add a membership unless the pair already has a link of any type.

        New links use a null type and priority scoped to the Tag/Series's Works.
        Existing links are unchanged. Missing endpoints raise CatalogNotFoundError.

        :param entity_id: Existing Tag or Series ID for this repository.
        :param work_id: Existing Work ID.
        :return: None; the operation joins enclosing macro transactions.
        """

    def unlink_from_work(self, *, entity_id: EntityId, work_id: EntityId) -> None:
        """Remove all types for this pair, preserving other links and entity rows.

        :param entity_id: Existing Tag or Series ID for this repository.
        :param work_id: Existing Work ID; missing endpoints raise.
        :return: None; an absent link between valid endpoints is a no-op.
        """

    def is_linked_to_work(self, *, entity_id: EntityId, work_id: EntityId) -> bool:
        """Check membership across all types after requiring both endpoints.

        :param entity_id: Existing Tag or Series ID for this repository.
        :param work_id: Existing Work ID; missing endpoints raise.
        :return: True if at least one pair link exists, otherwise False.
        """

    def list_for_work(self, work_id: EntityId) -> Sequence[RowMapping]:
        """Read copied member rows and link metadata within one transaction.

        Each row includes _catalog_link. Multiple Series link types can repeat
        an entity. Reader priority order is not preferred-Series selection.

        :param work_id: Existing Work ID; missing or dangling rows raise.
        :return: Linked rows, or an empty tuple for an unlinked Work.
        """

    def replace_for_work(
        self, *, work_id: EntityId, entity_ids: Iterable[EntityId]
    ) -> None:
        """Replace one Work's membership set while preserving retained link data.

        Input order does not reorder existing links; removed pairs lose all types.

        :param work_id: Existing Work ID.
        :param entity_ids: Unique existing member IDs; empty input clears links.
        :return: None; invalid input fails before mutations, which are atomic.
        """

    def replace_for_works(
        self, replacements: Mapping[EntityId, Iterable[EntityId]]
    ) -> None:
        """Validate all endpoints and atomically replace a batch of memberships.

        :param replacements: Work IDs mapped to unique Tag/Series IDs; generators
            are materialized before querying. Empty mappings are no-ops.
        :return: None; a late failure rolls back the complete batch.
        """


__all__ = ["WorkMembershipRepositoryAPI"]
