"""Catalog repository contracts for parent-linked vocabulary forests."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Protocol

from LiuXin_alpha.catalog.api.common import RowMapping
from LiuXin_alpha.catalog.api.repositories.exact_entity import ExactEntityRepositoryAPI
from LiuXin_alpha.catalog.api.repositories.work_memberships import (
    WorkMembershipRepositoryAPI,
)

if TYPE_CHECKING:
    from LiuXin_alpha.catalog.api.metadata_trees import VocabularyTreeInput


# Existing repository bases are checked by basedpyright; mypy skips them.
class HierarchicalRepositoryAPI(ExactEntityRepositoryAPI, Protocol):  # type: ignore[misc]
    """Own authoritative hierarchy reads and writes for one native vocabulary.

    Structural mutations validate the complete proposed tree inside a serialized
    transaction. Reads expose detached physical mappings; direct repository writes
    leave cache invalidation to the caller.
    """

    def roots(self, *, limit: int = 100, offset: int = 0) -> tuple[RowMapping, ...]:
        """Read one page of ordered roots."""
        ...

    def children(
        self, entity_id: int, *, limit: int = 100, offset: int = 0
    ) -> tuple[RowMapping, ...]:
        """Require a parent and read its ordered immediate children."""
        ...

    def ancestors(
        self, entity_id: int, *, max_nodes: int = 10000
    ) -> tuple[RowMapping, ...]:
        """Read complete ancestry from root to parent."""
        ...

    def subtree(
        self, entity_id: int, *, max_nodes: int = 10000
    ) -> tuple[RowMapping, ...]:
        """Read a complete root-inclusive preorder subtree."""
        ...

    def ensure_path(
        self,
        names: Sequence[str],
        *,
        parent_id: int | None = None,
        max_nodes: int = 10000,
    ) -> tuple[tuple[int, ...], bool]:
        """Return component IDs and whether any node was created."""
        ...

    def create_tree(
        self,
        node: VocabularyTreeInput,
        *,
        parent_id: int | None = None,
        max_nodes: int = 10000,
    ) -> tuple[int, ...]:
        """Create fresh nodes atomically and return preorder IDs."""
        ...

    def move(
        self, entity_id: int, *, parent_id: int | None, before_id: int | None = None
    ) -> bool:
        """Move a branch and report whether its location/order changed."""
        ...

    def reorder(self, *, parent_id: int | None, entity_ids: Sequence[int]) -> bool:
        """Replace a complete sibling order and report whether it changed."""
        ...

    def delete(self, entity_id: int, *, detach: bool = False) -> None:
        """Delete a leaf only; attachments require explicit detachment."""
        ...

    def delete_subtree(
        self, entity_id: int, *, detach: bool = False, max_nodes: int = 10000
    ) -> tuple[int, ...]:
        """Delete an explicit bounded subtree and return child-first IDs."""
        ...


class HierarchicalMembershipRepositoryAPI(
    HierarchicalRepositoryAPI,
    WorkMembershipRepositoryAPI,  # type: ignore[misc]
    Protocol,
):
    """Combine Series hierarchy operations with existing explicit Work memberships.

    Series names remain globally unique. Sibling positions describe the Series
    forest and do not change the priorities of its Work membership relationships.
    """
