"""Item-scoped Annotation repository contract."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol, runtime_checkable

from ..common import EntityId, RowMapping
from .exact_entity import ExactEntityRepositoryAPI


@runtime_checkable
class AnnotationRepositoryAPI(ExactEntityRepositoryAPI, Protocol):
    """Store, match, and list reading annotations within one Item scope.

    Annotation identity is never global: Item ID is a required matching scope
    and the listing convenience always validates that Item first.
    """

    def list_for_item(
        self,
        item_id: EntityId,
        *,
        user_id: EntityId | None = None,
        kind: str | None = None,
    ) -> Sequence[RowMapping]:
        """Return Item Annotations filtered by optional user and kind."""


__all__ = ["AnnotationRepositoryAPI"]
