"""Comment repository contract."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol, runtime_checkable

from ..common import EntityId, RowInput, RowMapping, WemiLevel
from .exact_entity import ExactEntityRepositoryAPI


@runtime_checkable
class CommentRepositoryAPI(ExactEntityRepositoryAPI, Protocol):
    """Create, replace, clear, and read WEMI-owned comments.

    Comments are not globally reused by identity. ``replace_for_wemi`` is the
    preferred edit operation because it makes clear/replace semantics atomic.
    """

    def add_for_wemi(
        self,
        *,
        level: WemiLevel,
        entity_id: EntityId,
        data: RowInput,
    ) -> EntityId:
        """Create and append one Comment to a WEMI entity."""

    def replace_for_wemi(
        self,
        *,
        level: WemiLevel,
        entity_id: EntityId,
        data: RowInput | None,
    ) -> EntityId | None:
        """Replace the owned Comment, or clear it with ``None``."""

    def list_for_wemi(
        self,
        *,
        level: WemiLevel,
        entity_id: EntityId,
    ) -> Sequence[RowMapping]:
        """Return priority-ordered Comments attached to one WEMI entity."""


__all__ = ["CommentRepositoryAPI"]
