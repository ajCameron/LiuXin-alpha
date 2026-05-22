"""Title repository API."""

from __future__ import annotations

from typing import Protocol, Sequence, runtime_checkable

from ..common import EntityId, RowInput, RowMapping, WemiLevel
from .base import BaseRepositoryAPI


@runtime_checkable
class TitleRepositoryAPI(BaseRepositoryAPI, Protocol):
    """Storage and linking API for WEMI titles."""

    def add_for_wemi(self, *, level: WemiLevel, entity_id: EntityId, data: RowInput) -> EntityId:
        """Create a title and link it to a WEMI entity."""

    def list_for_wemi(self, *, level: WemiLevel, entity_id: EntityId) -> Sequence[RowMapping]:
        """Return titles linked to a WEMI entity."""

    def preferred_for_wemi(self, *, level: WemiLevel, entity_id: EntityId) -> RowMapping | None:
        """Return the preferred title for a WEMI entity, if present."""
