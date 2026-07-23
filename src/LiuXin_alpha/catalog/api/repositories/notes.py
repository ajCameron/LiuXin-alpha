"""Note repository API."""

from __future__ import annotations

from typing import Protocol, Sequence, runtime_checkable

from ..common import EntityId, RowInput, RowMapping, WemiLevel
from .exact_entity import ExactEntityRepositoryAPI


@runtime_checkable
class NoteRepositoryAPI(ExactEntityRepositoryAPI, Protocol):
    """Storage and linking API for WEMI notes."""

    def add_for_wemi(self, *, level: WemiLevel, entity_id: EntityId, data: RowInput) -> EntityId:
        """Create a note and link it to a WEMI entity."""

    def list_for_wemi(self, *, level: WemiLevel, entity_id: EntityId) -> Sequence[RowMapping]:
        """Return notes linked to a WEMI entity."""
