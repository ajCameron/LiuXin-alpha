"""Synopsis repository contract."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol, runtime_checkable

from ..common import EntityId, RowInput, RowMapping, WemiLevel
from .exact_entity import ExactEntityRepositoryAPI


@runtime_checkable
class SynopsisRepositoryAPI(ExactEntityRepositoryAPI, Protocol):
    """Create, replace, clear, and read reusable WEMI Synopses.

    Replacement affects only the selected WEMI relationship set; unrelated
    entities and their Synopsis links remain untouched.
    """

    def add_for_wemi(
        self,
        *,
        level: WemiLevel,
        entity_id: EntityId,
        data: RowInput,
    ) -> EntityId:
        """Create and append one Synopsis to a WEMI entity."""

    def replace_for_wemi(
        self,
        *,
        level: WemiLevel,
        entity_id: EntityId,
        synopses: Sequence[str | RowInput],
    ) -> tuple[EntityId, ...]:
        """Replace all linked Synopses, clearing them with an empty sequence."""

    def list_for_wemi(
        self,
        *,
        level: WemiLevel,
        entity_id: EntityId,
    ) -> Sequence[RowMapping]:
        """Return priority-ordered Synopses attached to one WEMI entity."""


__all__ = ["SynopsisRepositoryAPI"]
