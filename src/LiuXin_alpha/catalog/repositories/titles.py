"""Title repository implementation scaffold."""

from __future__ import annotations

from typing import Sequence

from ..api.common import EntityId, RowInput, RowMapping, WemiLevel
from .base import BaseRepository


class TitleRepository(BaseRepository):
    table_name = "titles"
    id_column = "title_id"

    def add_for_wemi(self, *, level: WemiLevel, entity_id: EntityId, data: RowInput) -> EntityId:
        title_id = self.create(data)
        # Link write belongs here, but exact link-table helper depends on current DB API.
        raise NotImplementedError("After create(), link title_id to the requested WEMI entity")

    def list_for_wemi(self, *, level: WemiLevel, entity_id: EntityId) -> Sequence[RowMapping]:
        raise NotImplementedError("Move WEMI-title link reads here from databases")

    def preferred_for_wemi(self, *, level: WemiLevel, entity_id: EntityId) -> RowMapping | None:
        titles = self.list_for_wemi(level=level, entity_id=entity_id)
        return titles[0] if titles else None
