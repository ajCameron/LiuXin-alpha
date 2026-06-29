"""Note repository implementation scaffold."""

from __future__ import annotations

from typing import Sequence

from ..api.common import EntityId, RowInput, RowMapping, WemiLevel
from .base import BaseRepository


class NoteRepository(BaseRepository):
    table_name = "notes"
    id_column = "note_id"

    def add_for_wemi(self, *, level: WemiLevel, entity_id: EntityId, data: RowInput) -> EntityId:
        note_id = self.create(data)
        # Link write belongs here, but exact link-table helper depends on current DB API.
        raise NotImplementedError("After create(), link note_id to the requested WEMI entity")

    def list_for_wemi(self, *, level: WemiLevel, entity_id: EntityId) -> Sequence[RowMapping]:
        raise NotImplementedError("Move WEMI-note link reads here from databases")
