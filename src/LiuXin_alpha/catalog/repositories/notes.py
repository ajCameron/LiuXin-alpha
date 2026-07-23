"""Repository for notes attached to WEMI entities."""

from __future__ import annotations

from typing import ClassVar, Mapping, Sequence

from ..api.common import EntityId, RowInput, RowMapping, WemiLevel
from ..matching.entity_specs import NOTE_SPEC
from .base import WEMI_TABLES
from .exact import ExactEntityRepository


class NoteRepository(ExactEntityRepository):
    """Store reusable note rows and their WEMI relationships."""

    table_name = NOTE_SPEC.table_name
    id_column = NOTE_SPEC.id_column
    input_aliases: ClassVar[Mapping[str, str]] = NOTE_SPEC.input_aliases
    match_spec = NOTE_SPEC

    def add_for_wemi(self, *, level: WemiLevel, entity_id: EntityId, data: RowInput) -> EntityId:
        """Create and attach a note to one WEMI entity."""

        if level not in WEMI_TABLES:
            raise ValueError(f"unknown WEMI level: {level!r}")
        self._require_table_row(WEMI_TABLES[level], entity_id)
        self._link_spec(WEMI_TABLES[level], self.table_name)
        note_id = self.create(data)
        self._link(WEMI_TABLES[level], entity_id, self.table_name, note_id)
        return note_id

    def list_for_wemi(self, *, level: WemiLevel, entity_id: EntityId) -> Sequence[RowMapping]:
        """Return notes attached to one WEMI entity."""

        if level not in WEMI_TABLES:
            raise ValueError(f"unknown WEMI level: {level!r}")
        self._require_table_row(WEMI_TABLES[level], entity_id)
        if self._wrapper.get_link_spec(WEMI_TABLES[level], self.table_name) is None:
            return ()
        return self._linked_rows(WEMI_TABLES[level], entity_id, self.table_name)


__all__ = ["NoteRepository"]
