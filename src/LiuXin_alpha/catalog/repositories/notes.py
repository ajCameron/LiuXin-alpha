"""Repository for notes attached to WEMI entities."""

from __future__ import annotations

from typing import ClassVar, Mapping, Sequence

from LiuXin_alpha.databases.macro_types import LinkValue

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

    def replace_for_wemi(
        self,
        *,
        level: WemiLevel,
        entity_id: EntityId,
        notes: Sequence[str | RowInput],
    ) -> tuple[EntityId, ...]:
        """Replace all Notes attached to one WEMI entity atomically."""

        if level not in WEMI_TABLES:
            raise ValueError(f"unknown WEMI level: {level!r}")
        if not isinstance(notes, Sequence) or isinstance(notes, (str, bytes)):
            raise TypeError("notes must be a sequence of strings or mappings")
        payloads: list[RowInput] = []
        for note in notes:
            if isinstance(note, str):
                payloads.append({"note": note})
            elif isinstance(note, Mapping):
                payloads.append(dict(note))
            else:
                raise TypeError("notes must contain only strings or mappings")
        table = WEMI_TABLES[level]
        self._require_table_row(table, entity_id)
        spec = self._link_spec(table, self.table_name)
        with self._macros.transaction():
            note_ids = tuple(self.create(payload) for payload in payloads)
            self._macros.replace_links(
                spec,
                entity_id,
                (LinkValue(note_id) for note_id in note_ids),
            )
        return note_ids


__all__ = ["NoteRepository"]
