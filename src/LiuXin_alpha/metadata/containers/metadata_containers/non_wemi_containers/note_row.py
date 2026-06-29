"""Concrete row container for the ``notes`` main table."""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

from ._row_base import MetadataTableRow


@dataclass(slots=True, kw_only=True)
class NoteRow(MetadataTableRow):
    TABLE_NAME: ClassVar[str] = "notes"
    ID_COLUMN: ClassVar[str] = "note_id"

    note_id: int | None = None
    note: str | None = None
    note_created_timestamp_ep_k: int | None = None
    note_modified_timestamp_ep_k: int | None = None
    note_source_created_datestamp_ep_k: int | None = None
    note_source_modified_datestamp_ep_k: int | None = None
    note_scratch: str | None = None


__all__ = ["NoteRow"]
