"""
Provide the concrete notes row value used by metadata callers.

The NoteRow dataclass stores database-shaped fields in memory and inherits column
mapping and diagnostic-string helpers. Creating or editing it performs no database
write.

Example:
    >>> row = NoteRow(note='Read chapter two')
    >>> row.note
    'Read chapter two'
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

from ._row_base import MetadataTableRow


@dataclass(slots=True, kw_only=True)
class NoteRow(MetadataTableRow):
    """
    Store reusable note text with source and modification timestamps.

    The note and scratch values are retained verbatim; no markup parsing or timestamp
    conversion is performed.

    Fields are keyword-only, mutable and default to None. from_mapping ignores unknown
    columns; to_mapping returns the stored fields without persisting them.

    Example:
        >>> row = NoteRow.from_mapping({'note_id': 7, 'note': 'Read chapter two'})
        >>> row.primary_id, row.to_mapping()['note']
        (7, 'Read chapter two')
    """
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
