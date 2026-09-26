"""
Provide the concrete synopses row value used by metadata callers.

The SynopsisRow dataclass stores database-shaped fields in memory and inherits
column mapping and diagnostic-string helpers. Creating or editing it performs no
database write.

Example:
    >>> row = SynopsisRow(synopsis='A journey begins.')
    >>> row.synopsis
    'A journey begins.'
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

from ._row_base import MetadataTableRow


@dataclass(slots=True, kw_only=True)
class SynopsisRow(MetadataTableRow):
    """
    Store reusable synopsis text with source and modification timestamps.

    Synopsis and scratch text are retained verbatim without parsing or generating a
    summary.

    Fields are keyword-only, mutable and default to None. from_mapping ignores unknown
    columns; to_mapping returns the stored fields without persisting them.

    Example:
        >>> row = SynopsisRow.from_mapping({'synopsis_id': 7, 'synopsis': 'A journey begins.'})
        >>> row.primary_id, row.to_mapping()['synopsis']
        (7, 'A journey begins.')
    """
    TABLE_NAME: ClassVar[str] = "synopses"
    ID_COLUMN: ClassVar[str] = "synopsis_id"

    synopsis_id: int | None = None
    synopsis: str | None = None
    synopsis_created_timestamp_ep_k: int | None = None
    synopsis_modified_timestamp_ep_k: int | None = None
    synopsis_source_created_datestamp_ep_k: int | None = None
    synopsis_source_modified_datestamp_ep_k: int | None = None
    synopsis_scratch: str | None = None


__all__ = ["SynopsisRow"]
