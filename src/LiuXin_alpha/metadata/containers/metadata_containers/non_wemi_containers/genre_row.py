"""
Provide the concrete genres row value used by metadata callers.

The GenreRow dataclass stores database-shaped fields in memory and inherits column
mapping and diagnostic-string helpers. Creating or editing it performs no database
write.

Example:
    >>> row = GenreRow(genre='Science Fiction')
    >>> row.genre
    'Science Fiction'
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

from ._row_base import MetadataTableRow


@dataclass(slots=True, kw_only=True)
class GenreRow(MetadataTableRow):
    """
    Store a genre vocabulary row with an optional parent, position and tree id.

    Display/sort text, full hierarchy text and hash fields are stored independently. Use
    a GenreTreeRelation for explicit relation validation.

    Fields are keyword-only, mutable and default to None. from_mapping ignores unknown
    columns; to_mapping returns the stored fields without persisting them.

    Example:
        >>> row = GenreRow.from_mapping({'genre_id': 7, 'genre': 'Science Fiction'})
        >>> row.primary_id, row.to_mapping()['genre']
        (7, 'Science Fiction')
    """
    TABLE_NAME: ClassVar[str] = "genres"
    ID_COLUMN: ClassVar[str] = "genre_id"

    genre_id: int | None = None
    genre: str | None = None
    genre_sort: str | None = None
    genre_phash: str | None = None
    genre_parent_id: int | None = None
    genre_position: int | None = None
    genre_tree_id: int | None = None
    genre_full: str | None = None
    genre_created_timestamp_ep_k: int | None = None
    genre_modified_timestamp_ep_k: int | None = None
    genre_source_created_datestamp_ep_k: int | None = None
    genre_source_modified_datestamp_ep_k: int | None = None
    genre_scratch: str | None = None


__all__ = ["GenreRow"]
