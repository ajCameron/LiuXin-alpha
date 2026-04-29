"""Concrete row container for the ``genres`` main table."""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

from ._row_base import MetadataTableRow


@dataclass(slots=True, kw_only=True)
class GenreRow(MetadataTableRow):
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
