"""
Provide the concrete series row value used by metadata callers.

The SeriesRow dataclass stores database-shaped fields in memory and inherits column
mapping and diagnostic-string helpers. Creating or editing it performs no database
write.

Example:
    >>> row = SeriesRow(series='Example Cycle')
    >>> row.series
    'Example Cycle'
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

from ._row_base import MetadataTableRow


@dataclass(slots=True, kw_only=True)
class SeriesRow(MetadataTableRow):
    """
    Store a series vocabulary entry with optional parent and tree metadata.

    Display, normalized/sort text and full hierarchy text are independent. Use
    SeriesTreeRelation for explicit local relation validation.

    Fields are keyword-only, mutable and default to None. from_mapping ignores unknown
    columns; to_mapping returns the stored fields without persisting them.

    Example:
        >>> row = SeriesRow.from_mapping({'series_id': 7, 'series': 'Example Cycle'})
        >>> row.primary_id, row.to_mapping()['series']
        (7, 'Example Cycle')
    """
    TABLE_NAME: ClassVar[str] = "series"
    ID_COLUMN: ClassVar[str] = "series_id"

    series_id: int | None = None
    series: str | None = None
    series_name_norm: str | None = None
    series_sort: str | None = None
    series_phash: str | None = None
    series_over_author: int | None = None
    series_parent_id: int | None = None
    series_parent_position: int | None = None
    series_tree_id: str | None = None
    series_full: str | None = None
    series_created_timestamp_ep_k: int | None = None
    series_modified_timestamp_ep_k: int | None = None
    series_source_created_datestamp_ep_k: int | None = None
    series_source_modified_datestamp_ep_k: int | None = None
    series_scratch: str | None = None


__all__ = ["SeriesRow"]
