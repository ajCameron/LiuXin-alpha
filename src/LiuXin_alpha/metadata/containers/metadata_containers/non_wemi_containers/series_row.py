"""Concrete row container for the ``series`` main table."""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

from ._row_base import MetadataTableRow


@dataclass(slots=True, kw_only=True)
class SeriesRow(MetadataTableRow):
    """
    Represent a hierarchical series vocabulary row.
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
