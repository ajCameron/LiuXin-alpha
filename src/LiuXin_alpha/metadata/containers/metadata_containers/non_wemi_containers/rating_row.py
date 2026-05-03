"""Concrete row container for the ``ratings`` main table."""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

from ._row_base import MetadataTableRow


@dataclass(slots=True, kw_only=True)
class RatingRow(MetadataTableRow):
    TABLE_NAME: ClassVar[str] = "ratings"
    ID_COLUMN: ClassVar[str] = "rating_id"

    rating_id: int | None = None
    rating: float | None = None
    rating_out_of: int | None = None
    rating_for_calibre_tag_viewer: int | None = None
    rating_source: str | None = None
    rating_created_timestamp_ep_k: int | None = None
    rating_modified_timestamp_ep_k: int | None = None
    rating_source_created_datestamp_ep_k: int | None = None
    rating_source_modified_datestamp_ep_k: int | None = None
    rating_scratch: str | None = None


__all__ = ["RatingRow"]
