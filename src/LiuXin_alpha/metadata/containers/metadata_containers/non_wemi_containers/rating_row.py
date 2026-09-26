"""
Provide the concrete ratings row value used by metadata callers.

The RatingRow dataclass stores database-shaped fields in memory and inherits column
mapping and diagnostic-string helpers. Creating or editing it performs no database
write.

Example:
    >>> row = RatingRow(rating=4.5)
    >>> row.rating
    4.5
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

from ._row_base import MetadataTableRow


@dataclass(slots=True, kw_only=True)
class RatingRow(MetadataTableRow):
    """
    Store a numeric rating together with scale, source and optional Calibre-viewer value.

    No scale conversion or range validation is performed; rating_for_calibre_tag_viewer
    is supplied independently from rating and rating_out_of.

    Fields are keyword-only, mutable and default to None. from_mapping ignores unknown
    columns; to_mapping returns the stored fields without persisting them.

    Example:
        >>> row = RatingRow.from_mapping({'rating_id': 7, 'rating': 4.5})
        >>> row.primary_id, row.to_mapping()['rating']
        (7, 4.5)
    """
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
