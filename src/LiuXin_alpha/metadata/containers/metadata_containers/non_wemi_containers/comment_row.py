"""
Provide the concrete comments row value used by metadata callers.

The CommentRow dataclass stores database-shaped fields in memory and inherits column
mapping and diagnostic-string helpers. Creating or editing it performs no database
write.

Example:
    >>> row = CommentRow(comment='A useful observation')
    >>> row.comment
    'A useful observation'
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

from ._row_base import MetadataTableRow


@dataclass(slots=True, kw_only=True)
class CommentRow(MetadataTableRow):
    """
    Store reusable commentary text with source and modification timestamps.

    The comment text and scratch field are retained verbatim; timestamp fields are
    optional integer values.

    Fields are keyword-only, mutable and default to None. from_mapping ignores unknown
    columns; to_mapping returns the stored fields without persisting them.

    Example:
        >>> row = CommentRow.from_mapping({'comment_id': 7, 'comment': 'A useful observation'})
        >>> row.primary_id, row.to_mapping()['comment']
        (7, 'A useful observation')
    """
    TABLE_NAME: ClassVar[str] = "comments"
    ID_COLUMN: ClassVar[str] = "comment_id"

    comment_id: int | None = None
    comment: str | None = None
    comment_created_timestamp_ep_k: int | None = None
    comment_modified_timestamp_ep_k: int | None = None
    comment_source_created_datestamp_ep_k: int | None = None
    comment_source_modified_datestamp_ep_k: int | None = None
    comment_scratch: str | None = None


__all__ = ["CommentRow"]
