"""
Provide the concrete tags row value used by metadata callers.

The TagRow dataclass stores database-shaped fields in memory and inherits column
mapping and diagnostic-string helpers. Creating or editing it performs no database
write.

Example:
    >>> row = TagRow(tag='reference')
    >>> row.tag
    'reference'
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

from ._row_base import MetadataTableRow


@dataclass(slots=True, kw_only=True)
class TagRow(MetadataTableRow):
    """
    Store a tag vocabulary entry with optional description and search hash.

    tag_phash is supplied independently; this container does not normalize the tag or
    calculate its hash.

    Fields are keyword-only, mutable and default to None. from_mapping ignores unknown
    columns; to_mapping returns the stored fields without persisting them.

    Example:
        >>> row = TagRow.from_mapping({'tag_id': 7, 'tag': 'reference'})
        >>> row.primary_id, row.to_mapping()['tag']
        (7, 'reference')
    """
    TABLE_NAME: ClassVar[str] = "tags"
    ID_COLUMN: ClassVar[str] = "tag_id"

    tag_id: int | None = None
    tag: str | None = None
    tag_phash: str | None = None
    tag_description: str | None = None
    tag_scratch: str | None = None
    tag_created_timestamp_ep_k: int | None = None
    tag_modified_timestamp_ep_k: int | None = None
    tag_source_created_datestamp_ep_k: int | None = None
    tag_source_modified_datestamp_ep_k: int | None = None


__all__ = ["TagRow"]
