"""
Provide the concrete labels row value used by metadata callers.

The LabelRow dataclass stores database-shaped fields in memory and inherits column
mapping and diagnostic-string helpers. Creating or editing it performs no database
write.

Example:
    >>> row = LabelRow(label_text='Reference')
    >>> row.label_text
    'Reference'
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

from ._row_base import MetadataTableRow


@dataclass(slots=True, kw_only=True)
class LabelRow(MetadataTableRow):
    """
    Store display text, normalized text and description for a metadata label.

    label_text_norm is supplied independently; this container does not derive it from
    label_text.

    Fields are keyword-only, mutable and default to None. from_mapping ignores unknown
    columns; to_mapping returns the stored fields without persisting them.

    Example:
        >>> row = LabelRow.from_mapping({'label_id': 7, 'label_text': 'Reference'})
        >>> row.primary_id, row.to_mapping()['label_text']
        (7, 'Reference')
    """
    TABLE_NAME: ClassVar[str] = "labels"
    ID_COLUMN: ClassVar[str] = "label_id"

    label_id: int | None = None
    label_text: str | None = None
    label_text_norm: str | None = None
    label_description: str | None = None
    label_scratch: str | None = None
    label_created_timestamp_ep_k: int | None = None
    label_modified_timestamp_ep_k: int | None = None
    label_source_created_datestamp_ep_k: int | None = None
    label_source_modified_datestamp_ep_k: int | None = None


__all__ = ["LabelRow"]
