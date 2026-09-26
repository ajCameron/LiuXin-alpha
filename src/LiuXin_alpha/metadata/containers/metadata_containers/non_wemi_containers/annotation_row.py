"""
Provide the concrete annotations row value used by metadata callers.

The AnnotationRow dataclass stores database-shaped fields in memory and inherits
column mapping and diagnostic-string helpers. Creating or editing it performs no
database write.

Example:
    >>> row = AnnotationRow(annotation_item_id=7)
    >>> row.annotation_item_id
    7
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

from ._row_base import MetadataTableRow


@dataclass(slots=True, kw_only=True)
class AnnotationRow(MetadataTableRow):
    """
    Store a reader annotation anchored to an item and optional user/device.

    Anchor positions, selected/note text, source timestamps and extra JSON remain
    supplied values; construction does not interpret anchors or decode
    annotation_extra_json.

    Fields are keyword-only, mutable and default to None. from_mapping ignores unknown
    columns; to_mapping returns the stored fields without persisting them.

    Example:
        >>> row = AnnotationRow.from_mapping({'annotation_id': 7, 'annotation_item_id': 7})
        >>> row.primary_id, row.to_mapping()['annotation_item_id']
        (7, 7)
    """
    TABLE_NAME: ClassVar[str] = "annotations"
    ID_COLUMN: ClassVar[str] = "annotation_id"

    annotation_id: int | None = None
    annotation_user_id: int | None = None
    annotation_item_id: int | None = None
    annotation_kind: str | None = None
    annotation_anchor_type: str | None = None
    annotation_anchor_start: str | None = None
    annotation_anchor_end: str | None = None
    annotation_selected_text: str | None = None
    annotation_note_text: str | None = None
    annotation_source_created_datestamp_ep_k: int | None = None
    annotation_source_modified_datestamp_ep_k: int | None = None
    annotation_source_deleted_datestamp_ep_k: int | None = None
    annotation_source: str | None = None
    annotation_device_id: int | None = None
    annotation_extra_json: str | None = None
    annotation_created_timestamp_ep_k: int | None = None
    annotation_modified_timestamp_ep_k: int | None = None
    annotation_scratch: str | None = None


__all__ = ["AnnotationRow"]
