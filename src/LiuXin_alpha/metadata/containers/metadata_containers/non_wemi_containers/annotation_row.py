"""Concrete row container for the ``annotations`` main table."""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

from ._row_base import MetadataTableRow


@dataclass(slots=True, kw_only=True)
class AnnotationRow(MetadataTableRow):
    """
    Represent a reader annotation anchored to an Item.
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
