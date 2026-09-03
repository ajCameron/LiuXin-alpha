"""Concrete row container for the ``labels`` main table."""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

from ._row_base import MetadataTableRow


@dataclass(slots=True, kw_only=True)
class LabelRow(MetadataTableRow):
    """
    Represent a descriptive label row with normalized text.
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
