"""Concrete row container for the ``tags`` main table."""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

from ._row_base import MetadataTableRow


@dataclass(slots=True, kw_only=True)
class TagRow(MetadataTableRow):
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
