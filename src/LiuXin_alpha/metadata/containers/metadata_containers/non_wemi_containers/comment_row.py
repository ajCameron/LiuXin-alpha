"""Concrete row container for the ``comments`` main table."""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

from ._row_base import MetadataTableRow


@dataclass(slots=True, kw_only=True)
class CommentRow(MetadataTableRow):
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
