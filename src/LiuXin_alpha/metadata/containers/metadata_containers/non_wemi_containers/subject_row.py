"""Concrete row container for the ``subjects`` main table."""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

from ._row_base import MetadataTableRow


@dataclass(slots=True, kw_only=True)
class SubjectRow(MetadataTableRow):
    TABLE_NAME: ClassVar[str] = "subjects"
    ID_COLUMN: ClassVar[str] = "subject_id"

    subject_id: int | None = None
    subject: str | None = None
    subject_phash: str | None = None
    subject_sort: str | None = None
    subject_parent_id: int | None = None
    subject_parent_position: int | None = None
    subject_tree_id: str | None = None
    subject_full: str | None = None
    subject_created_timestamp_ep_k: int | None = None
    subject_modified_timestamp_ep_k: int | None = None
    subject_source_created_datestamp_ep_k: int | None = None
    subject_source_modified_datestamp_ep_k: int | None = None
    subject_scratch: str | None = None


__all__ = ["SubjectRow"]
