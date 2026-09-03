"""Concrete row container for the ``synopses`` main table."""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

from ._row_base import MetadataTableRow


@dataclass(slots=True, kw_only=True)
class SynopsisRow(MetadataTableRow):
    """
    Represent a reusable synopsis row.
    """
    TABLE_NAME: ClassVar[str] = "synopses"
    ID_COLUMN: ClassVar[str] = "synopsis_id"

    synopsis_id: int | None = None
    synopsis: str | None = None
    synopsis_created_timestamp_ep_k: int | None = None
    synopsis_modified_timestamp_ep_k: int | None = None
    synopsis_source_created_datestamp_ep_k: int | None = None
    synopsis_source_modified_datestamp_ep_k: int | None = None
    synopsis_scratch: str | None = None


__all__ = ["SynopsisRow"]
