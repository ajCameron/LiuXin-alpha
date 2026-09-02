"""Concrete row container for the ``languages`` main table."""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

from ._row_base import MetadataTableRow


@dataclass(slots=True, kw_only=True)
class LanguageRow(MetadataTableRow):
    """
    Represent a persisted language vocabulary row and its standard codes.
    """
    TABLE_NAME: ClassVar[str] = "languages"
    ID_COLUMN: ClassVar[str] = "language_id"

    language_id: int | None = None
    language: str | None = None
    language_code: str | None = None
    language_iso639_1: str | None = None
    language_iso639_2_b: str | None = None
    language_iso639_2_t: str | None = None
    language_bcp47_primary: str | None = None
    language_bcp47_variants: str | None = None
    language_created_timestamp_ep_k: int | None = None
    language_modified_timestamp_ep_k: int | None = None
    language_source_created_datestamp_ep_k: int | None = None
    language_source_modified_datestamp_ep_k: int | None = None
    language_scratch: str | None = None

    @property
    def display_name(self) -> str | None:
        return self.language or self.language_code or self.language_bcp47_primary


__all__ = ["LanguageRow"]
