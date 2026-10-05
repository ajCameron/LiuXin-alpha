"""
Provide the concrete languages row value used by metadata callers.

The LanguageRow dataclass stores database-shaped fields in memory and inherits
column mapping and diagnostic-string helpers. Creating or editing it performs no
database write.

Example:
    >>> row = LanguageRow(language='English')
    >>> row.language
    'English'
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

from ._row_base import MetadataTableRow


@dataclass(slots=True, kw_only=True)
class LanguageRow(MetadataTableRow):
    """
    Store a language vocabulary row and its optional code variants.

    Name, legacy code, ISO 639 codes and BCP 47 components remain independent supplied
    values. display_name chooses a name/code fallback without code validation.

    Fields are keyword-only, mutable and default to None. from_mapping ignores unknown
    columns; to_mapping returns the stored fields without persisting them.

    Example:
        >>> row = LanguageRow.from_mapping({'language_id': 7, 'language': 'English'})
        >>> row.primary_id, row.to_mapping()['language']
        (7, 'English')
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
        """
        Prefer the language name, then language_code, then language_bcp47_primary.

        The first truthy value is returned as stored, without trimming or code
        normalization.

        Example:
            >>> LanguageRow(language_code="eng").display_name
            'eng'


        :return: Selected display value; None when all default fields are absent.
        """
        return self.language or self.language_code or self.language_bcp47_primary


__all__ = ["LanguageRow"]
