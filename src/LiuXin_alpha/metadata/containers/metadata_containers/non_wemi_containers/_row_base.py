"""
Share mapping and diagnostic behavior across concrete metadata row dataclasses.

The base uses dataclass constructor fields as its serialized column surface.
Database-shaped values are retained in memory without implicit reads or writes.

Example:
    >>> from LiuXin_alpha.metadata.containers import LanguageRow
    >>> LanguageRow.from_mapping({"language_id": 7}).primary_id
    7
"""

from __future__ import annotations

from dataclasses import fields
from typing import ClassVar, Self

from LiuXin_alpha.metadata.api.containers_api.main_table_containers_api import (
    MetadataRowMapping,
    MetadataRowValue,
    MetadataTableRowAPI,
)
from LiuXin_alpha.metadata.containers.metadata_containers._string_formatting import (
    compact_mapping_string,
)


class MetadataTableRow(MetadataTableRowAPI):
    """
    Provide table/id metadata and mapping helpers for concrete row dataclasses.

    Subclasses define TABLE_NAME and ID_COLUMN and supply dataclass fields. The base
    itself does not implement a generic row constructor or runtime field validation.

    Example:
        >>> from LiuXin_alpha.metadata.containers import LanguageRow
        >>> isinstance(LanguageRow(), MetadataTableRow)
        True
    """

    TABLE_NAME: ClassVar[str]
    ID_COLUMN: ClassVar[str]

    @classmethod
    def from_mapping(cls, row: MetadataRowMapping) -> Self:
        """
        Construct a row from recognized dataclass constructor fields in a mapping-like input.

        A callable keys method supports sqlite3.Row. Extra keys are ignored and omitted
        fields use constructor defaults; supplied values are not coerced or deep-copied.

        Example:
            >>> from LiuXin_alpha.metadata.containers import LanguageRow
            >>> row = LanguageRow.from_mapping({"language": "English", "ignored": 1})
            >>> row.language, "ignored" in row.to_mapping()
            ('English', False)


        :param row: Column-keyed mapping-like source supporting subscription and keys or
            iteration.
        :return: New instance of the concrete dataclass.
        """
        keys_method = getattr(row, "keys", None)
        row_keys = set(keys_method()) if callable(keys_method) else set(row)
        kwargs = {
            field.name: row[field.name]
            for field in fields(cls)
            if field.init and field.name in row_keys
        }
        return cls(**kwargs)

    @property
    def primary_id(self) -> int | None:
        """
        Read the configured id column only when it contains an exact int.

        Bool and numeric strings yield None; the value is not coerced.

        Example:
            >>> from LiuXin_alpha.metadata.containers import LanguageRow
            >>> LanguageRow(language_id=True).primary_id is None
            True


        :return: Integer primary id, or None.
        """
        value = getattr(self, self.ID_COLUMN)
        return value if type(value) is int else None

    def to_mapping(self) -> dict[str, MetadataRowValue]:
        """
        Copy constructor-backed dataclass fields into a column-keyed dictionary.

        Class variables and init=False fields are omitted. Values are retained by reference.

        Example:
            >>> from LiuXin_alpha.metadata.containers import LanguageRow
            >>> LanguageRow(language="English").to_mapping()["language"]
            'English'


        :return: New dictionary of stored row values.
        """
        return {
            field.name: getattr(self, field.name)
            for field in fields(self)
            if field.init
        }

    def __str__(self) -> str:
        """
        Summarize the id and selected nonempty fields using the compact mapping formatter.

        Example:
            >>> from LiuXin_alpha.metadata.containers import LanguageRow
            >>> str(LanguageRow(language_id=7, language="English"))
            "LanguageRow(language_id=7, language='English')"


        :return: Class-named diagnostic description.
        """
        return compact_mapping_string(
            self,
            self.to_mapping(),
            id_keys=(self.ID_COLUMN,),
        )


__all__ = [
    "MetadataRowMapping",
    "MetadataRowValue",
    "MetadataTableRow",
]
