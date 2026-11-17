"""
Metadata capabilities: reading metadata, writing it, and extracting covers.

A metadata reader returns everything it found in a file as
:class:`MetadataFields`: a mapping from calibre's field names (``title``,
``authors``, ``series``, ``identifiers``, ``pubdate`` and so on) to values that
JSON can hold. The whole mapping is what the library keeps as the reading for
that file, so nothing a reader says is lost even when the catalogue does not
accept it. Which WEMI level each field belongs to is decided when a value is
accepted, not here.

A metadata writer takes fields in the same form and writes them into a file. A
cover extractor returns the cover image a file carries. Covers are kept out of
the fields because images do not belong in a JSON document.

Nothing in this module reads a file or touches the database. The decisions
behind it are recorded in the format-provider briefing under "Readings and
claims".
"""

from __future__ import annotations

import json
import math
from abc import abstractmethod
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import ClassVar, Self, cast

from LiuXin_alpha.file_formats.providers.base import (
    Provider,
    ProviderContext,
    declared_formats,
    is_format_name,
)

type JSONValue = (
    None | bool | int | float | str | tuple[JSONValue, ...] | Mapping[str, JSONValue]
)
"""A value JSON can hold, with arrays as tuples and objects as read-only mappings."""


class MetadataFieldsError(ValueError):
    """
    Report a field name or value that cannot be part of a reading.

    Example:
        >>> try:
        ...     MetadataFields({"pubdate": object()})
        ... except MetadataFieldsError as error:
        ...     print(error)
        pubdate holds an object, which JSON cannot hold
    """


def frozen_json(value: object, where: str) -> JSONValue:
    """
    Check that a value is JSON-compatible and make an unchangeable copy of it.

    Lists and tuples become tuples, and objects become read-only mappings with
    string keys. Floats must be finite, because JSON has no NaN or infinity.
    Dates and other richer values must already be text: a date is written in
    ISO 8601 form.

    Example:
        >>> frozen_json(["José", "Иван"], "authors")
        ('José', 'Иван')
        >>> dict(frozen_json({"isbn": "9780000000002"}, "identifiers"))
        {'isbn': '9780000000002'}
        >>> try:
        ...     frozen_json(float("nan"), "rating")
        ... except MetadataFieldsError as error:
        ...     print(error)
        rating holds a float that is not finite


    :param value: The candidate, of any type.
    :param where: The field and position, used in error messages.
    :return: The value with lists as tuples and objects as read-only mappings.
    :raises MetadataFieldsError: If the value, or anything inside it, cannot be
        held by JSON.
    """
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise MetadataFieldsError(f"{where} holds a float that is not finite")
        return value
    if isinstance(value, (list, tuple)):
        items = cast("list[object] | tuple[object, ...]", value)
        return tuple(
            frozen_json(item, f"{where}[{index}]") for index, item in enumerate(items)
        )
    if isinstance(value, Mapping):
        entries = cast("Mapping[object, object]", value)
        copied: dict[str, JSONValue] = {}
        for key, item in entries.items():
            if not isinstance(key, str):
                raise MetadataFieldsError(
                    f"{where} has a key that is not text: {key!r}"
                )
            copied[key] = frozen_json(item, f"{where}.{key}")
        return MappingProxyType(copied)
    raise MetadataFieldsError(
        f"{where} holds {_article(type(value).__name__)}, which JSON cannot hold"
    )


def thawed_json(value: JSONValue) -> object:
    """
    Turn a frozen value back into plain lists and dictionaries for ``json``.

    Example:
        >>> thawed_json(frozen_json({"tags": ["a", "b"]}, "x"))
        {'tags': ['a', 'b']}


    :param value: A value made by :func:`frozen_json`.
    :return: The same value with tuples as lists and mappings as dictionaries.
    """
    if isinstance(value, tuple):
        return [thawed_json(item) for item in value]
    if isinstance(value, Mapping):
        return {key: thawed_json(item) for key, item in value.items()}
    return value


def _article(noun: str) -> str:
    """
    Put "a" or "an" in front of a type name.

    Example:
        >>> _article("object"), _article("set")
        ('an object', 'a set')


    :param noun: The type name.
    :return: The name with its indefinite article.
    """
    return f"an {noun}" if noun[:1].lower() in "aeiou" else f"a {noun}"


@dataclass(frozen=True, slots=True)
class MetadataFields:
    """
    Hold metadata as calibre's field names mapped to JSON-compatible values.

    This is what a metadata reader returns and what a metadata writer is
    given. Field names are calibre's, because every existing reader already
    uses them. Values are frozen on construction, so a value cannot change
    after it has been read. Absent fields mean the source said nothing about
    them; a field set to ``None`` means the source said it was empty.

    Example:
        >>> fields = MetadataFields(
        ...     {"title": "A Book", "authors": ["José", "Иван"], "series_index": 2.0}
        ... )
        >>> fields.values["authors"]
        ('José', 'Иван')
        >>> fields.to_document()
        '{"authors": ["José", "Иван"], "series_index": 2.0, "title": "A Book"}'


    :param values: Field names mapped to their values. Lists may be given as
        lists; they are stored as tuples.
    """

    values: Mapping[str, JSONValue]

    def __post_init__(self: Self) -> None:
        """
        Check every field name and freeze every value.

        Example:
            >>> try:
            ...     MetadataFields({"": "A Book"})
            ... except MetadataFieldsError as error:
            ...     print(error)
            a field name must be non-empty text, got ''


        :return: None.
        :raises MetadataFieldsError: If a name is not text, is blank, or a value
            cannot be held by JSON.
        """
        given = cast(object, self.values)
        if not isinstance(given, Mapping):
            raise MetadataFieldsError(f"fields must be a mapping, got {given!r}")
        entries = cast("Mapping[object, object]", given)
        frozen: dict[str, JSONValue] = {}
        for name, value in entries.items():
            if not isinstance(name, str) or not name.strip():
                raise MetadataFieldsError(
                    f"a field name must be non-empty text, got {name!r}"
                )
            frozen[name] = frozen_json(value, name)
        object.__setattr__(self, "values", MappingProxyType(frozen))

    def to_document(self: Self) -> str:
        """
        Write the fields as the JSON document stored for a reading.

        Keys are sorted and non-ASCII text is kept as it is, so the same fields
        always give the same document.

        Example:
            >>> MetadataFields({"title": "世界", "languages": ["zho"]}).to_document()
            '{"languages": ["zho"], "title": "世界"}'


        :return: The document.
        """
        return json.dumps(thawed_json(self.values), ensure_ascii=False, sort_keys=True)

    @classmethod
    def from_document(cls: type[Self], document: str) -> Self:
        """
        Read fields back from a stored reading's JSON document.

        Example:
            >>> MetadataFields.from_document('{"title": "A Book"}').values["title"]
            'A Book'
            >>> try:
            ...     MetadataFields.from_document('["A Book"]')
            ... except MetadataFieldsError as error:
            ...     print(error)
            a reading document must be a JSON object


        :param document: The JSON text.
        :return: The fields it holds.
        :raises MetadataFieldsError: If the text is not a JSON object of fields.
        :raises json.JSONDecodeError: If the text is not JSON at all.
        """
        loaded: object = json.loads(document)
        if not isinstance(loaded, dict):
            raise MetadataFieldsError("a reading document must be a JSON object")
        return cls(cast("dict[str, JSONValue]", loaded))


@dataclass(frozen=True, slots=True)
class CoverImage:
    """
    Hold a cover image and the format its bytes are in.

    Example:
        >>> cover = CoverImage(b"\\x89PNG...", "png")
        >>> cover.image_format, len(cover.data)
        ('png', 7)


    :param data: The image file's bytes.
    :param image_format: The image format in normal form, such as ``jpeg`` or
        ``png``.
    """

    data: bytes
    image_format: str

    def __post_init__(self: Self) -> None:
        """
        Refuse an empty image or a format name not in normal form.

        Example:
            >>> try:
            ...     CoverImage(b"", "png")
            ... except ValueError as error:
            ...     print(error)
            a cover image must have bytes


        :return: None.
        :raises ValueError: If ``data`` is empty or not bytes, or the format is
            not a lower-case name without a leading dot.
        """
        data = cast(object, self.data)
        if not isinstance(data, bytes) or not data:
            raise ValueError("a cover image must have bytes")
        if not is_format_name(self.image_format):
            raise ValueError(
                "a cover image format must be a lower-case name without a "
                f"leading dot, got {self.image_format!r}"
            )


class MetadataReader(Provider):
    """
    Read the metadata a file carries.

    A subclass sets ``metadata_read_formats`` and implements
    :meth:`read_metadata`.

    Example:
        >>> class TitleReader(MetadataReader):
        ...     provider_id = "txt.title-reader.example"
        ...     version = "1"
        ...     licence = "GPL-3.0-only"
        ...     metadata_read_formats = frozenset({"txt"})
        ...     def read_metadata(self, source, source_format, context):
        ...         return MetadataFields({"title": source.stem})
        >>> TitleReader.validate_declaration()
        >>> TitleReader().read_metadata(Path("A Book.txt"), "txt", None).values
        mappingproxy({'title': 'A Book'})
    """

    metadata_read_formats: ClassVar[frozenset[str]]

    @classmethod
    def validate_declaration(cls: type[Self]) -> None:
        """
        Check the shared record and the formats this reader reads.

        Example:
            >>> class Bad(MetadataReader):
            ...     provider_id = "bad.example"
            ...     version = "1"
            ...     licence = "GPL-3.0-only"
            >>> from LiuXin_alpha.file_formats.providers.base import (
            ...     ProviderDefinitionError,
            ... )
            >>> try:
            ...     Bad.validate_declaration()
            ... except ProviderDefinitionError as error:
            ...     print(error)
            Bad.metadata_read_formats must be set


        :return: None when the declaration is well formed.
        :raises ProviderDefinitionError: If an attribute is missing or malformed.
        """
        super().validate_declaration()
        declared_formats(cls, "metadata_read_formats")

    @abstractmethod
    def read_metadata(
        self: Self, source: Path, source_format: str, context: ProviderContext
    ) -> MetadataFields:
        """
        Return everything the file says about the book.

        Report every field the file gives, including values the reader thinks
        doubtful; the library decides what to accept. Leave out fields the
        file says nothing about, and leave out the cover.

        Example:
            A runner stores the result as the reading for this file::

                fields = reader.read_metadata(Path("book.epub"), "epub", context)


        :param source: The file to read. It must not be changed.
        :param source_format: Which of ``metadata_read_formats`` it is in.
        :param context: Options, log, report and work folder for this call.
        :return: The fields found, under calibre's names.
        """


class MetadataWriter(Provider):
    """
    Write metadata into a file.

    A subclass sets ``metadata_write_formats`` and implements
    :meth:`write_metadata`.

    Example:
        >>> class Noop(MetadataWriter):
        ...     provider_id = "txt.noop-writer.example"
        ...     version = "1"
        ...     licence = "GPL-3.0-only"
        ...     metadata_write_formats = frozenset({"txt"})
        ...     def write_metadata(self, target, target_format, fields, context):
        ...         pass
        >>> Noop.validate_declaration()
    """

    metadata_write_formats: ClassVar[frozenset[str]]

    @classmethod
    def validate_declaration(cls: type[Self]) -> None:
        """
        Check the shared record and the formats this writer writes.

        Example:
            >>> class Bad(MetadataWriter):
            ...     provider_id = "bad.example"
            ...     version = "1"
            ...     licence = "GPL-3.0-only"
            ...     metadata_write_formats = frozenset({".EPUB"})
            >>> from LiuXin_alpha.file_formats.providers.base import (
            ...     ProviderDefinitionError,
            ... )
            >>> try:
            ...     Bad.validate_declaration()
            ... except ProviderDefinitionError as error:
            ...     print(error)
            Bad.metadata_write_formats must hold lower-case format names with no leading dot, got '.EPUB'


        :return: None when the declaration is well formed.
        :raises ProviderDefinitionError: If an attribute is missing or malformed.
        """
        super().validate_declaration()
        declared_formats(cls, "metadata_write_formats")

    @abstractmethod
    def write_metadata(
        self: Self,
        target: Path,
        target_format: str,
        fields: MetadataFields,
        context: ProviderContext,
    ) -> None:
        """
        Write fields into a file, changing it in place.

        Fields not in ``fields`` are left as the file has them. The file is
        changed in place, so a caller passes a copy: an archive's original
        files are never written to.

        Example:
            A runner writes into a copy it has just made::

                writer.write_metadata(copy, "epub", fields, context)


        :param target: The file to change.
        :param target_format: Which of ``metadata_write_formats`` it is in.
        :param fields: The fields to write, under calibre's names.
        :param context: Options, log, report and work folder for this call.
        :return: None.
        """


class CoverExtractor(Provider):
    """
    Extract the cover image a file carries.

    A subclass sets ``cover_source_formats`` and implements
    :meth:`extract_cover`. A metadata reader that also finds covers inherits
    both classes.

    Example:
        >>> class NoCover(CoverExtractor):
        ...     provider_id = "txt.no-cover.example"
        ...     version = "1"
        ...     licence = "GPL-3.0-only"
        ...     cover_source_formats = frozenset({"txt"})
        ...     def extract_cover(self, source, source_format, context):
        ...         return None
        >>> NoCover.validate_declaration()
        >>> NoCover().extract_cover(Path("book.txt"), "txt", None) is None
        True
    """

    cover_source_formats: ClassVar[frozenset[str]]

    @classmethod
    def validate_declaration(cls: type[Self]) -> None:
        """
        Check the shared record and the formats this extractor reads.

        Example:
            >>> class Bad(CoverExtractor):
            ...     provider_id = "bad.example"
            ...     version = "1"
            ...     licence = "GPL-3.0-only"
            ...     cover_source_formats = frozenset()
            >>> from LiuXin_alpha.file_formats.providers.base import (
            ...     ProviderDefinitionError,
            ... )
            >>> try:
            ...     Bad.validate_declaration()
            ... except ProviderDefinitionError as error:
            ...     print(error)
            Bad.cover_source_formats must be a non-empty frozenset of format names, got frozenset()


        :return: None when the declaration is well formed.
        :raises ProviderDefinitionError: If an attribute is missing or malformed.
        """
        super().validate_declaration()
        declared_formats(cls, "cover_source_formats")

    @abstractmethod
    def extract_cover(
        self: Self, source: Path, source_format: str, context: ProviderContext
    ) -> CoverImage | None:
        """
        Return the file's cover image, or ``None`` if it has none.

        Example:
            A runner stores the image beside the reading for the same file::

                cover = extractor.extract_cover(Path("book.epub"), "epub", context)


        :param source: The file to read. It must not be changed.
        :param source_format: Which of ``cover_source_formats`` it is in.
        :param context: Options, log, report and work folder for this call.
        :return: The cover, or ``None``.
        """
