"""
Metadata capabilities: reading metadata, writing it, and extracting covers.

Metadata comes in two models, and each has its own reader and writer classes:

- **calibre**: calibre's field names and value shapes (``title``, ``authors``
  as a list of names, ``identifiers`` as a mapping of scheme to value, dates
  as ISO 8601 text, and so on), held as :class:`CalibreFields`. Most existing
  readers produce calibre metadata. Which WEMI level each field belongs to is
  decided when a value is accepted into the catalogue.
- **WEMI**: values already placed at the Work, Expression, Manifestation or
  Item they describe, held as :class:`WEMIFields`. A WEMI reading needs no
  mapping, and it outranks a calibre reading of the same file.

:class:`MetadataReader` and :class:`MetadataWriter` are family parents; a
provider subclasses the class for its model. Covers are the same in both
models, so there is one :class:`CoverExtractor`. Covers are kept out of the
fields because images do not belong in a JSON document.

Either kind of fields is what the library keeps, whole, as the reading for a
file, so nothing a reader says is lost even when the catalogue does not accept
it. Nothing in this module reads a file or touches the database. The
decisions behind it are recorded in the format-provider briefing under
"Readings and claims".
"""

from __future__ import annotations

import json
import math
from abc import abstractmethod
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import ClassVar, Literal, Self, cast

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

type WEMILevel = Literal["work", "expression", "manifestation", "item"]
"""One of the four levels of the WEMI model."""

WEMI_LEVELS: tuple[WEMILevel, ...] = ("work", "expression", "manifestation", "item")
"""The WEMI levels, from the most abstract to the most concrete."""


class MetadataFieldsError(ValueError):
    """
    Report a field name or value that cannot be part of a reading.

    Example:
        >>> try:
        ...     CalibreFields({"pubdate": object()})
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


def frozen_fields(values: object, where: str) -> Mapping[str, JSONValue]:
    """
    Check a mapping of field names to values and make an unchangeable copy.

    Example:
        >>> dict(frozen_fields({"tags": ["a"]}, "fields"))
        {'tags': ('a',)}
        >>> try:
        ...     frozen_fields({"": 1}, "fields")
        ... except MetadataFieldsError as error:
        ...     print(error)
        a field name in fields must be non-empty text, got ''


    :param values: The candidate mapping, of any type.
    :param where: What the mapping is, used in error messages.
    :return: A read-only mapping with every value frozen.
    :raises MetadataFieldsError: If it is not a mapping, a name is not
        non-empty text, or a value cannot be held by JSON.
    """
    if not isinstance(values, Mapping):
        raise MetadataFieldsError(f"{where} must be a mapping, got {values!r}")
    entries = cast("Mapping[object, object]", values)
    frozen: dict[str, JSONValue] = {}
    for name, value in entries.items():
        if not isinstance(name, str) or not name.strip():
            raise MetadataFieldsError(
                f"a field name in {where} must be non-empty text, got {name!r}"
            )
        frozen[name] = frozen_json(value, name)
    return MappingProxyType(frozen)


def _document(value: JSONValue) -> str:
    """
    Write a frozen value as deterministic JSON text.

    Keys are sorted and non-ASCII text is kept as it is, so the same value
    always gives the same document.

    Example:
        >>> _document(frozen_json({"b": 1, "a": "世界"}, "x"))
        '{"a": "世界", "b": 1}'


    :param value: A value made by :func:`frozen_json`.
    :return: The JSON text.
    """
    return json.dumps(thawed_json(value), ensure_ascii=False, sort_keys=True)


def _object_document(document: str) -> dict[str, object]:
    """
    Read JSON text that must hold an object.

    Example:
        >>> _object_document('{"title": "A"}')
        {'title': 'A'}


    :param document: The JSON text.
    :return: The object, as a dictionary.
    :raises MetadataFieldsError: If the text holds anything but an object.
    :raises json.JSONDecodeError: If the text is not JSON at all.
    """
    loaded: object = json.loads(document)
    if not isinstance(loaded, dict):
        raise MetadataFieldsError("a reading document must be a JSON object")
    return cast("dict[str, object]", loaded)


@dataclass(frozen=True, slots=True)
class CalibreFields:
    """
    Hold calibre metadata: calibre's field names mapped to JSON values.

    This is what a calibre metadata reader returns and what a calibre metadata
    writer is given. Values follow calibre's own shapes as its metadata
    serialiser writes them, with dates as ISO 8601 text. Values are frozen on
    construction. An absent field means the source said nothing about it; a
    field set to ``None`` means the source said it was empty.

    Example:
        >>> fields = CalibreFields(
        ...     {"title": "A Book", "authors": ["José", "Иван"], "series_index": 2.0}
        ... )
        >>> fields.values["authors"]
        ('José', 'Иван')
        >>> fields.to_document()
        '{"authors": ["José", "Иван"], "series_index": 2.0, "title": "A Book"}'


    :param values: Field names mapped to their values. Lists may be given as
        lists; they are stored as tuples.
    """

    model: ClassVar[str] = "calibre"
    """The model a stored reading of these fields is recorded under."""

    values: Mapping[str, JSONValue]

    def __post_init__(self: Self) -> None:
        """
        Check every field name and freeze every value.

        Example:
            >>> try:
            ...     CalibreFields({"": "A Book"})
            ... except MetadataFieldsError as error:
            ...     print(error)
            a field name in calibre fields must be non-empty text, got ''


        :return: None.
        :raises MetadataFieldsError: If a name is not text, is blank, or a value
            cannot be held by JSON.
        """
        object.__setattr__(
            self, "values", frozen_fields(cast(object, self.values), "calibre fields")
        )

    def to_document(self: Self) -> str:
        """
        Write the fields as the JSON document stored for a reading.

        Example:
            >>> CalibreFields({"title": "世界", "languages": ["zho"]}).to_document()
            '{"languages": ["zho"], "title": "世界"}'


        :return: The document; the same fields always give the same text.
        """
        return _document(self.values)

    @classmethod
    def from_document(cls: type[Self], document: str) -> Self:
        """
        Read fields back from a stored reading's JSON document.

        Example:
            >>> CalibreFields.from_document('{"title": "A Book"}').values["title"]
            'A Book'
            >>> try:
            ...     CalibreFields.from_document('["A Book"]')
            ... except MetadataFieldsError as error:
            ...     print(error)
            a reading document must be a JSON object


        :param document: The JSON text.
        :return: The fields it holds.
        :raises MetadataFieldsError: If the text is not a JSON object of fields.
        :raises json.JSONDecodeError: If the text is not JSON at all.
        """
        return cls(cast("Mapping[str, JSONValue]", _object_document(document)))


@dataclass(frozen=True, slots=True)
class WEMIFields:
    """
    Hold metadata already placed at the WEMI level it describes.

    Each level present maps field names to JSON values: titles and creators
    with their roles at the Work, languages at the Expression, publisher and
    ISBN at the Manifestation, and so on. A level that is absent means the
    source said nothing about it. The field names inside each level follow
    the WEMI metadata bundles and are fixed with the first WEMI reader; this
    type checks only the levels and that every value is JSON.

    Example:
        >>> fields = WEMIFields({
        ...     "work": {"titles": ["A Book"], "creators": [{"name": "José", "role": "authors"}]},
        ...     "manifestation": {"identifiers": {"isbn": ["9780000000002"]}},
        ... })
        >>> sorted(fields.levels)
        ['manifestation', 'work']
        >>> fields.levels["manifestation"]["identifiers"]["isbn"]
        ('9780000000002',)


    :param levels: WEMI levels mapped to their fields.
    """

    model: ClassVar[str] = "wemi"
    """The model a stored reading of these fields is recorded under."""

    levels: Mapping[WEMILevel, Mapping[str, JSONValue]]

    def __post_init__(self: Self) -> None:
        """
        Check every level name and freeze every level's fields.

        Example:
            >>> try:
            ...     WEMIFields({"edition": {"title": "A Book"}})
            ... except MetadataFieldsError as error:
            ...     print(error)
            'edition' is not a WEMI level; use one of work, expression, manifestation, item


        :return: None.
        :raises MetadataFieldsError: If a level is unknown or its fields are
            not a mapping of non-empty names to JSON values.
        """
        given = cast(object, self.levels)
        if not isinstance(given, Mapping):
            raise MetadataFieldsError(f"levels must be a mapping, got {given!r}")
        entries = cast("Mapping[object, object]", given)
        frozen: dict[WEMILevel, Mapping[str, JSONValue]] = {}
        for level, fields in entries.items():
            if level not in WEMI_LEVELS:
                raise MetadataFieldsError(
                    f"{level!r} is not a WEMI level; use one of "
                    + ", ".join(WEMI_LEVELS)
                )
            frozen[level] = frozen_fields(fields, f"{level} fields")
        object.__setattr__(self, "levels", MappingProxyType(frozen))

    def to_document(self: Self) -> str:
        """
        Write the levels as the JSON document stored for a reading.

        Example:
            >>> WEMIFields({"work": {"titles": ["A"]}}).to_document()
            '{"work": {"titles": ["A"]}}'


        :return: The document; the same fields always give the same text.
        """
        return _document(cast("JSONValue", self.levels))

    @classmethod
    def from_document(cls: type[Self], document: str) -> Self:
        """
        Read levels back from a stored reading's JSON document.

        Example:
            >>> WEMIFields.from_document('{"item": {"page_count": 12}}').levels["item"]
            mappingproxy({'page_count': 12})


        :param document: The JSON text.
        :return: The fields it holds.
        :raises MetadataFieldsError: If the text is not a JSON object of levels.
        :raises json.JSONDecodeError: If the text is not JSON at all.
        """
        loaded = _object_document(document)
        return cls(cast("Mapping[WEMILevel, Mapping[str, JSONValue]]", loaded))


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
    Group the providers that read the metadata a file carries.

    A provider never subclasses this directly; it subclasses the class for the
    model it produces, :class:`CalibreMetadataReader` or
    :class:`WEMIMetadataReader`.

    Example:
        >>> issubclass(CalibreMetadataReader, MetadataReader)
        True
        >>> issubclass(WEMIMetadataReader, MetadataReader)
        True
    """


class CalibreMetadataReader(MetadataReader):
    """
    Read a file's metadata as calibre fields.

    A subclass sets ``calibre_read_formats`` and implements
    :meth:`read_calibre_metadata`.

    Example:
        >>> class TitleReader(CalibreMetadataReader):
        ...     provider_id = "txt.title-reader.example"
        ...     version = "1"
        ...     licence = "GPL-3.0-only"
        ...     calibre_read_formats = frozenset({"txt"})
        ...     def read_calibre_metadata(self, source, source_format, context):
        ...         return CalibreFields({"title": source.stem})
        >>> TitleReader.validate_declaration()
        >>> TitleReader().read_calibre_metadata(Path("A Book.txt"), "txt", None).values
        mappingproxy({'title': 'A Book'})
    """

    calibre_read_formats: ClassVar[frozenset[str]]

    @classmethod
    def validate_declaration(cls: type[Self]) -> None:
        """
        Check the shared record and the formats this reader reads.

        Example:
            >>> from LiuXin_alpha.file_formats.providers.base import (
            ...     ProviderDefinitionError,
            ... )
            >>> class Bad(CalibreMetadataReader):
            ...     provider_id = "bad.example"
            ...     version = "1"
            ...     licence = "GPL-3.0-only"
            >>> try:
            ...     Bad.validate_declaration()
            ... except ProviderDefinitionError as error:
            ...     print(error)
            Bad.calibre_read_formats must be set


        :return: None when the declaration is well formed.
        :raises ProviderDefinitionError: If an attribute is missing or malformed.
        """
        super().validate_declaration()
        declared_formats(cls, "calibre_read_formats")

    @abstractmethod
    def read_calibre_metadata(
        self: Self, source: Path, source_format: str, context: ProviderContext
    ) -> CalibreFields:
        """
        Return everything the file says about the book, as calibre fields.

        Report every field the file gives, including values the reader thinks
        doubtful; the library decides what to accept. Leave out fields the
        file says nothing about, and leave out the cover.

        Example:
            A runner stores the result as a calibre reading for this file::

                fields = reader.read_calibre_metadata(Path("b.epub"), "epub", context)


        :param source: The file to read. It must not be changed.
        :param source_format: Which of ``calibre_read_formats`` it is in.
        :param context: Options, log, report and work folder for this call.
        :return: The fields found.
        """


class WEMIMetadataReader(MetadataReader):
    """
    Read a file's metadata already placed at its WEMI levels.

    A subclass sets ``wemi_read_formats`` and implements
    :meth:`read_wemi_metadata`. A WEMI reading of a file outranks a calibre
    reading of the same file.

    Example:
        >>> class OpfReader(WEMIMetadataReader):
        ...     provider_id = "opf.wemi-reader.example"
        ...     version = "1"
        ...     licence = "GPL-3.0-only"
        ...     wemi_read_formats = frozenset({"opf"})
        ...     def read_wemi_metadata(self, source, source_format, context):
        ...         return WEMIFields({"work": {"titles": [source.stem]}})
        >>> OpfReader.validate_declaration()
    """

    wemi_read_formats: ClassVar[frozenset[str]]

    @classmethod
    def validate_declaration(cls: type[Self]) -> None:
        """
        Check the shared record and the formats this reader reads.

        Example:
            >>> from LiuXin_alpha.file_formats.providers.base import (
            ...     ProviderDefinitionError,
            ... )
            >>> class Bad(WEMIMetadataReader):
            ...     provider_id = "bad.example"
            ...     version = "1"
            ...     licence = "GPL-3.0-only"
            ...     wemi_read_formats = frozenset({".OPF"})
            >>> try:
            ...     Bad.validate_declaration()
            ... except ProviderDefinitionError as error:
            ...     print(error)
            Bad.wemi_read_formats must hold lower-case format names with no leading dot, got '.OPF'


        :return: None when the declaration is well formed.
        :raises ProviderDefinitionError: If an attribute is missing or malformed.
        """
        super().validate_declaration()
        declared_formats(cls, "wemi_read_formats")

    @abstractmethod
    def read_wemi_metadata(
        self: Self, source: Path, source_format: str, context: ProviderContext
    ) -> WEMIFields:
        """
        Return everything the file says about the book, placed by WEMI level.

        As for calibre readers: report every value the file gives, leave out
        what it does not say, and leave out the cover.

        Example:
            A runner stores the result as a WEMI reading for this file::

                fields = reader.read_wemi_metadata(Path("b.epub"), "epub", context)


        :param source: The file to read. It must not be changed.
        :param source_format: Which of ``wemi_read_formats`` it is in.
        :param context: Options, log, report and work folder for this call.
        :return: The fields found, by level.
        """


class MetadataWriter(Provider):
    """
    Group the providers that write metadata into a file.

    A provider never subclasses this directly; it subclasses the class for the
    model it takes, :class:`CalibreMetadataWriter` or
    :class:`WEMIMetadataWriter`.

    Writers change the file in place, so a caller passes a copy: an archive's
    original files are never written to. Fields not given are left as the
    file has them.

    Example:
        >>> issubclass(CalibreMetadataWriter, MetadataWriter)
        True
        >>> issubclass(WEMIMetadataWriter, MetadataWriter)
        True
    """


class CalibreMetadataWriter(MetadataWriter):
    """
    Write calibre fields into a file.

    A subclass sets ``calibre_write_formats`` and implements
    :meth:`write_calibre_metadata`.

    Example:
        >>> class Noop(CalibreMetadataWriter):
        ...     provider_id = "txt.noop-writer.example"
        ...     version = "1"
        ...     licence = "GPL-3.0-only"
        ...     calibre_write_formats = frozenset({"txt"})
        ...     def write_calibre_metadata(self, target, target_format, fields, context):
        ...         pass
        >>> Noop.validate_declaration()
    """

    calibre_write_formats: ClassVar[frozenset[str]]

    @classmethod
    def validate_declaration(cls: type[Self]) -> None:
        """
        Check the shared record and the formats this writer writes.

        Example:
            >>> from LiuXin_alpha.file_formats.providers.base import (
            ...     ProviderDefinitionError,
            ... )
            >>> class Bad(CalibreMetadataWriter):
            ...     provider_id = "bad.example"
            ...     version = "1"
            ...     licence = "GPL-3.0-only"
            ...     calibre_write_formats = frozenset({".EPUB"})
            >>> try:
            ...     Bad.validate_declaration()
            ... except ProviderDefinitionError as error:
            ...     print(error)
            Bad.calibre_write_formats must hold lower-case format names with no leading dot, got '.EPUB'


        :return: None when the declaration is well formed.
        :raises ProviderDefinitionError: If an attribute is missing or malformed.
        """
        super().validate_declaration()
        declared_formats(cls, "calibre_write_formats")

    @abstractmethod
    def write_calibre_metadata(
        self: Self,
        target: Path,
        target_format: str,
        fields: CalibreFields,
        context: ProviderContext,
    ) -> None:
        """
        Write calibre fields into a file, changing it in place.

        Example:
            A runner writes into a copy it has just made::

                writer.write_calibre_metadata(copy, "epub", fields, context)


        :param target: The file to change; never an archive's original.
        :param target_format: Which of ``calibre_write_formats`` it is in.
        :param fields: The fields to write.
        :param context: Options, log, report and work folder for this call.
        :return: None.
        """


class WEMIMetadataWriter(MetadataWriter):
    """
    Write metadata placed by WEMI level into a file.

    A subclass sets ``wemi_write_formats`` and implements
    :meth:`write_wemi_metadata`.

    Example:
        >>> class Noop(WEMIMetadataWriter):
        ...     provider_id = "opf.noop-writer.example"
        ...     version = "1"
        ...     licence = "GPL-3.0-only"
        ...     wemi_write_formats = frozenset({"opf"})
        ...     def write_wemi_metadata(self, target, target_format, fields, context):
        ...         pass
        >>> Noop.validate_declaration()
    """

    wemi_write_formats: ClassVar[frozenset[str]]

    @classmethod
    def validate_declaration(cls: type[Self]) -> None:
        """
        Check the shared record and the formats this writer writes.

        Example:
            >>> from LiuXin_alpha.file_formats.providers.base import (
            ...     ProviderDefinitionError,
            ... )
            >>> class Bad(WEMIMetadataWriter):
            ...     provider_id = "bad.example"
            ...     version = "1"
            ...     licence = "GPL-3.0-only"
            >>> try:
            ...     Bad.validate_declaration()
            ... except ProviderDefinitionError as error:
            ...     print(error)
            Bad.wemi_write_formats must be set


        :return: None when the declaration is well formed.
        :raises ProviderDefinitionError: If an attribute is missing or malformed.
        """
        super().validate_declaration()
        declared_formats(cls, "wemi_write_formats")

    @abstractmethod
    def write_wemi_metadata(
        self: Self,
        target: Path,
        target_format: str,
        fields: WEMIFields,
        context: ProviderContext,
    ) -> None:
        """
        Write fields placed by WEMI level into a file, changing it in place.

        Example:
            A runner writes into a copy it has just made::

                writer.write_wemi_metadata(copy, "opf", fields, context)


        :param target: The file to change; never an archive's original.
        :param target_format: Which of ``wemi_write_formats`` it is in.
        :param fields: The fields to write, by level.
        :param context: Options, log, report and work folder for this call.
        :return: None.
        """


class CoverExtractor(Provider):
    """
    Extract the cover image a file carries.

    A subclass sets ``cover_source_formats`` and implements
    :meth:`extract_cover`. A metadata reader that also finds covers inherits
    this class as well as its reader class.

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
