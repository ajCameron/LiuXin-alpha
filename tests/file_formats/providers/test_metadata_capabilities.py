"""
Check the metadata capabilities and the form a metadata reading takes.

There are two models. Calibre fields use calibre's names and shapes; WEMI
fields are already placed by level. Each has its own reader and writer
classes under a family parent. These tests cover what each kind of reading
may hold, that it cannot change once made, that its document round-trips
exactly, that a WEMI reader is preferred over a calibre reader, and that
metadata providers are declared and found like every other provider.
"""

from __future__ import annotations

import dataclasses
import json
import math
from collections.abc import Mapping
from datetime import datetime
from pathlib import Path

import pytest

from LiuXin_alpha.file_formats.providers.base import (
    ProviderContext,
    ProviderDefinitionError,
)
from LiuXin_alpha.file_formats.providers.metadata import (
    WEMI_LEVELS,
    CalibreFields,
    CalibreMetadataReader,
    CalibreMetadataWriter,
    CoverExtractor,
    CoverImage,
    MetadataFieldsError,
    MetadataReader,
    MetadataWriter,
    WEMIFields,
    WEMIMetadataReader,
    WEMIMetadataWriter,
)
from LiuXin_alpha.file_formats.providers.registry import (
    ProviderRegistry,
    capabilities_of,
)
from tests.support.format_providers import Recorded, declare

READING: dict[str, object] = {
    "title": "EPUB Καλημέρα 世界",
    "authors": ["José", "Иван"],
    "identifiers": {"isbn": "9780000000002", "uuid": "1f0e"},
    "series_index": 2.5,
    "rating": 8,
    "languages": [],
    "comments": None,
    "pubdate": "2026-11-17T00:00:00+00:00",
    "user_metadata": {"#genre": {"value": ["novel"], "datatype": "text"}},
}

WEMI_READING: dict[str, object] = {
    "work": {
        "titles": ["EPUB Καλημέρα 世界"],
        "creators": [
            {"name": "José", "role": "authors"},
            {"name": "Иван", "role": "translators"},
        ],
    },
    "expression": {"languages": ["ell", "zho"]},
    "manifestation": {"identifiers": {"isbn": ["9780000000002"]}},
}


class EpubMetadata(Recorded, CalibreMetadataReader, CoverExtractor):
    """
    Read calibre metadata and the cover from EPUB, as one provider.

    Example:
        >>> sorted(EpubMetadata.calibre_read_formats)
        ['epub']
    """

    provider_id = "epub.metadata.test"
    calibre_read_formats = frozenset({"epub"})
    cover_source_formats = frozenset({"epub"})

    def read_calibre_metadata(
        self, source: Path, source_format: str, context: ProviderContext
    ) -> CalibreFields:
        """
        Return a fixed calibre reading.

        Example:
            >>> "read_calibre_metadata" in vars(EpubMetadata)
            True


        :param source: Unused.
        :param source_format: Unused.
        :param context: Unused.
        :return: The reading in :data:`READING`.
        """
        return CalibreFields(READING)

    def extract_cover(
        self, source: Path, source_format: str, context: ProviderContext
    ) -> CoverImage | None:
        """
        Return a fixed cover.

        Example:
            >>> "extract_cover" in vars(EpubMetadata)
            True


        :param source: Unused.
        :param source_format: Unused.
        :param context: Unused.
        :return: A one-byte PNG stand-in.
        """
        return CoverImage(b"\x89", "png")


class EpubWEMIMetadata(Recorded, WEMIMetadataReader):
    """
    Read WEMI-placed metadata from EPUB.

    Example:
        >>> sorted(EpubWEMIMetadata.wemi_read_formats)
        ['epub']
    """

    provider_id = "epub.wemi-metadata.test"
    wemi_read_formats = frozenset({"epub"})

    def read_wemi_metadata(
        self, source: Path, source_format: str, context: ProviderContext
    ) -> WEMIFields:
        """
        Return a fixed WEMI reading.

        Example:
            >>> "read_wemi_metadata" in vars(EpubWEMIMetadata)
            True


        :param source: Unused.
        :param source_format: Unused.
        :param context: Unused.
        :return: The reading in :data:`WEMI_READING`.
        """
        return WEMIFields(WEMI_READING)


class EpubWriters(Recorded, CalibreMetadataWriter, WEMIMetadataWriter):
    """
    Accept both kinds of EPUB metadata write without doing anything.

    Example:
        >>> sorted(EpubWriters.calibre_write_formats)
        ['epub']
    """

    provider_id = "epub.metadata-writers.test"
    calibre_write_formats = frozenset({"epub"})
    wemi_write_formats = frozenset({"epub"})

    def write_calibre_metadata(
        self,
        target: Path,
        target_format: str,
        fields: CalibreFields,
        context: ProviderContext,
    ) -> None:
        """
        Do nothing.

        Example:
            >>> "write_calibre_metadata" in vars(EpubWriters)
            True


        :param target: Unused.
        :param target_format: Unused.
        :param fields: Unused.
        :param context: Unused.
        :return: None.
        """

    def write_wemi_metadata(
        self,
        target: Path,
        target_format: str,
        fields: WEMIFields,
        context: ProviderContext,
    ) -> None:
        """
        Do nothing.

        Example:
            >>> "write_wemi_metadata" in vars(EpubWriters)
            True


        :param target: Unused.
        :param target_format: Unused.
        :param fields: Unused.
        :param context: Unused.
        :return: None.
        """


def test_calibre_fields_hold_frozen_json() -> None:
    """
    Store lists as tuples and objects as read-only mappings.

    Example:
        >>> test_calibre_fields_hold_frozen_json()


    :return: None after each kind of value is checked.
    """
    fields = CalibreFields(READING)
    assert CalibreFields.model == "calibre"
    assert fields.values["authors"] == ("José", "Иван")
    assert fields.values["languages"] == ()
    assert fields.values["comments"] is None
    identifiers = fields.values["identifiers"]
    assert isinstance(identifiers, Mapping)
    assert dict(identifiers) == {"isbn": "9780000000002", "uuid": "1f0e"}
    genre = fields.values["user_metadata"]
    assert isinstance(genre, Mapping)
    assert genre["#genre"] == {"value": ("novel",), "datatype": "text"}


def test_fields_cannot_change_after_they_are_made() -> None:
    """
    Refuse changes to either kind of fields and the values inside them.

    A change to the dictionary the fields were made from does not reach them
    either.

    Example:
        >>> test_fields_cannot_change_after_they_are_made()


    :return: None after each kind of change is refused.
    """
    source: dict[str, object] = {"tags": ["a"], "identifiers": {"isbn": "1"}}
    fields = CalibreFields(source)
    source["tags"] = ["changed"]
    assert fields.values["tags"] == ("a",)
    with pytest.raises(dataclasses.FrozenInstanceError):
        fields.values = {}
    with pytest.raises(TypeError):
        fields.values["title"] = "Other"
    identifiers = fields.values["identifiers"]
    assert isinstance(identifiers, Mapping)
    with pytest.raises(TypeError):
        identifiers["isbn"] = "2"

    wemi = WEMIFields(WEMI_READING)
    with pytest.raises(TypeError):
        wemi.levels["item"] = {}
    with pytest.raises(TypeError):
        wemi.levels["work"]["titles"] = ()


@pytest.mark.parametrize(
    ("kind", "values"),
    [(CalibreFields, READING), (WEMIFields, WEMI_READING)],
)
def test_the_document_round_trips_exactly(
    kind: type[CalibreFields] | type[WEMIFields], values: dict[str, object]
) -> None:
    """
    Write a reading as JSON and read the same reading back.

    Example:
        >>> test_the_document_round_trips_exactly(CalibreFields, READING)


    :param kind: The fields class.
    :param values: What to build it from.
    :return: None after the document and the round trip are checked.
    """
    fields = kind(values)
    document = fields.to_document()
    assert "Καλημέρα" in document and "\\u" not in document
    assert list(json.loads(document)) == sorted(values)
    assert kind.from_document(document) == fields
    assert kind.from_document(document).to_document() == document


@pytest.mark.parametrize(
    ("values", "message"),
    [
        ({"": "x"}, "non-empty text"),
        ({"  ": "x"}, "non-empty text"),
        ({1: "x"}, "non-empty text"),
        ({"pubdate": datetime(2026, 11, 17)}, "pubdate holds a datetime"),
        ({"tags": {"a", "b"}}, "tags holds a set"),
        ({"cover_data": b"\x89PNG"}, "cover_data holds a bytes"),
        ({"rating": math.inf}, "rating holds a float that is not finite"),
        ({"rating": math.nan}, "rating holds a float that is not finite"),
        ({"identifiers": {1: "x"}}, "identifiers has a key that is not text"),
        ({"authors": ["a", object()]}, r"authors\[1\] holds an object"),
    ],
)
def test_calibre_fields_refuse_what_json_cannot_hold(
    values: dict[object, object], message: str
) -> None:
    """
    Refuse names and values a JSON document could not store faithfully.

    Example:
        >>> test_calibre_fields_refuse_what_json_cannot_hold({"": 1}, "non-empty")  # doctest: +SKIP


    :param values: A mapping with one unacceptable name or value.
    :param message: Part of the expected error message.
    :return: None after the mapping is refused.
    """
    with pytest.raises(MetadataFieldsError, match=message):
        CalibreFields(values)


@pytest.mark.parametrize(
    ("levels", "message"),
    [
        ({"edition": {"title": "x"}}, "'edition' is not a WEMI level"),
        ({"Work": {"title": "x"}}, "'Work' is not a WEMI level"),
        ({"work": ["A Book"]}, "work fields must be a mapping"),
        ({"item": {"": 1}}, "a field name in item fields"),
        ({"manifestation": {"pubdate": datetime(2026, 1, 1)}}, "pubdate holds"),
        (["work"], "levels must be a mapping"),
    ],
)
def test_wemi_fields_take_only_the_four_levels(levels: object, message: str) -> None:
    """
    Refuse unknown levels and malformed fields within a level.

    Example:
        >>> test_wemi_fields_take_only_the_four_levels({"edition": {}}, "edition")  # doctest: +SKIP


    :param levels: Something that is not a valid WEMI reading.
    :param message: Part of the expected error message.
    :return: None after it is refused.
    """
    with pytest.raises(MetadataFieldsError, match=message):
        WEMIFields(levels)


def test_wemi_fields_keep_levels_apart() -> None:
    """
    Hold each level's fields separately, and allow levels to be absent.

    Example:
        >>> test_wemi_fields_keep_levels_apart()


    :return: None after the levels are read back.
    """
    fields = WEMIFields(WEMI_READING)
    assert WEMIFields.model == "wemi"
    assert WEMI_LEVELS == ("work", "expression", "manifestation", "item")
    assert set(fields.levels) == {"work", "expression", "manifestation"}
    creators = fields.levels["work"]["creators"]
    assert isinstance(creators, tuple)
    assert creators[1] == {"name": "Иван", "role": "translators"}
    assert WEMIFields({}).to_document() == "{}"


def test_a_stored_document_must_be_an_object() -> None:
    """
    Refuse a stored document that is not a JSON object.

    Example:
        >>> test_a_stored_document_must_be_an_object()


    :return: None after an array and broken JSON are refused.
    """
    for kind in (CalibreFields, WEMIFields):
        with pytest.raises(MetadataFieldsError, match="JSON object"):
            kind.from_document('["title"]')
        with pytest.raises(json.JSONDecodeError):
            kind.from_document("{title")


def test_a_cover_needs_bytes_and_a_format_name() -> None:
    """
    Refuse an empty cover, text in place of bytes and an unnormalised format.

    Example:
        >>> test_a_cover_needs_bytes_and_a_format_name()


    :return: None after each bad cover is refused.
    """
    assert CoverImage(b"\xff\xd8", "jpeg").image_format == "jpeg"
    for data, image_format in [(b"", "png"), ("png", "png"), (b"x", ".PNG")]:
        with pytest.raises(ValueError):
            CoverImage(data, image_format)


@pytest.mark.parametrize(
    ("base", "attribute"),
    [
        (CalibreMetadataReader, "calibre_read_formats"),
        (WEMIMetadataReader, "wemi_read_formats"),
        (CalibreMetadataWriter, "calibre_write_formats"),
        (WEMIMetadataWriter, "wemi_write_formats"),
        (CoverExtractor, "cover_source_formats"),
    ],
)
def test_metadata_providers_declare_their_formats(base: type, attribute: str) -> None:
    """
    Require each metadata capability to declare normalised formats.

    Example:
        >>> test_metadata_providers_declare_their_formats(CoverExtractor, "cover_source_formats")  # doctest: +SKIP


    :param base: The capability class.
    :param attribute: The format attribute it requires.
    :return: None after a missing and a malformed declaration are refused.
    """
    with pytest.raises(ProviderDefinitionError, match=f"{attribute} must be set"):
        declare("Missing", Recorded, base).validate_declaration()
    with pytest.raises(ProviderDefinitionError, match=attribute):
        declare(
            "Dotted", Recorded, base, **{attribute: frozenset({".pdf"})}
        ).validate_declaration()
    declare(
        "Good", Recorded, base, **{attribute: frozenset({"pdf"})}
    ).validate_declaration()


def test_family_parents_are_not_capabilities() -> None:
    """
    Refuse a provider that subclasses only a family parent.

    Example:
        >>> test_family_parents_are_not_capabilities()


    :return: None after both family parents are refused.
    """
    registry = ProviderRegistry()
    for family in (MetadataReader, MetadataWriter):
        with pytest.raises(ProviderDefinitionError, match="offers no capability"):
            registry.register(declare(f"Only{family.__name__}", Recorded, family))


def test_the_registry_finds_metadata_providers_by_capability() -> None:
    """
    File each provider under every capability it offers.

    Example:
        >>> test_the_registry_finds_metadata_providers_by_capability()


    :return: None after each metadata query is run.
    """
    registry = ProviderRegistry()
    for provider in (EpubMetadata, EpubWEMIMetadata, EpubWriters):
        registry.register(provider)

    assert capabilities_of(EpubMetadata) == (CalibreMetadataReader, CoverExtractor)
    assert capabilities_of(EpubWriters) == (CalibreMetadataWriter, WEMIMetadataWriter)
    assert registry.calibre_metadata_readers(".EPUB") == (EpubMetadata,)
    assert registry.wemi_metadata_readers("epub") == (EpubWEMIMetadata,)
    assert registry.cover_extractors("epub") == (EpubMetadata,)
    assert registry.calibre_metadata_writers("epub") == (EpubWriters,)
    assert registry.wemi_metadata_writers("epub") == (EpubWriters,)
    assert registry.calibre_metadata_readers("pdf") == ()
    assert registry.oeb_importers("epub") == ()


def test_wemi_readers_come_before_calibre_readers() -> None:
    """
    List WEMI readers first, whatever the priorities and registration order.

    A WEMI reading of a file outranks a calibre reading of the same file.

    Example:
        >>> test_wemi_readers_come_before_calibre_readers()


    :return: None after mixed readers are listed.
    """
    favoured_calibre = declare(
        "FavouredCalibre", EpubMetadata, provider_id="epub.favoured", priority=50
    )
    both = declare(
        "Both",
        EpubWEMIMetadata,
        CalibreMetadataReader,
        provider_id="epub.both",
        calibre_read_formats=frozenset({"epub"}),
        read_calibre_metadata=EpubMetadata.read_calibre_metadata,
    )
    registry = ProviderRegistry()
    for provider in (favoured_calibre, EpubMetadata, EpubWEMIMetadata, both):
        registry.register(provider)

    assert [found.provider_id for found in registry.metadata_readers("epub")] == [
        "epub.wemi-metadata.test",
        "epub.both",
        "epub.favoured",
        "epub.metadata.test",
    ]


def test_readers_return_their_model_and_extractors_a_cover(tmp_path: Path) -> None:
    """
    Call each capability of the test providers.

    Example:
        >>> test_readers_return_their_model_and_extractors_a_cover(Path.cwd())  # doctest: +SKIP


    :param tmp_path: Pytest scratch folder standing in for the file.
    :return: None after each result is checked.
    """
    book = tmp_path / "book.epub"
    calibre = EpubMetadata().read_calibre_metadata(book, "epub", None)
    wemi = EpubWEMIMetadata().read_wemi_metadata(book, "epub", None)
    cover = EpubMetadata().extract_cover(book, "epub", None)
    assert calibre.values["title"] == READING["title"]
    assert wemi.levels["expression"]["languages"] == ("ell", "zho")
    assert cover == CoverImage(b"\x89", "png")
