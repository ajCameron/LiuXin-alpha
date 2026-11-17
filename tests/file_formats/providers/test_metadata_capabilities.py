"""
Check the metadata capabilities and the form a metadata reading takes.

A reading is calibre-named fields with JSON-compatible values, stored whole as
one document. These tests cover what a reading may hold, that it cannot
change once made, that its document round-trips exactly, and that metadata
providers are declared and found like every other provider.
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
    CoverExtractor,
    CoverImage,
    MetadataFields,
    MetadataFieldsError,
    MetadataReader,
    MetadataWriter,
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


class EpubMetadata(Recorded, MetadataReader, CoverExtractor):
    """
    Read EPUB metadata and its cover, as one provider offering both.

    Example:
        >>> sorted(EpubMetadata.metadata_read_formats)
        ['epub']
    """

    provider_id = "epub.metadata.test"
    metadata_read_formats = frozenset({"epub"})
    cover_source_formats = frozenset({"epub"})

    def read_metadata(
        self, source: Path, source_format: str, context: ProviderContext
    ) -> MetadataFields:
        """
        Return a fixed reading.

        Example:
            >>> "read_metadata" in vars(EpubMetadata)
            True


        :param source: Unused.
        :param source_format: Unused.
        :param context: Unused.
        :return: The reading in :data:`READING`.
        """
        return MetadataFields(READING)

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


class EpubWriter(Recorded, MetadataWriter):
    """
    Accept EPUB metadata writes without doing anything.

    Example:
        >>> sorted(EpubWriter.metadata_write_formats)
        ['epub']
    """

    provider_id = "epub.metadata-writer.test"
    metadata_write_formats = frozenset({"epub"})

    def write_metadata(
        self,
        target: Path,
        target_format: str,
        fields: MetadataFields,
        context: ProviderContext,
    ) -> None:
        """
        Do nothing.

        Example:
            >>> "write_metadata" in vars(EpubWriter)
            True


        :param target: Unused.
        :param target_format: Unused.
        :param fields: Unused.
        :param context: Unused.
        :return: None.
        """


def test_a_reading_holds_calibre_fields_as_frozen_json() -> None:
    """
    Store lists as tuples and objects as read-only mappings.

    Example:
        >>> test_a_reading_holds_calibre_fields_as_frozen_json()


    :return: None after each kind of value is checked.
    """
    fields = MetadataFields(READING)
    assert fields.values["authors"] == ("José", "Иван")
    assert fields.values["languages"] == ()
    assert fields.values["comments"] is None
    identifiers = fields.values["identifiers"]
    assert isinstance(identifiers, Mapping)
    assert dict(identifiers) == {"isbn": "9780000000002", "uuid": "1f0e"}
    genre = fields.values["user_metadata"]
    assert isinstance(genre, Mapping)
    assert genre["#genre"] == {"value": ("novel",), "datatype": "text"}


def test_a_reading_cannot_change_after_it_is_made() -> None:
    """
    Refuse changes to the reading, its mapping and the values inside it.

    A change to the dictionary the reading was made from does not reach it
    either.

    Example:
        >>> test_a_reading_cannot_change_after_it_is_made()


    :return: None after each kind of change is refused.
    """
    source: dict[str, object] = {"tags": ["a"], "identifiers": {"isbn": "1"}}
    fields = MetadataFields(source)
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


def test_the_document_round_trips_exactly() -> None:
    """
    Write a reading as JSON and read the same reading back.

    Example:
        >>> test_the_document_round_trips_exactly()


    :return: None after the document and the round trip are checked.
    """
    fields = MetadataFields(READING)
    document = fields.to_document()
    assert "Καλημέρα" in document and "\\u" not in document
    assert list(json.loads(document)) == sorted(READING)
    assert MetadataFields.from_document(document) == fields
    assert MetadataFields.from_document(document).to_document() == document


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
def test_a_reading_refuses_what_json_cannot_hold(
    values: dict[object, object], message: str
) -> None:
    """
    Refuse names and values a JSON document could not store faithfully.

    Example:
        >>> test_a_reading_refuses_what_json_cannot_hold({"": 1}, "non-empty")  # doctest: +SKIP


    :param values: A mapping with one unacceptable name or value.
    :param message: Part of the expected error message.
    :return: None after the mapping is refused.
    """
    with pytest.raises(MetadataFieldsError, match=message):
        MetadataFields(values)


def test_a_stored_document_must_be_an_object() -> None:
    """
    Refuse a stored document that is not a JSON object.

    Example:
        >>> test_a_stored_document_must_be_an_object()


    :return: None after an array and broken JSON are refused.
    """
    with pytest.raises(MetadataFieldsError, match="JSON object"):
        MetadataFields.from_document('["title"]')
    with pytest.raises(json.JSONDecodeError):
        MetadataFields.from_document("{title")


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
        (MetadataReader, "metadata_read_formats"),
        (MetadataWriter, "metadata_write_formats"),
        (CoverExtractor, "cover_source_formats"),
    ],
)
def test_metadata_providers_declare_their_formats(base: type, attribute: str) -> None:
    """
    Require each metadata capability to declare normalised formats.

    Example:
        >>> test_metadata_providers_declare_their_formats(MetadataReader, "metadata_read_formats")  # doctest: +SKIP


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


def test_the_registry_finds_metadata_providers_by_capability() -> None:
    """
    File a reader that also extracts covers under both capabilities.

    Example:
        >>> test_the_registry_finds_metadata_providers_by_capability()


    :return: None after each metadata query is run.
    """
    registry = ProviderRegistry()
    registry.register(EpubMetadata)
    registry.register(EpubWriter)

    assert capabilities_of(EpubMetadata) == (MetadataReader, CoverExtractor)
    assert registry.metadata_readers(".EPUB") == (EpubMetadata,)
    assert registry.cover_extractors("epub") == (EpubMetadata,)
    assert registry.metadata_writers("epub") == (EpubWriter,)
    assert registry.metadata_readers("pdf") == ()
    assert registry.oeb_importers("epub") == ()


def test_a_reader_returns_a_reading_and_an_extractor_a_cover(tmp_path: Path) -> None:
    """
    Call both capabilities of one provider.

    Example:
        >>> test_a_reader_returns_a_reading_and_an_extractor_a_cover(Path.cwd())  # doctest: +SKIP


    :param tmp_path: Pytest scratch folder standing in for the file.
    :return: None after both results are checked.
    """
    provider = EpubMetadata()
    book = tmp_path / "book.epub"
    reading = provider.read_metadata(book, "epub", None)
    cover = provider.extract_cover(book, "epub", None)
    assert reading.values["title"] == READING["title"]
    assert cover == CoverImage(b"\x89", "png")
