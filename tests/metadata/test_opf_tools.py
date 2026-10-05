"""
Verify OPF metadata parsing, rendering and identifier helpers against representative packages.

The module keeps its fixtures and doubles local so the assertions remain
deterministic.

Example:
    Exercise test opf tools through its owning regression module::

        python -m pytest -q tests/metadata/test_opf_tools.py
"""
from __future__ import annotations

from collections.abc import Mapping

import LiuXin_alpha.databases.database_driver_plugins.SQL.databasedriver.utils
from LiuXin_alpha.metadata.book.base import calibreMetadata
from LiuXin_alpha.metadata.containers.calibre_like_book_metadata import (
    CalibreLikeLiuXinBookMetaData,
)
from LiuXin_alpha.metadata.containers import LiuXinWEMIMetadata
from LiuXin_alpha.metadata.opf_tools import (
    calibre_metadata_from_opf,
    liuxin_metadata_from_opf,
    metadata_from_opf,
    metadata_to_opf_bytes,
    metadata_to_opf_file,
    update_opf_bytes,
    update_opf_file,
)
from LiuXin_alpha.utils.libraries.liuxin_etree import etree


OPF2_MINIMAL = b"""<?xml version='1.0' encoding='utf-8'?>
<package xmlns="http://www.idpf.org/2007/opf"
         xmlns:dc="http://purl.org/dc/elements/1.1/"
         xmlns:opf="http://www.idpf.org/2007/opf"
         unique-identifier="BookId"
         version="2.0">
  <metadata>
    <dc:title>Seed Title</dc:title>
    <dc:creator opf:role="aut">Seed Author</dc:creator>
    <dc:identifier id="BookId">seed-id</dc:identifier>
  </metadata>
  <manifest>
    <item id="chap1" href="text/chap1.xhtml" media-type="application/xhtml+xml"/>
  </manifest>
  <spine>
    <itemref idref="chap1"/>
  </spine>
</package>
"""


def _values(raw):
    """
    Perform the values test-helper operation with deterministic inputs.

    Example:
        Exercise values through its owning regression module::

            python -m pytest -q tests/metadata/test_opf_tools.py


    :param raw: Value supplied for raw in the focused test operation.
    :return: The deterministic value, row, identity or collection described above.
    """
    if raw is None:
        return []
    if isinstance(raw, Mapping):
        return list(raw.keys())
    if isinstance(raw, str):
        return [raw]
    try:
        return list(raw)
    except TypeError:
        return [raw]


def _first_mapping_value(raw, default=None):
    """
    Perform the first mapping value test-helper operation with deterministic inputs.

    Example:
        Exercise first mapping value through its owning regression module::

            python -m pytest -q tests/metadata/test_opf_tools.py


    :param raw: Value supplied for raw in the focused test operation.
    :param default: Value supplied for default in the focused test operation.
    :return: The deterministic value, row, identity or collection described above.
    """
    if isinstance(raw, Mapping):
        try:
            return next(iter(raw.values()))
        except StopIteration:
            return default
    return raw if raw is not None else default


def _contains_forbidden_xml_char(text: str) -> bool:
    """
    Perform the contains forbidden xml char test-helper operation with deterministic inputs.

    Example:
        Exercise contains forbidden xml char through its owning regression module::

            python -m pytest -q tests/metadata/test_opf_tools.py


    :param text: Value supplied for text in the focused test operation.
    :return: The deterministic value, row, identity or collection described above.
    """
    for ch in text:
        cp = ord(ch)
        if cp == 0x7F:
            return True
        if cp in (0x9, 0xA, 0xD):
            continue
        if 0x20 <= cp <= 0xD7FF:
            continue
        if 0xE000 <= cp <= 0xFFFD:
            continue
        if 0x10000 <= cp <= 0x10FFFF:
            continue
        return True
    return False


def _sample_liuxin_metadata() -> CalibreLikeLiuXinBookMetaData:
    """
    Perform the sample liuxin metadata test-helper operation with deterministic inputs.

    Example:
        Exercise sample liuxin metadata through its owning regression module::

            python -m pytest -q tests/metadata/test_opf_tools.py


    :return: The deterministic value, row, identity or collection described above.
    """
    metadata = CalibreLikeLiuXinBookMetaData("OPF Tools Title", ["Ada Lovelace"])
    metadata.title_sort = "Tools, OPF"
    metadata.tags = ["tag-one", "tag-two"]
    metadata.comments = "Round trip comments"
    metadata.publisher = "OPF Press"
    metadata.languages = ["en"]
    metadata.series = "OPF Series"
    metadata.series_index = ("OPF Series", 2.5)
    metadata.set_identifier("doi", "10.1234/example")
    return metadata


def test_liuxin_metadata_serializes_to_opf_bytes_and_reads_back() -> None:
    """
    Verify liuxin metadata serializes to opf bytes and reads back.

    Example:
        Exercise test liuxin metadata serializes to opf bytes and reads back through its owning regression module::

            python -m pytest -q tests/metadata/test_opf_tools.py


    :return: None; the function records state or raises through its assertions.
    """
    raw = metadata_to_opf_bytes(_sample_liuxin_metadata())

    assert b"<package" in raw
    assert b"OPF Tools Title" in raw

    parsed = liuxin_metadata_from_opf(raw)

    assert parsed.title == "OPF Tools Title"
    assert _values(parsed.authors) == ["Ada Lovelace"]
    assert set(_values(parsed.tags)) >= {"tag-one", "tag-two"}
    assert _values(parsed.comments) == ["Round trip comments"]
    assert _values(parsed.publisher) == ["OPF Press"]
    assert _values(parsed.series) == ["OPF Series"]
    assert float(_first_mapping_value(parsed.series_index, 0.0)) == 2.5
    assert parsed.title_sort == "Tools, OPF"
    assert "10.1234/example" in _values(parsed.get_identifiers()["doi"])


def test_metadata_to_opf_bytes_sanitizes_hostile_xml_without_mutating_input() -> None:
    """
    Verify metadata to opf bytes sanitizes hostile xml without mutating input.

    Example:
        Exercise test metadata to opf bytes sanitizes hostile xml without mutating input through its owning regression module::

            python -m pytest -q tests/metadata/test_opf_tools.py


    :return: None; the function records state or raises through its assertions.
    """
    metadata = CalibreLikeLiuXinBookMetaData("Bad\x00Title\ud800 😀", ["Author\x01 One"])
    metadata.tags = ["Tag\x02One", "Emoji 😀"]
    metadata.comments = "Comment\x03 with <xml> & emoji 😀"
    metadata.publisher = "Pub\x04lisher"
    metadata.languages = ["en\x05", "fr"]
    metadata.set_identifier("doi", "10.1234/bad\x07id")

    raw = metadata_to_opf_bytes(metadata)

    xml_text = raw.decode("utf-8")
    assert not _contains_forbidden_xml_char(xml_text)
    etree.fromstring(raw)

    parsed = liuxin_metadata_from_opf(raw)
    assert "BadTitle" in parsed.title
    assert "😀" in parsed.title
    assert set(_values(parsed.tags)) >= {"TagOne", "Emoji 😀"}
    assert "10.1234/badid" in xml_text

    assert metadata.title == "Bad\x00Title\ud800 😀"
    assert _values(metadata.authors) == ["Author\x01 One"]
    assert _values(metadata.tags) == ["Tag\x02One", "Emoji 😀"]


def test_opf_file_helpers_round_trip_calibre_and_liuxin_metadata(tmp_path) -> None:
    """
    Verify opf file helpers round trip calibre and liuxin metadata.

    Example:
        Exercise test opf file helpers round trip calibre and liuxin metadata through its owning regression module::

            python -m pytest -q tests/metadata/test_opf_tools.py


    :param tmp_path: Pytest-managed temporary directory for filesystem assertions.
    :return: None; the function records state or raises through its assertions.
    """
    target = tmp_path / "metadata.opf"
    written = metadata_to_opf_file(_sample_liuxin_metadata(), target)

    assert written == target

    calibre_parsed = metadata_from_opf(target, kind="calibre")
    assert isinstance(calibre_parsed, calibreMetadata)
    assert calibre_parsed.title == "OPF Tools Title"
    assert set(calibre_parsed.tags) >= {"tag-one", "tag-two"}

    liuxin_parsed = CalibreLikeLiuXinBookMetaData.from_opf(target)
    assert liuxin_parsed.title == "OPF Tools Title"
    assert set(_values(liuxin_parsed.tags)) >= {"tag-one", "tag-two"}


def test_update_opf_bytes_preserves_existing_package_structure() -> None:
    """
    Verify update opf bytes preserves existing package structure.

    Example:
        Exercise test update opf bytes preserves existing package structure through its owning regression module::

            python -m pytest -q tests/metadata/test_opf_tools.py


    :return: None; the function records state or raises through its assertions.
    """
    metadata = CalibreLikeLiuXinBookMetaData("Updated OPF Title", ["Updated Author"])
    metadata.tags = "updated-tag"
    metadata.comments = "Updated comments"

    updated = update_opf_bytes(OPF2_MINIMAL, metadata)

    assert b"text/chap1.xhtml" in updated
    parsed = calibre_metadata_from_opf(updated)
    assert parsed.title == "Updated OPF Title"
    assert parsed.authors == ["Updated Author"]
    assert "updated-tag" in parsed.tags
    assert parsed.comments == "Updated comments"


def test_update_opf_file_sanitizes_hostile_xml_and_preserves_source_file(tmp_path) -> None:
    """
    Verify update opf file sanitizes hostile xml and preserves source file.

    Example:
        Exercise test update opf file sanitizes hostile xml and preserves source file through its owning regression module::

            python -m pytest -q tests/metadata/test_opf_tools.py


    :param tmp_path: Pytest-managed temporary directory for filesystem assertions.
    :return: None; the function records state or raises through its assertions.
    """
    source = tmp_path / "source.opf"
    output = tmp_path / "updated.opf"
    source.write_bytes(OPF2_MINIMAL)

    metadata = CalibreLikeLiuXinBookMetaData("File\x00Title\ud800 😀", ["File\x01 Author"])
    metadata.tags = ["file\x02tag"]
    metadata.comments = "File comment\x03"

    written = update_opf_file(source, metadata, output_path=output)

    assert written == output
    assert source.read_bytes() == OPF2_MINIMAL

    raw = output.read_bytes()
    xml_text = raw.decode("utf-8")
    assert b"text/chap1.xhtml" in raw
    assert not _contains_forbidden_xml_char(xml_text)
    etree.fromstring(raw)

    parsed = calibre_metadata_from_opf(raw)
    assert "FileTitle" in parsed.title
    assert "😀" in parsed.title
    assert parsed.authors == ["File Author"]
    assert "filetag" in parsed.tags


def test_wemi_metadata_opf_helpers_keep_explicit_item_id() -> None:
    """
    Verify wemi metadata opf helpers keep explicit item id.

    Example:
        Exercise test wemi metadata opf helpers keep explicit item id through its owning regression module::

            python -m pytest -q tests/metadata/test_opf_tools.py


    :return: None; the function records state or raises through its assertions.
    """
    metadata = LiuXinWEMIMetadata("WEMI OPF Title", ["WEMI Author"])
    metadata.tags = "wemi-tag"

    raw = metadata.to_opf_bytes()
    parsed = LiuXinWEMIMetadata.from_opf(raw, item_id=77)

    assert parsed.display_title == "WEMI OPF Title"
    assert parsed.get_database_id("item") == 77
    assert set(_values(parsed.tags)) >= {"wemi-tag"}
