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
)


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
    if isinstance(raw, Mapping):
        try:
            return next(iter(raw.values()))
        except StopIteration:
            return default
    return raw if raw is not None else default


def _sample_liuxin_metadata() -> CalibreLikeLiuXinBookMetaData:
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
    assert LiuXin_alpha.databases.database_driver_plugins.SQL.databasedriver.utils.title_sort == "Tools, OPF"
    assert "10.1234/example" in _values(parsed.get_identifiers()["doi"])


def test_opf_file_helpers_round_trip_calibre_and_liuxin_metadata(tmp_path) -> None:
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


def test_wemi_metadata_opf_helpers_keep_explicit_item_id() -> None:
    metadata = LiuXinWEMIMetadata("WEMI OPF Title", ["WEMI Author"])
    metadata.tags = "wemi-tag"

    raw = metadata.to_opf_bytes()
    parsed = LiuXinWEMIMetadata.from_opf(raw, item_id=77)

    assert parsed.display_title == "WEMI OPF Title"
    assert parsed.get_database_id("item") == 77
    assert set(_values(parsed.tags)) >= {"wemi-tag"}
