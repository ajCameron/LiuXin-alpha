from __future__ import annotations

import pytest

from tests.support.file_format_mobi import (
    MobiLog,
    build_minimal_mobi,
    build_mobi_exth,
    build_palmdb,
    mobi_exth_record,
    mobi_stream,
    palmdb_record_offsets,
    rewrite_palmdb_record_offset,
    rewrite_palmdb_record_offsets,
    truncate_mobi_payload,
)


def _values(raw):
    if raw is None:
        return []
    if isinstance(raw, dict):
        return list(raw.keys())
    if isinstance(raw, str):
        return [raw]
    try:
        return list(raw)
    except TypeError:
        return [raw]


def test_minimal_mobi_fixture_parses_with_metadata_header() -> None:
    from LiuXin_alpha.file_formats.mobi.reader.headers import MetadataHeader

    payload = build_minimal_mobi(
        title="主題 MOBI — Καλημέρα — 😀",
        authors=["Alice Δ", "李白"],
        publisher="Publisher Ω",
        comments="Combining cafe\u0301 and RTL مرحبا",
        tags=["fiction", "δοκιμή", "漢字"],
        body_html="<html><body><p>Καλημέρα κόσμε — مرحبا — 漢字 😀</p></body></html>",
    )

    header = MetadataHeader(mobi_stream(payload), MobiLog())

    assert header.ident == b"BOOKMOBI"
    assert header.num_sections == 2
    assert header.title == "主題 MOBI — Καλημέρα — 😀"
    assert header.exth.mi.title == "主題 MOBI — Καλημέρα — 😀"
    assert set(_values(header.exth.mi.authors)) == {"Alice Δ", "李白"}
    assert header.exth.mi.publisher == "Publisher Ω"
    assert "مرحبا" in header.exth.mi.comments
    assert header.section_data(1).startswith(b"<html>")


def test_minimal_mobi_fixture_parses_with_mobi_reader_extract_text() -> None:
    from LiuXin_alpha.file_formats.mobi.reader.mobi6 import MobiReader

    payload = build_minimal_mobi(
        title="Reader Fixture",
        authors=["Reader Author"],
        body_html="<html><body><p>naïve Καλημέρα 漢字 😀</p></body></html>",
    )
    reader = MobiReader(mobi_stream(payload), MobiLog())

    processed = reader.extract_text()

    assert processed == [0, 1]
    assert reader.kf8_type is None
    assert reader.book_header.records == 1
    assert "Reader Fixture" == reader.book_header.title
    assert "Καλημέρα".encode("utf-8") in reader.mobi_html


def test_minimal_mobi_fixture_can_drive_metadata_source_read() -> None:
    from LiuXin_alpha.metadata.file_sources.mobi import read_metadata_from_stream

    payload = build_minimal_mobi(
        title="Metadata Fixture — café — 世界",
        authors=["Author Ω", "著者"],
        tags=["fixture", "日本語"],
    )

    metadata = read_metadata_from_stream(mobi_stream(payload), extract_cover=False)

    assert metadata.title == "Metadata Fixture — café — 世界"
    assert set(_values(metadata.authors)) == {"Author Ω", "著者"}
    assert {"fixture", "日本語"} <= set(_values(metadata.tags))


def test_exth_fixture_helpers_support_unicode_and_malformed_sizes() -> None:
    from LiuXin_alpha.file_formats.mobi.reader.headers import EXTHHeader

    exth = build_mobi_exth(
        [
            (100, "Renée Faßbinder"),
            (100, "李白"),
            (503, "EXTH Title — 😀"),
            (524, "fr"),
        ]
    )
    parsed = EXTHHeader(exth, "utf-8", "fallback")

    assert parsed.mi.title == "EXTH Title — 😀"
    assert set(_values(parsed.mi.authors)) == {"Renée Faßbinder", "李白"}
    assert str(parsed.mi.language).lower() in {"fr", "french"}

    malformed = build_mobi_exth([mobi_exth_record(100, b"payload", declared_size=7)], item_count=1)
    assert malformed.startswith(b"EXTH")
    assert b"payload" in malformed


def test_palmdb_offset_and_truncation_helpers_prepare_hostile_payloads() -> None:
    payload = build_minimal_mobi(title="Offset Fixture", authors=["Offset Author"])
    offsets = palmdb_record_offsets(payload)

    assert len(offsets) == 2
    assert offsets == sorted(offsets)

    duplicate_offset = rewrite_palmdb_record_offset(payload, 1, offsets[0])
    assert palmdb_record_offsets(duplicate_offset) == [offsets[0], offsets[0]]

    reversed_offsets = rewrite_palmdb_record_offsets(payload, list(reversed(offsets)))
    assert palmdb_record_offsets(reversed_offsets) == list(reversed(offsets))

    with pytest.raises(ValueError):
        rewrite_palmdb_record_offsets(payload, offsets[:1])

    truncated = truncate_mobi_payload(payload, offsets[1] + 8)
    assert truncated == payload[: offsets[1] + 8]


def test_palmdb_fixture_rejects_invalid_builder_arguments() -> None:
    with pytest.raises(ValueError, match="ident"):
        build_palmdb([b"record"], ident=b"SHORT")

    payload = build_minimal_mobi()
    with pytest.raises(ValueError, match="negative"):
        truncate_mobi_payload(payload, -1)
