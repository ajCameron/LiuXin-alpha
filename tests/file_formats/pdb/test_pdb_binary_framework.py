"""
Provide test pdb binary framework utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test pdb binary framework through a consuming regression::

        python -m pytest -q tests/file_formats/pdb/test_pdb_binary_framework.py
"""
from __future__ import annotations

from pathlib import Path

import pytest

from tests.support.file_format_pdb import (
    PdbLog,
    build_ereader_header_record,
    build_minimal_haodoo_pdb,
    build_minimal_ereader_pdb,
    build_minimal_palmdoc_pdb,
    build_minimal_ztxt_pdb,
    build_pdb,
    build_plucker_metadata_section,
    build_ztxt_header_record,
    pdb_input_options,
    pdb_record_offsets,
    pdb_stream,
    rewrite_pdb_record_offset,
    rewrite_pdb_record_offsets,
    truncate_pdb_payload,
    ztxt_compressed_records,
)


def _values(raw):
    """
    Perform the values operation under explicit file-format and conversion rules.

    Example:
        Exercise  values through a consuming regression::

            python -m pytest -q tests/file_formats/pdb/test_pdb_binary_framework.py


    :param raw: Value supplied for raw under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
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


def test_pdb_fixture_parses_with_header_reader() -> None:
    """
    Perform the test pdb fixture parses with header reader operation under explicit file-format and conversion rules.

    Example:
        Exercise test pdb fixture parses with header reader through a consuming regression::

            python -m pytest -q tests/file_formats/pdb/test_pdb_binary_framework.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats.pdb.header import PdbHeaderReader

    payload = build_pdb([b"record-zero", b"record-one"], title="PDB Fixture", ident="TEXtREAd")
    header = PdbHeaderReader(pdb_stream(payload))

    assert header.ident == "TEXtREAd"
    assert header.title == "PDB Fixture"
    assert header.num_sections == 2
    assert header.section_data(0) == b"record-zero"
    assert header.section_data(1) == b"record-one"


def test_minimal_palmdoc_fixture_parses_with_reader() -> None:
    """
    Perform the test minimal palmdoc fixture parses with reader operation under explicit file-format and conversion rules.

    Example:
        Exercise test minimal palmdoc fixture parses with reader through a consuming regression::

            python -m pytest -q tests/file_formats/pdb/test_pdb_binary_framework.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats.pdb.header import PdbHeaderReader
    from LiuXin_alpha.file_formats.pdb.palmdoc.reader import Reader

    body = "naive cafe - Καλημέρα - 漢字".encode("utf-8")
    stream = pdb_stream(build_minimal_palmdoc_pdb(title="PalmDOC Fixture", body_text=body))
    header = PdbHeaderReader(stream)
    reader = Reader(header, stream, PdbLog(), pdb_input_options())

    assert reader.header_record.compression == 1
    assert reader.header_record.num_records == 1
    assert reader.decompress_text(1) == body


def test_minimal_ztxt_fixture_parses_with_reader() -> None:
    """
    Perform the test minimal ztxt fixture parses with reader operation under explicit file-format and conversion rules.

    Example:
        Exercise test minimal ztxt fixture parses with reader through a consuming regression::

            python -m pytest -q tests/file_formats/pdb/test_pdb_binary_framework.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats.pdb.header import PdbHeaderReader
    from LiuXin_alpha.file_formats.pdb.ztxt.reader import Reader

    body = "zTXT cafe - Καλημέρα - 漢字".encode("utf-8")
    stream = pdb_stream(build_minimal_ztxt_pdb(title="zTXT Fixture", body_text=body))
    header = PdbHeaderReader(stream)
    reader = Reader(header, stream, PdbLog(), pdb_input_options())

    assert reader.header_record.num_records == 1
    assert reader.header_record.flags & 0x01
    assert reader.decompress_text(1) == body


def test_ereader_fixture_can_drive_metadata_source_read() -> None:
    """
    Perform the test ereader fixture can drive metadata source read operation under explicit file-format and conversion rules.

    Example:
        Exercise test ereader fixture can drive metadata source read through a consuming regression::

            python -m pytest -q tests/file_formats/pdb/test_pdb_binary_framework.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.metadata.file_sources.pdb import get_metadata

    payload = build_minimal_ereader_pdb(
        title="Wrapper Title",
        metadata_title="eReader Metadata Café",
        author="Alice Ω",
        publisher="Publisher Δ",
        isbn="9781234567890",
    )

    metadata = get_metadata(pdb_stream(payload), extract_cover=False)

    assert metadata.title == "eReader Metadata Café"
    assert _values(metadata.authors) == ["Alice Ω"]
    assert metadata.publisher == "Publisher Δ"
    assert metadata.isbn == "9781234567890"


@pytest.mark.parametrize(
    ("unicode_variant", "book_title", "chapter_title", "body_text"),
    (
        (False, "標題", "第一章", "第一章\n世界 咖啡"),
        (True, "Unicode 標題", "Unicode 第一章", "Unicode 第一章\n世界 Καλημέρα"),
    ),
)
def test_minimal_haodoo_fixture_drives_reader_output(
    tmp_path,
    unicode_variant: bool,
    book_title: str,
    chapter_title: str,
    body_text: str,
) -> None:
    """
    Perform the test minimal haodoo fixture drives reader output operation under explicit file-format and conversion rules.

    Example:
        Exercise test minimal haodoo fixture drives reader output through a consuming regression::

            python -m pytest -q tests/file_formats/pdb/test_pdb_binary_framework.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :param unicode_variant: Value supplied for unicode variant under the utility
        contract.
    :param book_title: Value supplied for book title under the utility contract.
    :param chapter_title: Value supplied for chapter title under the utility contract.
    :param body_text: Value supplied for body text under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats.pdb.haodoo.reader import Reader
    from LiuXin_alpha.file_formats.pdb.header import PdbHeaderReader

    payload = build_minimal_haodoo_pdb(
        book_title=book_title,
        chapter_title=chapter_title,
        body_text=body_text,
        unicode_variant=unicode_variant,
    )
    stream = pdb_stream(payload)
    header = PdbHeaderReader(stream)
    reader = Reader(header, stream, PdbLog(), pdb_input_options())
    opf_path = reader.extract_content(str(tmp_path))

    html = (tmp_path / "index.html").read_text("utf-8", "replace")
    assert opf_path == str(tmp_path / "metadata.opf")
    assert book_title in html
    assert chapter_title in html
    assert "世界" in html


def test_minimal_haodoo_fixture_converts_through_pdb_input(tmp_path: Path, monkeypatch) -> None:
    """
    Perform the test minimal haodoo fixture converts through pdb input operation under explicit file-format and conversion rules.

    Example:
        Exercise test minimal haodoo fixture converts through pdb input through a consuming regression::

            python -m pytest -q tests/file_formats/pdb/test_pdb_binary_framework.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats.conversion.plugins.pdb_input import PDBInput

    workdir = tmp_path / "haodoo_plugin"
    workdir.mkdir()
    monkeypatch.chdir(workdir)
    payload = build_minimal_haodoo_pdb(
        book_title="Plugin 標題",
        chapter_title="Plugin 第一章",
        body_text="Plugin 第一章\n世界 咖啡",
    )

    opf_path = Path(
        PDBInput(None).convert(
            pdb_stream(payload),
            pdb_input_options(),
            "pdb",
            PdbLog(),
            {},
        )
    )

    assert opf_path == workdir / "metadata.opf"
    html = (workdir / "index.html").read_text("utf-8", "replace")
    assert "Plugin 標題" in html
    assert "Plugin 第一章" in html
    assert "世界 咖啡" in html


def test_plucker_metadata_section_fixture_can_drive_metadata_reader() -> None:
    """
    Perform the test plucker metadata section fixture can drive metadata reader operation under explicit file-format and conversion rules.

    Example:
        Exercise test plucker metadata section fixture can drive metadata reader through a consuming regression::

            python -m pytest -q tests/file_formats/pdb/test_pdb_binary_framework.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.metadata.file_sources.pdb import get_metadata

    section = build_plucker_metadata_section(
        title="Plucker 世界",
        author="Alice,Bob",
        pubdate=1700000000,
    )
    payload = build_pdb([b"\0" * 8, section], title="Fallback", ident="DataPlkr")

    metadata = get_metadata(pdb_stream(payload), extract_cover=False)

    assert metadata.title == "Plucker 世界"
    assert _values(metadata.authors) == ["Alice", "Bob"]
    assert (metadata.pubdate.year, metadata.pubdate.month, metadata.pubdate.day) == (2023, 11, 14)


def test_pdb_offset_and_truncation_helpers_prepare_hostile_payloads() -> None:
    """
    Perform the test pdb offset and truncation helpers prepare hostile payloads operation under explicit file-format and conversion rules.

    Example:
        Exercise test pdb offset and truncation helpers prepare hostile payloads through a consuming regression::

            python -m pytest -q tests/file_formats/pdb/test_pdb_binary_framework.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    payload = build_minimal_palmdoc_pdb(title="Offset Fixture", body_text=b"body")
    offsets = pdb_record_offsets(payload)

    assert len(offsets) == 2
    assert offsets == sorted(offsets)

    duplicate_offset = rewrite_pdb_record_offset(payload, 1, offsets[0])
    assert pdb_record_offsets(duplicate_offset) == [offsets[0], offsets[0]]

    reversed_offsets = rewrite_pdb_record_offsets(payload, list(reversed(offsets)))
    assert pdb_record_offsets(reversed_offsets) == list(reversed(offsets))

    with pytest.raises(ValueError):
        rewrite_pdb_record_offsets(payload, offsets[:1])

    truncated = truncate_pdb_payload(payload, offsets[1] + 2)
    assert truncated == payload[: offsets[1] + 2]


def test_pdb_fixture_rejects_invalid_builder_arguments() -> None:
    """
    Perform the test pdb fixture rejects invalid builder arguments operation under explicit file-format and conversion rules.

    Example:
        Exercise test pdb fixture rejects invalid builder arguments through a consuming regression::

            python -m pytest -q tests/file_formats/pdb/test_pdb_binary_framework.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    with pytest.raises(ValueError, match="identity"):
        build_pdb([b"record"], ident=b"SHORT")

    payload = build_minimal_palmdoc_pdb()
    with pytest.raises(ValueError, match="negative"):
        truncate_pdb_payload(payload, -1)


def test_subformat_record_helpers_support_malformed_future_cases() -> None:
    """
    Perform the test subformat record helpers support malformed future cases operation under explicit file-format and conversion rules.

    Example:
        Exercise test subformat record helpers support malformed future cases through a consuming regression::

            python -m pytest -q tests/file_formats/pdb/test_pdb_binary_framework.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    header = build_ereader_header_record(metadata_offset=99, last_data_offset=100)
    assert len(header) == 132

    compressed, total_length, crc32 = ztxt_compressed_records([b"one", b"two"])
    ztxt_header = build_ztxt_header_record(
        text_length=total_length,
        record_count=len(compressed),
        crc32=crc32,
        flags=0,
    )

    assert len(compressed) == 2
    assert len(ztxt_header) == 32
    assert ztxt_header[18] == 0
