from __future__ import annotations

import io

from LiuXin_alpha.file_formats.mobi.reader.headers import MetadataHeader
from LiuXin_alpha.utils.logging import default_log


def test_metadata_header_identity_accepts_bookmobi_bytes() -> None:
    payload = bytearray(80)
    payload[60:68] = b"BOOKMOBI"
    payload[76:78] = b"\x00\x00"

    header = MetadataHeader(io.BytesIO(bytes(payload)), default_log)
    assert header.ident == b"BOOKMOBI"


def test_metadata_header_section_data_last_section_works_without_stream_name() -> None:
    header = MetadataHeader.__new__(MetadataHeader)
    header.stream = io.BytesIO(b"abcdef")
    header.num_sections = 1
    header.section_offset = lambda _number: 0

    assert header.section_data(0) == b"abcdef"
