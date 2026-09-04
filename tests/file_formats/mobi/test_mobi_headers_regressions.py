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


def test_kf8_chunker_preserves_placeholder_when_target_aid_was_removed() -> None:
    from LiuXin_alpha.file_formats.mobi.writer8.skeleton import Chunker

    class _Log:
        def __init__(self) -> None:
            self.messages: list[str] = []

        def warning(self, message: str) -> None:
            self.messages.append(message)

    placeholder = b"kindle:pos:fid:0000:off:0000000001"
    text = b'<a href="' + placeholder + b'">missing target</a>'
    chunker = Chunker.__new__(Chunker)
    chunker.chunk_table = []
    chunker.placeholder_map = {placeholder: "MISSING"}
    chunker.log = _Log()

    assert chunker.set_internal_links(text, b"<html><body/></html>") == text
    assert "missing aid 'MISSING'" in chunker.log.messages[0]
