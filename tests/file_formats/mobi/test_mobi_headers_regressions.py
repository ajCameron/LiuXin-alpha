"""
Provide test mobi headers regressions utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test mobi headers regressions through a consuming regression::

        python -m pytest -q tests/file_formats/mobi/test_mobi_headers_regressions.py
"""
from __future__ import annotations

import io

from LiuXin_alpha.file_formats.mobi.reader.headers import MetadataHeader
from LiuXin_alpha.utils.logging import default_log


def test_metadata_header_identity_accepts_bookmobi_bytes() -> None:
    """
    Perform the test metadata header identity accepts bookmobi bytes operation under explicit file-format and conversion rules.

    Example:
        Exercise test metadata header identity accepts bookmobi bytes through a consuming regression::

            python -m pytest -q tests/file_formats/mobi/test_mobi_headers_regressions.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    payload = bytearray(80)
    payload[60:68] = b"BOOKMOBI"
    payload[76:78] = b"\x00\x00"

    header = MetadataHeader(io.BytesIO(bytes(payload)), default_log)
    assert header.ident == b"BOOKMOBI"


def test_metadata_header_section_data_last_section_works_without_stream_name() -> None:
    """
    Perform the test metadata header section data last section works without stream name operation under explicit file-format and conversion rules.

    Example:
        Exercise test metadata header section data last section works without stream name through a consuming regression::

            python -m pytest -q tests/file_formats/mobi/test_mobi_headers_regressions.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    header = MetadataHeader.__new__(MetadataHeader)
    header.stream = io.BytesIO(b"abcdef")
    header.num_sections = 1
    header.section_offset = lambda _number: 0

    assert header.section_data(0) == b"abcdef"


def test_kf8_chunker_preserves_placeholder_when_target_aid_was_removed() -> None:
    """
    Perform the test kf8 chunker preserves placeholder when target aid was removed operation under explicit file-format and conversion rules.

    Example:
        Exercise test kf8 chunker preserves placeholder when target aid was removed through a consuming regression::

            python -m pytest -q tests/file_formats/mobi/test_mobi_headers_regressions.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats.mobi.writer8.skeleton import Chunker

    class _Log:
        """
        Provide the log contract for validated ebook processing.

        Example:
            Exercise test kf8 chunker preserves placeholder when target aid was removed. Log through a consuming regression::

                python -m pytest -q tests/file_formats/mobi/test_mobi_headers_regressions.py
        """
        def __init__(self) -> None:
            """
            Initialize and validate the log state.

            Example:
                Exercise test kf8 chunker preserves placeholder when target aid was removed. Log.  init   through a consuming regression::

                    python -m pytest -q tests/file_formats/mobi/test_mobi_headers_regressions.py


            :return: None; validated state is stored on the receiving object.
            """
            self.messages: list[str] = []

        def warning(self, message: str) -> None:
            """
            Perform the warning operation under explicit file-format and conversion rules.

            Example:
                Exercise test kf8 chunker preserves placeholder when target aid was removed. Log.warning through a consuming regression::

                    python -m pytest -q tests/file_formats/mobi/test_mobi_headers_regressions.py


            :param message: Value supplied for message under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.messages.append(message)

    placeholder = b"kindle:pos:fid:0000:off:0000000001"
    text = b'<a href="' + placeholder + b'">missing target</a>'
    chunker = Chunker.__new__(Chunker)
    chunker.chunk_table = []
    chunker.placeholder_map = {placeholder: "MISSING"}
    chunker.log = _Log()

    assert chunker.set_internal_links(text, b"<html><body/></html>") == text
    assert "missing aid 'MISSING'" in chunker.log.messages[0]
