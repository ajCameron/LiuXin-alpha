from __future__ import annotations

import io
from dataclasses import dataclass

import pytest


@dataclass(frozen=True)
class MalformedPayload:
    name: str
    data: bytes


MALFORMED_PAYLOADS = {
    "empty": MalformedPayload("empty", b""),
    "tiny_binary": MalformedPayload("tiny_binary", b"\x00\xffnot-a-book"),
    "png_header": MalformedPayload("png_header", b"\x89PNG\r\n\x1a\n" + b"\x00" * 16),
    "html_document": MalformedPayload(
        "html_document",
        b"<html><head><title>Wrong format</title></head><body></body></html>",
    ),
    "empty_zip": MalformedPayload("empty_zip", b"PK\x05\x06" + b"\x00" * 18),
    "opf_package": MalformedPayload(
        "opf_package",
        b"""<?xml version="1.0" encoding="utf-8"?>
<package xmlns="http://www.idpf.org/2007/opf"
         xmlns:dc="http://purl.org/dc/elements/1.1/"
         version="2.0">
  <metadata><dc:title>Wrong extractor</dc:title></metadata>
</package>
""",
    ),
    "fb2_document": MalformedPayload(
        "fb2_document",
        b"""<?xml version="1.0" encoding="utf-8"?>
<FictionBook xmlns="http://www.gribuser.ru/xml/fictionbook/2.0">
  <description><title-info><book-title>Wrong extractor</book-title></title-info></description>
</FictionBook>
""",
    ),
}


STRICT_CONTAINER_CASES = [
    ("epub", "EPUBMetadataReader", "tiny_binary"),
    ("epub", "EPUBMetadataReader", "png_header"),
    ("epub", "EPUBMetadataReader", "html_document"),
    ("epub", "EPUBMetadataReader", "empty_zip"),
    ("docx", "DocXMetadataReader", "empty"),
    ("docx", "DocXMetadataReader", "tiny_binary"),
    ("docx", "DocXMetadataReader", "empty_zip"),
    ("zip", "ZipMetadataReader", "empty"),
    ("zip", "ZipMetadataReader", "png_header"),
    ("zip", "ZipMetadataReader", "html_document"),
    ("zip", "ZipMetadataReader", "empty_zip"),
]


STRICT_XML_CASES = [
    ("opf", "OPFMetadataReader", "empty"),
    ("opf", "OPFMetadataReader", "tiny_binary"),
    ("opf", "OPFMetadataReader", "png_header"),
    ("opf", "OPFMetadataReader", "html_document"),
    ("opf", "OPFMetadataReader", "empty_zip"),
    ("opf", "OPFMetadataReader", "fb2_document"),
    ("fb2", "FB2MetadataReader", "empty"),
    ("fb2", "FB2MetadataReader", "tiny_binary"),
    ("fb2", "FB2MetadataReader", "png_header"),
    ("fb2", "FB2MetadataReader", "html_document"),
    ("fb2", "FB2MetadataReader", "empty_zip"),
    ("fb2", "FB2MetadataReader", "opf_package"),
]


def _stream_for(extension: str, payload: MalformedPayload) -> io.BytesIO:
    stream = io.BytesIO(payload.data)
    stream.name = f"{payload.name}.{extension}"
    return stream


@pytest.mark.parametrize(("extension", "reader_name", "payload_name"), STRICT_CONTAINER_CASES)
def test_strict_container_extractors_reject_wrong_format_payloads(
    extension: str,
    reader_name: str,
    payload_name: str,
) -> None:
    """
    Strict container readers should reject non-credible payloads.

    The dispatcher is allowed to wrap the underlying format error, but it must
    not return conservative fallback metadata for arbitrary wrong-format bytes.
    """
    from LiuXin_alpha.metadata.file_sources import get_metadata

    payload = MALFORMED_PAYLOADS[payload_name]
    with pytest.raises(RuntimeError, match=f"extension '{extension}'.*{reader_name}") as exc_info:
        get_metadata(_stream_for(extension, payload), force_type=extension)

    assert exc_info.value.__cause__ is not None


@pytest.mark.parametrize(("extension", "reader_name", "payload_name"), STRICT_XML_CASES)
def test_strict_xml_extractors_reject_wrong_format_payloads(
    extension: str,
    reader_name: str,
    payload_name: str,
) -> None:
    """
    Structured XML readers should validate the root format, not just XML-ness.
    """
    from LiuXin_alpha.metadata.file_sources import get_metadata

    payload = MALFORMED_PAYLOADS[payload_name]
    with pytest.raises(RuntimeError, match=f"extension '{extension}'.*{reader_name}") as exc_info:
        get_metadata(_stream_for(extension, payload), force_type=extension)

    assert exc_info.value.__cause__ is not None


def test_registry_lists_strict_container_readers_for_fuzzing() -> None:
    from LiuXin_alpha.metadata.file_sources import registry

    reader_names = {
        entry.name
        for extension in ("epub", "docx", "zip")
        for entry in registry.iter_metadata_reader_entries_for_extension(extension)
    }
    assert {"EPUBMetadataReader", "DocXMetadataReader", "ZipMetadataReader"} <= reader_names


def test_registry_lists_strict_xml_readers_for_fuzzing() -> None:
    from LiuXin_alpha.metadata.file_sources import registry

    reader_names = {
        entry.name
        for extension in ("opf", "fb2")
        for entry in registry.iter_metadata_reader_entries_for_extension(extension)
    }
    assert {"OPFMetadataReader", "FB2MetadataReader"} <= reader_names
