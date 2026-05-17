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
    "pdf_header_only": MalformedPayload("pdf_header_only", b"%PDF-1.4\n"),
    "mobi_marker_only": MalformedPayload("mobi_marker_only", b"BOOKMOBI payload"),
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
    ("htmlz", "HTMLZMetadataReader", "empty"),
    ("htmlz", "HTMLZMetadataReader", "tiny_binary"),
    ("htmlz", "HTMLZMetadataReader", "html_document"),
    ("htmlz", "HTMLZMetadataReader", "empty_zip"),
    ("txtz", "TXTZMetadataReader", "empty"),
    ("txtz", "TXTZMetadataReader", "tiny_binary"),
    ("txtz", "TXTZMetadataReader", "html_document"),
    ("txtz", "TXTZMetadataReader", "empty_zip"),
    ("odt", "ODTMetadataReader", "empty"),
    ("odt", "ODTMetadataReader", "tiny_binary"),
    ("odt", "ODTMetadataReader", "html_document"),
    ("odt", "ODTMetadataReader", "empty_zip"),
    ("rar", "RARMetadataReader", "empty"),
    ("rar", "RARMetadataReader", "tiny_binary"),
    ("rar", "RARMetadataReader", "html_document"),
    ("rar", "RARMetadataReader", "empty_zip"),
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


STRICT_BINARY_CASES = [
    ("pdf", "PDFMetadataReader", "empty"),
    ("pdf", "PDFMetadataReader", "tiny_binary"),
    ("pdf", "PDFMetadataReader", "png_header"),
    ("pdf", "PDFMetadataReader", "html_document"),
    ("pdf", "PDFMetadataReader", "empty_zip"),
    ("pdf", "PDFMetadataReader", "pdf_header_only"),
    ("mobi", "MOBIMetadataReader", "empty"),
    ("mobi", "MOBIMetadataReader", "tiny_binary"),
    ("mobi", "MOBIMetadataReader", "png_header"),
    ("mobi", "MOBIMetadataReader", "html_document"),
    ("mobi", "MOBIMetadataReader", "empty_zip"),
    ("mobi", "MOBIMetadataReader", "pdf_header_only"),
    ("mobi", "MOBIMetadataReader", "mobi_marker_only"),
    ("pdb", "PDBMetadataReader", "empty"),
    ("pdb", "PDBMetadataReader", "tiny_binary"),
    ("pdb", "PDBMetadataReader", "png_header"),
    ("pdb", "PDBMetadataReader", "html_document"),
    ("pdb", "PDBMetadataReader", "empty_zip"),
    ("pdb", "PDBMetadataReader", "pdf_header_only"),
    ("pdb", "PDBMetadataReader", "mobi_marker_only"),
    ("lrf", "LRFMetadataReader", "empty"),
    ("lrf", "LRFMetadataReader", "tiny_binary"),
    ("lrf", "LRFMetadataReader", "png_header"),
    ("lrf", "LRFMetadataReader", "html_document"),
    ("lrf", "LRFMetadataReader", "empty_zip"),
    ("lrf", "LRFMetadataReader", "pdf_header_only"),
]


STRICT_LEGACY_CASES = [
    ("rtf", "RTFMetadataReader", "empty"),
    ("rtf", "RTFMetadataReader", "tiny_binary"),
    ("rtf", "RTFMetadataReader", "png_header"),
    ("rtf", "RTFMetadataReader", "html_document"),
    ("rtf", "RTFMetadataReader", "empty_zip"),
    ("snb", "SNBMetadataReader", "empty"),
    ("snb", "SNBMetadataReader", "tiny_binary"),
    ("snb", "SNBMetadataReader", "png_header"),
    ("snb", "SNBMetadataReader", "html_document"),
    ("snb", "SNBMetadataReader", "empty_zip"),
    ("lrx", "LRXMetadataReader", "empty"),
    ("lrx", "LRXMetadataReader", "tiny_binary"),
    ("lrx", "LRXMetadataReader", "png_header"),
    ("lrx", "LRXMetadataReader", "html_document"),
    ("lrx", "LRXMetadataReader", "empty_zip"),
    ("rb", "RBMetadataReader", "empty"),
    ("rb", "RBMetadataReader", "tiny_binary"),
    ("rb", "RBMetadataReader", "png_header"),
    ("rb", "RBMetadataReader", "html_document"),
    ("rb", "RBMetadataReader", "empty_zip"),
    ("imp", "IMPMetadataReader", "empty"),
    ("imp", "IMPMetadataReader", "tiny_binary"),
    ("imp", "IMPMetadataReader", "png_header"),
    ("imp", "IMPMetadataReader", "html_document"),
    ("imp", "IMPMetadataReader", "empty_zip"),
    ("lit", "LITMetadataReader", "empty"),
    ("lit", "LITMetadataReader", "tiny_binary"),
    ("lit", "LITMetadataReader", "png_header"),
    ("lit", "LITMetadataReader", "html_document"),
    ("lit", "LITMetadataReader", "empty_zip"),
    ("pmlz", "PMLMetadataReader", "empty"),
    ("pmlz", "PMLMetadataReader", "tiny_binary"),
    ("pmlz", "PMLMetadataReader", "png_header"),
    ("pmlz", "PMLMetadataReader", "html_document"),
    ("pmlz", "PMLMetadataReader", "empty_zip"),
    ("tpz", "TOPAZMetadataReader", "empty"),
    ("tpz", "TOPAZMetadataReader", "tiny_binary"),
    ("tpz", "TOPAZMetadataReader", "png_header"),
    ("tpz", "TOPAZMetadataReader", "html_document"),
    ("tpz", "TOPAZMetadataReader", "empty_zip"),
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


@pytest.mark.parametrize(("extension", "reader_name", "payload_name"), STRICT_BINARY_CASES)
def test_strict_binary_extractors_reject_wrong_format_payloads(
    extension: str,
    reader_name: str,
    payload_name: str,
) -> None:
    """
    Binary readers should reject arbitrary bytes and header-only impostors.
    """
    from LiuXin_alpha.metadata.file_sources import get_metadata

    payload = MALFORMED_PAYLOADS[payload_name]
    with pytest.raises(RuntimeError, match=f"extension '{extension}'.*{reader_name}") as exc_info:
        get_metadata(_stream_for(extension, payload), force_type=extension)

    assert exc_info.value.__cause__ is not None


@pytest.mark.parametrize(("extension", "reader_name", "payload_name"), STRICT_LEGACY_CASES)
def test_strict_legacy_extractors_reject_wrong_format_payloads(
    extension: str,
    reader_name: str,
    payload_name: str,
) -> None:
    """
    Legacy/specialty readers should reject non-credible wrapper payloads.
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
        for extension in ("epub", "docx", "zip", "htmlz", "txtz", "odt", "rar")
        for entry in registry.iter_metadata_reader_entries_for_extension(extension)
    }
    assert {
        "EPUBMetadataReader",
        "DocXMetadataReader",
        "ZipMetadataReader",
        "HTMLZMetadataReader",
        "TXTZMetadataReader",
        "ODTMetadataReader",
        "RARMetadataReader",
    } <= reader_names


def test_registry_lists_strict_xml_readers_for_fuzzing() -> None:
    from LiuXin_alpha.metadata.file_sources import registry

    reader_names = {
        entry.name
        for extension in ("opf", "fb2")
        for entry in registry.iter_metadata_reader_entries_for_extension(extension)
    }
    assert {"OPFMetadataReader", "FB2MetadataReader"} <= reader_names


def test_registry_lists_strict_binary_readers_for_fuzzing() -> None:
    from LiuXin_alpha.metadata.file_sources import registry

    reader_names = {
        entry.name
        for extension in ("pdf", "mobi", "pdb", "lrf")
        for entry in registry.iter_metadata_reader_entries_for_extension(extension)
    }
    assert {"PDFMetadataReader", "MOBIMetadataReader", "PDBMetadataReader", "LRFMetadataReader"} <= reader_names


def test_registry_lists_strict_legacy_readers_for_fuzzing() -> None:
    from LiuXin_alpha.metadata.file_sources import registry

    reader_names = {
        entry.name
        for extension in ("rtf", "snb", "lrx", "rb", "imp", "lit", "pmlz", "tpz")
        for entry in registry.iter_metadata_reader_entries_for_extension(extension)
    }
    assert {
        "RTFMetadataReader",
        "SNBMetadataReader",
        "LRXMetadataReader",
        "RBMetadataReader",
        "IMPMetadataReader",
        "LITMetadataReader",
        "PMLMetadataReader",
        "TOPAZMetadataReader",
    } <= reader_names
