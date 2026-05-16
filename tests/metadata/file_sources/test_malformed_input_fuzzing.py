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


def test_registry_lists_strict_container_readers_for_fuzzing() -> None:
    from LiuXin_alpha.metadata.file_sources import registry

    reader_names = {
        entry.name
        for extension in ("epub", "docx", "zip")
        for entry in registry.iter_metadata_reader_entries_for_extension(extension)
    }
    assert {"EPUBMetadataReader", "DocXMetadataReader", "ZipMetadataReader"} <= reader_names
