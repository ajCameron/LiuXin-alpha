from __future__ import annotations

import io
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from struct import pack
from types import MethodType, SimpleNamespace

from LiuXin_alpha.file_formats.lit.reader import (
    FLAG_CLOSING,
    FLAG_OPENING,
    HTML_MAP,
    DirectoryEntry,
    LitFile,
    ManifestItem,
    UnBinary,
)


class LitLog:
    def __init__(self) -> None:
        self.messages: list[str] = []

    def _record(self, *parts) -> None:
        self.messages.append(" ".join(str(x) for x in parts))

    def __call__(self, *parts) -> None:
        self._record(*parts)

    def debug(self, *parts) -> None:
        self._record(*parts)

    def info(self, *parts) -> None:
        self._record(*parts)

    def warn(self, *parts) -> None:
        self._record(*parts)

    def warning(self, *parts) -> None:
        self._record(*parts)

    def error(self, *parts) -> None:
        self._record(*parts)

    def exception(self, *parts) -> None:
        self._record(*parts)


@dataclass(frozen=True)
class LitManifestRecord:
    internal: str
    original: str
    mime_type: str
    offset: int = 0
    state: str = "spine"


def lit_options(**overrides):
    values = {"pretty_print": False}
    values.update(overrides)
    return SimpleNamespace(**values)


def lit_stream(payload: bytes, *, name: str = "fixture.lit") -> io.BytesIO:
    stream = io.BytesIO(payload)
    stream.name = name
    return stream


def build_lit_header_payload(
    *,
    magic: bytes = b"ITOLITLS",
    version: int = 1,
    hdr_len: int = 40,
    num_pieces: int = 0,
    sec_hdr_len: int | None = None,
    guid: bytes = b"\0" * 16,
    secondary_header: bytes = b"",
) -> bytes:
    if len(magic) != 8:
        raise ValueError("LIT magic must be exactly 8 bytes")
    if len(guid) != 16:
        raise ValueError("LIT GUID must be exactly 16 bytes")
    if hdr_len < 40:
        raise ValueError("LIT primary header length must be at least 40 bytes")

    sec_len = len(secondary_header) if sec_hdr_len is None else sec_hdr_len
    primary = magic + pack("<Iiii", version, hdr_len, num_pieces, sec_len) + guid
    return primary + (b"\0" * (hdr_len - len(primary))) + secondary_header


def build_lit_secondary_header(*blocks: bytes, start_offset: int = 8) -> bytes:
    return b"\0\0\0\0" + pack("<l", start_offset) + b"".join(blocks)


def lit_sized_utf8(value: str, *, zpad: bool = False) -> bytes:
    if len(value) > 255:
        raise ValueError("LIT sized UTF-8 helper supports up to 255 characters")
    payload = bytes([len(value)]) + value.encode("utf-8")
    if zpad:
        payload += b"\0"
    return payload


def build_lit_manifest_payload(
    records: Sequence[LitManifestRecord],
    *,
    root: str = "\\",
) -> bytes:
    root_bytes = root.encode("utf-8")
    if len(root_bytes) > 255:
        raise ValueError("LIT manifest root is too long for the test helper")

    payload = [bytes([len(root_bytes)]), root_bytes]
    for state in ("spine", "not spine", "css", "images"):
        state_records = [record for record in records if record.state == state]
        payload.append(pack("<I", len(state_records)))
        for record in state_records:
            payload.append(pack("<I", record.offset))
            payload.append(lit_sized_utf8(record.internal))
            payload.append(lit_sized_utf8(record.original))
            payload.append(lit_sized_utf8(record.mime_type, zpad=True))
    payload.append(b"\0")
    return b"".join(payload)


def build_lit_namelist_payload(section_names: Sequence[str]) -> bytes:
    payload = [pack("<HH", 0x3C, len(section_names))]
    for name in section_names:
        encoded = name.encode("utf-16-le")
        payload.append(pack("<H", len(encoded) // 2))
        payload.append(encoded)
        payload.append(b"\0\0")
    return b"".join(payload)


def in_memory_lit_file(
    files: Mapping[str, bytes],
    *,
    opf_path: str = "content.opf",
    log: LitLog | None = None,
):
    payloads = dict(files)
    logger = log or LitLog()
    lit = object.__new__(LitFile)
    lit.entries = {
        name: DirectoryEntry(name, 0, 0, len(payload))
        for name, payload in payloads.items()
    }
    lit.opf_path = opf_path
    lit._warn = logger.warn

    def _get_file(self, name: str) -> bytes:
        return payloads[name]

    lit.get_file = MethodType(_get_file, lit)
    return lit


def read_manifest_from_payload(
    payload: bytes,
    *,
    opf_path: str = "content.opf",
    log: LitLog | None = None,
):
    lit = in_memory_lit_file({"/manifest": payload}, opf_path=opf_path, log=log)
    LitFile.read_manifest(lit)
    return lit


def read_namelist_from_payload(payload: bytes, *, log: LitLog | None = None):
    lit = in_memory_lit_file({"::DataSpace/NameList": payload}, log=log)
    LitFile.read_section_names(lit)
    return lit


def lit_manifest_item(
    *,
    original: str,
    internal: str,
    mime_type: str = "application/xhtml+xml",
    offset: int = 0,
    root: str = "\\",
    state: str = "spine",
) -> ManifestItem:
    return ManifestItem(original, internal, mime_type, offset, root, state)


def lit_html_tag_id(tag_name: str) -> int:
    tags = HTML_MAP[0]
    for index, tag in enumerate(tags):
        if tag == tag_name:
            return index
    raise KeyError(tag_name)


def lit_html_attr_id(tag_name: str, attr_name: str) -> int:
    tag_id = lit_html_tag_id(tag_name)
    local_attrs = HTML_MAP[2][tag_id] or {}
    for attrs in (local_attrs, HTML_MAP[1]):
        for attr_id, attr in attrs.items():
            if attr == attr_name:
                return attr_id
    raise KeyError(f"{tag_name}@{attr_name}")


def lit_internal_href(target: str) -> str:
    return "\x02" + target


def lit_external_href(target: str) -> str:
    return "\x03" + target


def _codepoint(value: int) -> bytes:
    return chr(value).encode("utf-8")


def _text_payload(value: str | bytes) -> bytes:
    if isinstance(value, bytes):
        return value
    return value.encode("utf-8")


def lit_binary_element(
    tag_name: str,
    text: str | bytes = b"",
    *,
    attrs: Mapping[str, str] | Sequence[tuple[str, str]] | None = None,
    children: Sequence[bytes] = (),
) -> bytes:
    tag_id = lit_html_tag_id(tag_name)
    attr_items = attrs.items() if isinstance(attrs, Mapping) else (attrs or ())

    payload = [b"\0", _codepoint(FLAG_OPENING), _codepoint(tag_id)]
    for attr_name, attr_value in attr_items:
        attr_text = str(attr_value)
        payload.append(_codepoint(lit_html_attr_id(tag_name, attr_name)))
        payload.append(_codepoint(len(attr_text) + 1))
        payload.append(attr_text.encode("utf-8"))
    payload.append(b"\0")
    payload.append(_text_payload(text))
    payload.extend(children)
    payload.extend([b"\0", _codepoint(FLAG_CLOSING), b"\0"])
    return b"".join(payload)


def render_unbinary_html(
    payload: bytes,
    *,
    path: str = "chapter.xhtml",
    manifest: Mapping[str, ManifestItem] | None = None,
) -> str:
    unbin = UnBinary(payload, path, dict(manifest or {}))
    return unbin.unicode_representation
