from __future__ import annotations

import io
from dataclasses import dataclass
from struct import pack, unpack_from
from types import SimpleNamespace
from typing import Iterable, Sequence


NULL_INDEX = 0xFFFFFFFF
PALMDB_HEADER_SIZE = 78
PALMDB_RECORD_TABLE_ENTRY_SIZE = 8
DEFAULT_MOBI_HEADER_LENGTH = 0xE8
DEFAULT_RECORD_SIZE = 0x1000


class MobiLog:
    def __init__(self) -> None:
        self.messages: list[str] = []

    def _record(self, *parts) -> None:
        self.messages.append(" ".join(str(part) for part in parts))

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
class PalmDBRecord:
    data: bytes
    flags: int = 0
    uid: int | None = None


def mobi_input_options(**overrides):
    values = {"input_encoding": "utf-8", "debug_pipeline": False}
    values.update(overrides)
    return SimpleNamespace(**values)


def mobi_stream(payload: bytes, *, name: str = "") -> io.BytesIO:
    stream = io.BytesIO(payload)
    stream.name = name
    return stream


def _as_bytes(value: bytes | bytearray | str, *, encoding: str = "utf-8") -> bytes:
    if isinstance(value, bytes):
        return value
    if isinstance(value, bytearray):
        return bytes(value)
    return value.encode(encoding)


def _normalise_records(records: Sequence[bytes | bytearray | PalmDBRecord]) -> list[PalmDBRecord]:
    normalised = []
    for record in records:
        if isinstance(record, PalmDBRecord):
            normalised.append(record)
        else:
            normalised.append(PalmDBRecord(_as_bytes(record)))
    return normalised


def palmdb_record(
    data: bytes | bytearray | str,
    *,
    flags: int = 0,
    uid: int | None = None,
    encoding: str = "utf-8",
) -> PalmDBRecord:
    return PalmDBRecord(_as_bytes(data, encoding=encoding), flags=flags, uid=uid)


def build_palmdb(
    records: Sequence[bytes | bytearray | PalmDBRecord],
    *,
    name: bytes | str = "MOBI Fixture",
    ident: bytes = b"BOOKMOBI",
    record_offsets: Sequence[int] | None = None,
    last_record_uid: int | None = None,
) -> bytes:
    if len(ident) != 8:
        raise ValueError("PalmDB ident must be exactly 8 bytes")

    normalised = _normalise_records(records)
    name_bytes = _as_bytes(name)[:31]
    name_bytes += b"\0" * (32 - len(name_bytes))

    record_count = len(normalised)
    header = bytearray()
    header += name_bytes
    header += pack(">HHIIIIII", 0, 0, 0, 0, 0, 0, 0, 0)
    header += ident
    uid_seed = last_record_uid if last_record_uid is not None else max(0, (2 * record_count) - 1)
    header += pack(">IIH", uid_seed, 0, record_count)

    first_record_offset = len(header) + (PALMDB_RECORD_TABLE_ENTRY_SIZE * record_count) + 2
    computed_offsets = []
    offset = first_record_offset
    for record in normalised:
        computed_offsets.append(offset)
        offset += len(record.data)

    offsets = list(computed_offsets if record_offsets is None else record_offsets)
    if len(offsets) != record_count:
        raise ValueError("record_offsets length must match the record count")

    table = bytearray()
    for index, (record, record_offset) in enumerate(zip(normalised, offsets)):
        if not 0 <= record.flags <= 0xFF:
            raise ValueError("PalmDB record flags must fit in one byte")
        uid = (2 * index) if record.uid is None else record.uid
        if not 0 <= uid <= 0xFFFFFF:
            raise ValueError("PalmDB record UID must fit in three bytes")
        table += pack(">I", record_offset)
        table += bytes([record.flags])
        table += pack(">I", uid)[1:]

    return bytes(header + table + b"\0\0" + b"".join(record.data for record in normalised))


def mobi_exth_record(
    code: int,
    payload: bytes | bytearray | str | int,
    *,
    encoding: str = "utf-8",
    declared_size: int | None = None,
) -> bytes:
    if isinstance(payload, int):
        body = pack(">I", payload)
    else:
        body = _as_bytes(payload, encoding=encoding)
    size = declared_size if declared_size is not None else len(body) + 8
    return pack(">II", code, size) + body


def build_mobi_exth(
    records: Sequence[tuple[int, bytes | bytearray | str | int] | bytes],
    *,
    encoding: str = "utf-8",
    length: int | None = None,
    item_count: int | None = None,
    pad_to_four: bool = True,
) -> bytes:
    body_parts = []
    for record in records:
        if isinstance(record, bytes):
            body_parts.append(record)
        else:
            code, payload = record
            body_parts.append(mobi_exth_record(code, payload, encoding=encoding))
    body = b"".join(body_parts)
    padding = b""
    if pad_to_four:
        padding = b"\0" * ((4 - ((12 + len(body)) % 4)) % 4)
    declared_length = length if length is not None else 12 + len(body) + len(padding)
    declared_count = item_count if item_count is not None else len(records)
    return b"EXTH" + pack(">II", declared_length, declared_count) + body + padding


def _default_exth_records(
    *,
    title: str,
    authors: Sequence[str],
    publisher: str | None,
    comments: str | None,
    tags: Sequence[str],
    language: str | None,
) -> list[tuple[int, bytes | str | int]]:
    records: list[tuple[int, bytes | str | int]] = [(503, title)]
    records.extend((100, author) for author in authors)
    if publisher:
        records.append((101, publisher))
    if comments:
        records.append((103, comments))
    if tags:
        records.append((105, ";".join(tags)))
    if language:
        records.append((524, language))
    return records


def build_mobi_record0(
    *,
    title: str = "Fixture MOBI",
    authors: Sequence[str] = ("Fixture Author",),
    publisher: str | None = None,
    comments: str | None = None,
    tags: Sequence[str] = (),
    language: str | None = "en",
    text_record_count: int = 1,
    compression: bytes = b"\0\1",
    encryption_type: int = 0,
    codepage: int = 65001,
    mobi_version: int = 6,
    unique_id: int = 1,
    mobi_type: int = 2,
    first_image_index: int = 1,
    header_length: int = DEFAULT_MOBI_HEADER_LENGTH,
    exth_records: Sequence[tuple[int, bytes | bytearray | str | int] | bytes] | None = None,
    include_exth: bool = True,
    language_code: int = 9,
) -> bytes:
    if len(compression) != 2:
        raise ValueError("MOBI compression marker must be exactly two bytes")
    if header_length < DEFAULT_MOBI_HEADER_LENGTH:
        raise ValueError("MOBI fixture header length must be at least 0xE8")

    raw = bytearray(16 + header_length)
    raw[0:2] = compression
    raw[8:12] = pack(">HH", text_record_count, DEFAULT_RECORD_SIZE)
    raw[12:14] = pack(">H", encryption_type)
    raw[16:20] = b"MOBI"
    raw[20:40] = pack(">LLLLL", header_length, mobi_type, codepage, unique_id, mobi_version)
    raw[0x5C:0x60] = pack(">L", language_code)
    raw[0x68:0x6C] = pack(">L", mobi_version)
    raw[0x6C:0x70] = pack(">L", first_image_index)
    raw[0x80:0x84] = pack(">L", 0x40 if include_exth else 0)
    raw[0xC0:0xC8] = pack(">LL", NULL_INDEX, 0)
    raw[0xF4:0xF8] = pack(">L", NULL_INDEX)

    exth = b""
    if include_exth:
        records = exth_records
        if records is None:
            records = _default_exth_records(
                title=title,
                authors=authors,
                publisher=publisher,
                comments=comments,
                tags=tags,
                language=language,
            )
        exth = build_mobi_exth(records)

    title_bytes = title.encode("utf-8")
    title_offset = len(raw) + len(exth)
    raw[0x54:0x5C] = pack(">II", title_offset, len(title_bytes))

    record0 = bytes(raw) + exth + title_bytes + b"\0"
    record0 += b"\0" * ((4 - (len(record0) % 4)) % 4)
    return record0


def build_minimal_mobi(
    *,
    title: str = "Fixture MOBI",
    authors: Sequence[str] = ("Fixture Author",),
    publisher: str | None = None,
    comments: str | None = None,
    tags: Sequence[str] = (),
    language: str | None = "en",
    body_html: bytes | str | None = None,
    name: bytes | str = "MOBI Fixture",
    ident: bytes = b"BOOKMOBI",
    extra_records: Iterable[bytes | bytearray | PalmDBRecord] = (),
) -> bytes:
    if body_html is None:
        body_html = (
            "<html><head><title>{title}</title></head>"
            "<body><h1>{title}</h1><p>Fixture body.</p></body></html>"
        ).format(title=title)
    body = _as_bytes(body_html)
    additional = list(extra_records)
    record0 = build_mobi_record0(
        title=title,
        authors=authors,
        publisher=publisher,
        comments=comments,
        tags=tags,
        language=language,
        text_record_count=1,
        first_image_index=1,
    )
    return build_palmdb([record0, body, *additional], name=name, ident=ident)


def palmdb_record_count(payload: bytes) -> int:
    return unpack_from(">H", payload, 76)[0]


def palmdb_record_offsets(payload: bytes) -> list[int]:
    count = palmdb_record_count(payload)
    return [
        unpack_from(
            ">I",
            payload,
            PALMDB_HEADER_SIZE + (index * PALMDB_RECORD_TABLE_ENTRY_SIZE),
        )[0]
        for index in range(count)
    ]


def rewrite_palmdb_record_offsets(payload: bytes, offsets: Sequence[int]) -> bytes:
    count = palmdb_record_count(payload)
    if len(offsets) != count:
        raise ValueError("offset count must match PalmDB record count")
    mutated = bytearray(payload)
    for index, offset in enumerate(offsets):
        entry_start = PALMDB_HEADER_SIZE + (index * PALMDB_RECORD_TABLE_ENTRY_SIZE)
        mutated[entry_start : entry_start + 4] = pack(">I", offset)
    return bytes(mutated)


def rewrite_palmdb_record_offset(payload: bytes, index: int, offset: int) -> bytes:
    offsets = palmdb_record_offsets(payload)
    offsets[index] = offset
    return rewrite_palmdb_record_offsets(payload, offsets)


def truncate_mobi_payload(payload: bytes, size: int) -> bytes:
    if size < 0:
        raise ValueError("truncated payload size cannot be negative")
    return payload[:size]
