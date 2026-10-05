"""
Build deterministic MOBI fixtures and test doubles.

The module keeps generated data, ordering and failure modes explicit so consumers
can assert stable behavior.

Example:
    Exercise file format mobi through a consuming regression::

        python -m pytest -q tests/metadata/file_sources/test_mobi_metadata_source.py
"""
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
    """
    Record or discard MobiLog messages without requiring the production logging stack.

    Example:
        Exercise MobiLog through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_mobi_metadata_source.py
    """
    def __init__(self) -> None:
        """
        Initialize and validate the MobiLog test-support state.

        Example:
            Exercise MobiLog.  init   through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_mobi_metadata_source.py


        :return: None; completion is expressed through state changes or assertions.
        """
        self.messages: list[str] = []

    def _record(self, *parts) -> None:
        """
        Perform the record step with deterministic fixture inputs.

        Example:
            Exercise MobiLog. record through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_mobi_metadata_source.py


        :param parts: Message fragments joined for the test logger.
        :return: None; fixture state or the supplied destination is updated in place.
        """
        self.messages.append(" ".join(str(part) for part in parts))

    def __call__(self, *parts) -> None:
        """
        Execute the configured fixture builder or test double operation.

        Example:
            Exercise MobiLog.  call   through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_mobi_metadata_source.py


        :param parts: Message fragments joined for the test logger.
        :return: None; fixture state or the supplied destination is updated in place.
        """
        self._record(*parts)

    def debug(self, *parts) -> None:
        """
        Record or discard a debug message for assertions without external logging.

        Example:
            Exercise MobiLog.debug through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_mobi_metadata_source.py


        :param parts: Message fragments joined for the test logger.
        :return: None; fixture state or the supplied destination is updated in place.
        """
        self._record(*parts)

    def info(self, *parts) -> None:
        """
        Record or discard a info message for assertions without external logging.

        Example:
            Exercise MobiLog.info through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_mobi_metadata_source.py


        :param parts: Message fragments joined for the test logger.
        :return: None; fixture state or the supplied destination is updated in place.
        """
        self._record(*parts)

    def warn(self, *parts) -> None:
        """
        Record or discard a warn message for assertions without external logging.

        Example:
            Exercise MobiLog.warn through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_mobi_metadata_source.py


        :param parts: Message fragments joined for the test logger.
        :return: None; fixture state or the supplied destination is updated in place.
        """
        self._record(*parts)

    def warning(self, *parts) -> None:
        """
        Record or discard a warning message for assertions without external logging.

        Example:
            Exercise MobiLog.warning through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_mobi_metadata_source.py


        :param parts: Message fragments joined for the test logger.
        :return: None; fixture state or the supplied destination is updated in place.
        """
        self._record(*parts)

    def error(self, *parts) -> None:
        """
        Record or discard a error message for assertions without external logging.

        Example:
            Exercise MobiLog.error through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_mobi_metadata_source.py


        :param parts: Message fragments joined for the test logger.
        :return: None; fixture state or the supplied destination is updated in place.
        """
        self._record(*parts)

    def exception(self, *parts) -> None:
        """
        Record or discard a exception message for assertions without external logging.

        Example:
            Exercise MobiLog.exception through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_mobi_metadata_source.py


        :param parts: Message fragments joined for the test logger.
        :return: None; fixture state or the supplied destination is updated in place.
        """
        self._record(*parts)


@dataclass(frozen=True)
class PalmDBRecord:
    """
    Carry the deterministic PalmDBRecord inputs and expected values used by format tests.

    Example:
        Exercise PalmDBRecord through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_mobi_metadata_source.py
    """
    data: bytes
    flags: int = 0
    uid: int | None = None


def mobi_input_options(**overrides):
    """
    Perform the mobi input options step with deterministic fixture inputs.

    Example:
        Exercise mobi input options through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_mobi_metadata_source.py


    :param overrides: Value supplied for overrides under the deterministic fixture
        contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    values = {"input_encoding": "utf-8", "debug_pipeline": False}
    values.update(overrides)
    return SimpleNamespace(**values)


def mobi_stream(payload: bytes, *, name: str = "") -> io.BytesIO:
    """
    Perform the mobi stream step with deterministic fixture inputs.

    Example:
        Exercise mobi stream through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_mobi_metadata_source.py


    :param payload: Binary or structured payload encoded into the fixture.
    :param name: Stable fixture, profile, member or field name.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    stream = io.BytesIO(payload)
    stream.name = name
    return stream


def _as_bytes(value: bytes | bytearray | str, *, encoding: str = "utf-8") -> bytes:
    """
    Perform the as bytes step with deterministic fixture inputs.

    Example:
        Exercise  as bytes through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_mobi_metadata_source.py


    :param value: Fixture value normalized, encoded, stored or returned.
    :param encoding: Character encoding used for deterministic fixture bytes.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    if isinstance(value, bytes):
        return value
    if isinstance(value, bytearray):
        return bytes(value)
    return value.encode(encoding)


def _normalise_records(records: Sequence[bytes | bytearray | PalmDBRecord]) -> list[PalmDBRecord]:
    """
    Normalize normalise records into a stable fixture representation.

    Example:
        Exercise  normalise records through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_mobi_metadata_source.py


    :param records: Value supplied for records under the deterministic fixture contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
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
    """
    Perform the palmdb record step with deterministic fixture inputs.

    Example:
        Exercise palmdb record through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_mobi_metadata_source.py


    :param data: Bytes or structured data consumed by the operation.
    :param flags: Value supplied for flags under the deterministic fixture contract.
    :param uid: Value supplied for uid under the deterministic fixture contract.
    :param encoding: Character encoding used for deterministic fixture bytes.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    return PalmDBRecord(_as_bytes(data, encoding=encoding), flags=flags, uid=uid)


def build_palmdb(
    records: Sequence[bytes | bytearray | PalmDBRecord],
    *,
    name: bytes | str = "MOBI Fixture",
    ident: bytes = b"BOOKMOBI",
    record_offsets: Sequence[int] | None = None,
    last_record_uid: int | None = None,
) -> bytes:
    """
    Build palmdb for deterministic fixture consumers.

    Example:
        Exercise build palmdb through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_mobi_metadata_source.py


    :param records: Value supplied for records under the deterministic fixture contract.
    :param name: Stable fixture, profile, member or field name.
    :param ident: Value supplied for ident under the deterministic fixture contract.
    :param record_offsets: Value supplied for record offsets under the deterministic
        fixture contract.
    :param last_record_uid: Value supplied for last record uid under the deterministic
        fixture contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
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
    """
    Perform the mobi exth record step with deterministic fixture inputs.

    Example:
        Exercise mobi exth record through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_mobi_metadata_source.py


    :param code: Value supplied for code under the deterministic fixture contract.
    :param payload: Binary or structured payload encoded into the fixture.
    :param encoding: Character encoding used for deterministic fixture bytes.
    :param declared_size: Value supplied for declared size under the deterministic
        fixture contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
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
    """
    Build mobi exth for deterministic fixture consumers.

    Example:
        Exercise build mobi exth through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_mobi_metadata_source.py


    :param records: Value supplied for records under the deterministic fixture contract.
    :param encoding: Character encoding used for deterministic fixture bytes.
    :param length: Value supplied for length under the deterministic fixture contract.
    :param item_count: Value supplied for item count under the deterministic fixture
        contract.
    :param pad_to_four: Value supplied for pad to four under the deterministic fixture
        contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
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
    """
    Perform the default exth records step with deterministic fixture inputs.

    Example:
        Exercise  default exth records through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_mobi_metadata_source.py


    :param title: Value supplied for title under the deterministic fixture contract.
    :param authors: Value supplied for authors under the deterministic fixture contract.
    :param publisher: Value supplied for publisher under the deterministic fixture
        contract.
    :param comments: Value supplied for comments under the deterministic fixture
        contract.
    :param tags: Value supplied for tags under the deterministic fixture contract.
    :param language: Value supplied for language under the deterministic fixture
        contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
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
    """
    Build mobi record0 for deterministic fixture consumers.

    Example:
        Exercise build mobi record0 through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_mobi_metadata_source.py


    :param title: Value supplied for title under the deterministic fixture contract.
    :param authors: Value supplied for authors under the deterministic fixture contract.
    :param publisher: Value supplied for publisher under the deterministic fixture
        contract.
    :param comments: Value supplied for comments under the deterministic fixture
        contract.
    :param tags: Value supplied for tags under the deterministic fixture contract.
    :param language: Value supplied for language under the deterministic fixture
        contract.
    :param text_record_count: Value supplied for text record count under the
        deterministic fixture contract.
    :param compression: Value supplied for compression under the deterministic fixture
        contract.
    :param encryption_type: Value supplied for encryption type under the deterministic
        fixture contract.
    :param codepage: Value supplied for codepage under the deterministic fixture
        contract.
    :param mobi_version: Value supplied for mobi version under the deterministic fixture
        contract.
    :param unique_id: Value supplied for unique id under the deterministic fixture
        contract.
    :param mobi_type: Value supplied for mobi type under the deterministic fixture
        contract.
    :param first_image_index: Value supplied for first image index under the
        deterministic fixture contract.
    :param header_length: Value supplied for header length under the deterministic
        fixture contract.
    :param exth_records: Value supplied for exth records under the deterministic fixture
        contract.
    :param include_exth: Value supplied for include exth under the deterministic fixture
        contract.
    :param language_code: Value supplied for language code under the deterministic
        fixture contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
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
    """
    Build minimal mobi for deterministic fixture consumers.

    Example:
        Exercise build minimal mobi through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_mobi_metadata_source.py


    :param title: Value supplied for title under the deterministic fixture contract.
    :param authors: Value supplied for authors under the deterministic fixture contract.
    :param publisher: Value supplied for publisher under the deterministic fixture
        contract.
    :param comments: Value supplied for comments under the deterministic fixture
        contract.
    :param tags: Value supplied for tags under the deterministic fixture contract.
    :param language: Value supplied for language under the deterministic fixture
        contract.
    :param body_html: Value supplied for body html under the deterministic fixture
        contract.
    :param name: Stable fixture, profile, member or field name.
    :param ident: Value supplied for ident under the deterministic fixture contract.
    :param extra_records: Value supplied for extra records under the deterministic
        fixture contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
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
    """
    Perform the palmdb record count step with deterministic fixture inputs.

    Example:
        Exercise palmdb record count through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_mobi_metadata_source.py


    :param payload: Binary or structured payload encoded into the fixture.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    return unpack_from(">H", payload, 76)[0]


def palmdb_record_offsets(payload: bytes) -> list[int]:
    """
    Perform the palmdb record offsets step with deterministic fixture inputs.

    Example:
        Exercise palmdb record offsets through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_mobi_metadata_source.py


    :param payload: Binary or structured payload encoded into the fixture.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
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
    """
    Perform the rewrite palmdb record offsets step with deterministic fixture inputs.

    Example:
        Exercise rewrite palmdb record offsets through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_mobi_metadata_source.py


    :param payload: Binary or structured payload encoded into the fixture.
    :param offsets: Value supplied for offsets under the deterministic fixture contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    count = palmdb_record_count(payload)
    if len(offsets) != count:
        raise ValueError("offset count must match PalmDB record count")
    mutated = bytearray(payload)
    for index, offset in enumerate(offsets):
        entry_start = PALMDB_HEADER_SIZE + (index * PALMDB_RECORD_TABLE_ENTRY_SIZE)
        mutated[entry_start : entry_start + 4] = pack(">I", offset)
    return bytes(mutated)


def rewrite_palmdb_record_offset(payload: bytes, index: int, offset: int) -> bytes:
    """
    Perform the rewrite palmdb record offset step with deterministic fixture inputs.

    Example:
        Exercise rewrite palmdb record offset through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_mobi_metadata_source.py


    :param payload: Binary or structured payload encoded into the fixture.
    :param index: Value supplied for index under the deterministic fixture contract.
    :param offset: Value supplied for offset under the deterministic fixture contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    offsets = palmdb_record_offsets(payload)
    offsets[index] = offset
    return rewrite_palmdb_record_offsets(payload, offsets)


def truncate_mobi_payload(payload: bytes, size: int) -> bytes:
    """
    Perform the truncate mobi payload step with deterministic fixture inputs.

    Example:
        Exercise truncate mobi payload through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_mobi_metadata_source.py


    :param payload: Binary or structured payload encoded into the fixture.
    :param size: Value supplied for size under the deterministic fixture contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    if size < 0:
        raise ValueError("truncated payload size cannot be negative")
    return payload[:size]
