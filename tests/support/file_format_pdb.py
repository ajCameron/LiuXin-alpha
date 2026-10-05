"""
Build deterministic PDB fixtures and test doubles.

The module keeps generated data, ordering and failure modes explicit so consumers
can assert stable behavior.

Example:
    Exercise file format pdb through a consuming regression::

        python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_source.py
"""
from __future__ import annotations

import io
import zlib
from dataclasses import dataclass
from struct import pack, unpack_from
from types import SimpleNamespace
from typing import Sequence


PALMDB_HEADER_SIZE = 78
PALMDB_RECORD_TABLE_ENTRY_SIZE = 8
PALMDOC_RECORD_SIZE = 4096
ZTXT_RECORD_SIZE = 8192


class PdbLog:
    """
    Record or discard PdbLog messages without requiring the production logging stack.

    Example:
        Exercise PdbLog through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_source.py
    """
    def __init__(self) -> None:
        """
        Initialize and validate the PdbLog test-support state.

        Example:
            Exercise PdbLog.  init   through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_source.py


        :return: None; completion is expressed through state changes or assertions.
        """
        self.messages: list[str] = []

    def _record(self, *parts) -> None:
        """
        Perform the record step with deterministic fixture inputs.

        Example:
            Exercise PdbLog. record through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_source.py


        :param parts: Message fragments joined for the test logger.
        :return: None; fixture state or the supplied destination is updated in place.
        """
        self.messages.append(" ".join(str(part) for part in parts))

    def __call__(self, *parts) -> None:
        """
        Execute the configured fixture builder or test double operation.

        Example:
            Exercise PdbLog.  call   through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_source.py


        :param parts: Message fragments joined for the test logger.
        :return: None; fixture state or the supplied destination is updated in place.
        """
        self._record(*parts)

    def debug(self, *parts) -> None:
        """
        Record or discard a debug message for assertions without external logging.

        Example:
            Exercise PdbLog.debug through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_source.py


        :param parts: Message fragments joined for the test logger.
        :return: None; fixture state or the supplied destination is updated in place.
        """
        self._record(*parts)

    def info(self, *parts) -> None:
        """
        Record or discard a info message for assertions without external logging.

        Example:
            Exercise PdbLog.info through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_source.py


        :param parts: Message fragments joined for the test logger.
        :return: None; fixture state or the supplied destination is updated in place.
        """
        self._record(*parts)

    def warn(self, *parts) -> None:
        """
        Record or discard a warn message for assertions without external logging.

        Example:
            Exercise PdbLog.warn through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_source.py


        :param parts: Message fragments joined for the test logger.
        :return: None; fixture state or the supplied destination is updated in place.
        """
        self._record(*parts)

    def warning(self, *parts) -> None:
        """
        Record or discard a warning message for assertions without external logging.

        Example:
            Exercise PdbLog.warning through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_source.py


        :param parts: Message fragments joined for the test logger.
        :return: None; fixture state or the supplied destination is updated in place.
        """
        self._record(*parts)

    def error(self, *parts) -> None:
        """
        Record or discard a error message for assertions without external logging.

        Example:
            Exercise PdbLog.error through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_source.py


        :param parts: Message fragments joined for the test logger.
        :return: None; fixture state or the supplied destination is updated in place.
        """
        self._record(*parts)

    def exception(self, *parts) -> None:
        """
        Record or discard a exception message for assertions without external logging.

        Example:
            Exercise PdbLog.exception through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_source.py


        :param parts: Message fragments joined for the test logger.
        :return: None; fixture state or the supplied destination is updated in place.
        """
        self._record(*parts)


@dataclass(frozen=True)
class PdbRecord:
    """
    Carry the deterministic PdbRecord inputs and expected values used by format tests.

    Example:
        Exercise PdbRecord through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_source.py
    """
    data: bytes
    flags: int = 0
    uid: int | None = None


def pdb_input_options(**overrides):
    """
    Perform the pdb input options step with deterministic fixture inputs.

    Example:
        Exercise pdb input options through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_source.py


    :param overrides: Value supplied for overrides under the deterministic fixture
        contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    values = {
        "input_encoding": "utf-8",
        "debug_pipeline": None,
        "pdb_output_encoding": "cp1252",
        "title": None,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def pdb_stream(payload: bytes, *, name: str = "") -> io.BytesIO:
    """
    Perform the pdb stream step with deterministic fixture inputs.

    Example:
        Exercise pdb stream through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_source.py


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

            python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_source.py


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


def _identity_bytes(ident: bytes | str) -> bytes:
    """
    Perform the identity bytes step with deterministic fixture inputs.

    Example:
        Exercise  identity bytes through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_source.py


    :param ident: Value supplied for ident under the deterministic fixture contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    raw = _as_bytes(ident)
    if len(raw) != 8:
        raise ValueError("PDB identity must be exactly 8 bytes")
    return raw


def _normalise_records(records: Sequence[bytes | bytearray | PdbRecord]) -> list[PdbRecord]:
    """
    Normalize normalise records into a stable fixture representation.

    Example:
        Exercise  normalise records through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_source.py


    :param records: Value supplied for records under the deterministic fixture contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    normalised = []
    for record in records:
        if isinstance(record, PdbRecord):
            normalised.append(record)
        else:
            normalised.append(PdbRecord(_as_bytes(record)))
    return normalised


def pdb_record(
    data: bytes | bytearray | str,
    *,
    flags: int = 0,
    uid: int | None = None,
    encoding: str = "utf-8",
) -> PdbRecord:
    """
    Perform the pdb record step with deterministic fixture inputs.

    Example:
        Exercise pdb record through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_source.py


    :param data: Bytes or structured data consumed by the operation.
    :param flags: Value supplied for flags under the deterministic fixture contract.
    :param uid: Value supplied for uid under the deterministic fixture contract.
    :param encoding: Character encoding used for deterministic fixture bytes.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    return PdbRecord(_as_bytes(data, encoding=encoding), flags=flags, uid=uid)


def build_pdb(
    records: Sequence[bytes | bytearray | PdbRecord],
    *,
    title: bytes | str = "PDB Fixture",
    ident: bytes | str = "TEXtREAd",
    record_offsets: Sequence[int] | None = None,
    last_record_uid: int | None = None,
) -> bytes:
    """
    Build pdb for deterministic fixture consumers.

    Example:
        Exercise build pdb through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_source.py


    :param records: Value supplied for records under the deterministic fixture contract.
    :param title: Value supplied for title under the deterministic fixture contract.
    :param ident: Value supplied for ident under the deterministic fixture contract.
    :param record_offsets: Value supplied for record offsets under the deterministic
        fixture contract.
    :param last_record_uid: Value supplied for last record uid under the deterministic
        fixture contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    ident_bytes = _identity_bytes(ident)
    normalised = _normalise_records(records)
    title_bytes = _as_bytes(title)[:31]
    title_bytes += b"\0" * (32 - len(title_bytes))

    record_count = len(normalised)
    header = bytearray()
    header += title_bytes
    header += pack(">HHIIIIII", 0, 0, 0, 0, 0, 0, 0, 0)
    uid_seed = last_record_uid if last_record_uid is not None else max(0, (2 * record_count) - 1)
    header += ident_bytes + pack(">IIH", uid_seed, 0, record_count)

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
            raise ValueError("PDB record flags must fit in one byte")
        uid = (2 * index) if record.uid is None else record.uid
        if not 0 <= uid <= 0xFFFFFF:
            raise ValueError("PDB record UID must fit in three bytes")
        table += pack(">I", record_offset)
        table += bytes([record.flags])
        table += pack(">I", uid)[1:]

    return bytes(header + table + b"\0\0" + b"".join(record.data for record in normalised))


def pdb_record_count(payload: bytes) -> int:
    """
    Perform the pdb record count step with deterministic fixture inputs.

    Example:
        Exercise pdb record count through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_source.py


    :param payload: Binary or structured payload encoded into the fixture.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    return unpack_from(">H", payload, 76)[0]


def pdb_record_offsets(payload: bytes) -> list[int]:
    """
    Perform the pdb record offsets step with deterministic fixture inputs.

    Example:
        Exercise pdb record offsets through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_source.py


    :param payload: Binary or structured payload encoded into the fixture.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    count = pdb_record_count(payload)
    return [
        unpack_from(
            ">I",
            payload,
            PALMDB_HEADER_SIZE + (index * PALMDB_RECORD_TABLE_ENTRY_SIZE),
        )[0]
        for index in range(count)
    ]


def rewrite_pdb_record_offsets(payload: bytes, offsets: Sequence[int]) -> bytes:
    """
    Perform the rewrite pdb record offsets step with deterministic fixture inputs.

    Example:
        Exercise rewrite pdb record offsets through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_source.py


    :param payload: Binary or structured payload encoded into the fixture.
    :param offsets: Value supplied for offsets under the deterministic fixture contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    count = pdb_record_count(payload)
    if len(offsets) != count:
        raise ValueError("offset count must match PDB record count")
    mutated = bytearray(payload)
    for index, offset in enumerate(offsets):
        entry_start = PALMDB_HEADER_SIZE + (index * PALMDB_RECORD_TABLE_ENTRY_SIZE)
        mutated[entry_start : entry_start + 4] = pack(">I", offset)
    return bytes(mutated)


def rewrite_pdb_record_offset(payload: bytes, index: int, offset: int) -> bytes:
    """
    Perform the rewrite pdb record offset step with deterministic fixture inputs.

    Example:
        Exercise rewrite pdb record offset through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_source.py


    :param payload: Binary or structured payload encoded into the fixture.
    :param index: Value supplied for index under the deterministic fixture contract.
    :param offset: Value supplied for offset under the deterministic fixture contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    offsets = pdb_record_offsets(payload)
    offsets[index] = offset
    return rewrite_pdb_record_offsets(payload, offsets)


def truncate_pdb_payload(payload: bytes, size: int) -> bytes:
    """
    Perform the truncate pdb payload step with deterministic fixture inputs.

    Example:
        Exercise truncate pdb payload through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_source.py


    :param payload: Binary or structured payload encoded into the fixture.
    :param size: Value supplied for size under the deterministic fixture contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    if size < 0:
        raise ValueError("truncated payload size cannot be negative")
    return payload[:size]


def build_palmdoc_header_record(
    *,
    text_length: int,
    record_count: int,
    compression: int = 1,
    record_size: int = PALMDOC_RECORD_SIZE,
) -> bytes:
    """
    Build palmdoc header record for deterministic fixture consumers.

    Example:
        Exercise build palmdoc header record through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_source.py


    :param text_length: Value supplied for text length under the deterministic fixture
        contract.
    :param record_count: Value supplied for record count under the deterministic fixture
        contract.
    :param compression: Value supplied for compression under the deterministic fixture
        contract.
    :param record_size: Value supplied for record size under the deterministic fixture
        contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    return b"".join(
        [
            pack(">H", compression),
            pack(">H", 0),
            pack(">L", text_length),
            pack(">H", record_count),
            pack(">H", record_size),
            pack(">L", 0),
        ]
    )


def build_minimal_palmdoc_pdb(
    *,
    title: str = "PalmDOC Fixture",
    body_text: bytes | str = "PalmDOC body.",
    encoding: str = "utf-8",
    compression: int = 1,
) -> bytes:
    """
    Build minimal palmdoc pdb for deterministic fixture consumers.

    Example:
        Exercise build minimal palmdoc pdb through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_source.py


    :param title: Value supplied for title under the deterministic fixture contract.
    :param body_text: Value supplied for body text under the deterministic fixture
        contract.
    :param encoding: Character encoding used for deterministic fixture bytes.
    :param compression: Value supplied for compression under the deterministic fixture
        contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    body = _as_bytes(body_text, encoding=encoding)
    header = build_palmdoc_header_record(
        text_length=len(body),
        record_count=1,
        compression=compression,
    )
    return build_pdb([header, body], title=title, ident="TEXtREAd")


def build_ztxt_header_record(
    *,
    text_length: int,
    record_count: int,
    crc32: int = 0,
    version: int = 0x012C,
    record_size: int = ZTXT_RECORD_SIZE,
    flags: int = 1,
) -> bytes:
    """
    Build ztxt header record for deterministic fixture consumers.

    Example:
        Exercise build ztxt header record through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_source.py


    :param text_length: Value supplied for text length under the deterministic fixture
        contract.
    :param record_count: Value supplied for record count under the deterministic fixture
        contract.
    :param crc32: Value supplied for crc32 under the deterministic fixture contract.
    :param version: Value supplied for version under the deterministic fixture contract.
    :param record_size: Value supplied for record size under the deterministic fixture
        contract.
    :param flags: Value supplied for flags under the deterministic fixture contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    return b"".join(
        [
            pack(">H", version),
            pack(">H", record_count),
            pack(">L", text_length),
            pack(">H", record_size),
            pack(">H", 0),
            pack(">H", 0),
            pack(">H", 0),
            pack(">H", 0),
            pack(">B", flags),
            pack(">B", 0),
            pack(">L", crc32),
            pack(">LL", 0, 0),
        ]
    )


def ztxt_compressed_records(records: Sequence[bytes | bytearray | str]) -> tuple[list[bytes], int, int]:
    """
    Perform the ztxt compressed records step with deterministic fixture inputs.

    Example:
        Exercise ztxt compressed records through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_source.py


    :param records: Value supplied for records under the deterministic fixture contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    compressor = zlib.compressobj(9)
    compressed = []
    total_length = 0
    crc32 = 0
    for record in records:
        raw = _as_bytes(record)
        total_length += len(raw)
        chunk = compressor.compress(raw) + compressor.flush(zlib.Z_FULL_FLUSH)
        compressed.append(chunk)
        crc32 = zlib.crc32(chunk, crc32) & 0xFFFFFFFF
    return compressed, total_length, crc32


def build_minimal_ztxt_pdb(
    *,
    title: str = "zTXT Fixture",
    body_text: bytes | str = "zTXT body.",
) -> bytes:
    """
    Build minimal ztxt pdb for deterministic fixture consumers.

    Example:
        Exercise build minimal ztxt pdb through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_source.py


    :param title: Value supplied for title under the deterministic fixture contract.
    :param body_text: Value supplied for body text under the deterministic fixture
        contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    compressed, total_length, crc32 = ztxt_compressed_records([body_text])
    header = build_ztxt_header_record(
        text_length=total_length,
        record_count=len(compressed),
        crc32=crc32,
    )
    return build_pdb([header, *compressed], title=title, ident="zTXTGPlm")


def build_ereader_header_record(
    *,
    metadata_offset: int = 1,
    last_data_offset: int = 2,
    image_count: int = 0,
    image_data_offset: int = 0,
    compression: int = 10,
    has_metadata: int = 1,
    non_text_offset: int = 1,
) -> bytes:
    """
    Build ereader header record for deterministic fixture consumers.

    Example:
        Exercise build ereader header record through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_source.py


    :param metadata_offset: Value supplied for metadata offset under the deterministic
        fixture contract.
    :param last_data_offset: Value supplied for last data offset under the deterministic
        fixture contract.
    :param image_count: Value supplied for image count under the deterministic fixture
        contract.
    :param image_data_offset: Value supplied for image data offset under the
        deterministic fixture contract.
    :param compression: Value supplied for compression under the deterministic fixture
        contract.
    :param has_metadata: Value supplied for has metadata under the deterministic fixture
        contract.
    :param non_text_offset: Value supplied for non text offset under the deterministic
        fixture contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    header = bytearray(132)

    def put(offset: int, value: int) -> None:
        """
        Perform the put step with deterministic fixture inputs.

        Example:
            Exercise build ereader header record.put through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_source.py


        :param offset: Value supplied for offset under the deterministic fixture contract.
        :param value: Fixture value normalized, encoded, stored or returned.
        :return: None; fixture state or the supplied destination is updated in place.
        """
        header[offset : offset + 2] = pack(">H", value)

    put(0, compression)
    put(12, non_text_offset)
    put(20, image_count)
    put(24, has_metadata)
    put(40, image_data_offset)
    put(44, metadata_offset)
    put(52, last_data_offset)
    return bytes(header)


def build_ereader202_header_record(
    *,
    version: int = 2,
    non_text_offset: int = 1,
    size: int = 116,
) -> bytes:
    """
    Build ereader202 header record for deterministic fixture consumers.

    Example:
        Exercise build ereader202 header record through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_source.py


    :param version: Value supplied for version under the deterministic fixture contract.
    :param non_text_offset: Value supplied for non text offset under the deterministic
        fixture contract.
    :param size: Value supplied for size under the deterministic fixture contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    header = bytearray(size)

    def put(offset: int, value: int) -> None:
        """
        Perform the put step with deterministic fixture inputs.

        Example:
            Exercise build ereader202 header record.put through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_source.py


        :param offset: Value supplied for offset under the deterministic fixture contract.
        :param value: Fixture value normalized, encoded, stored or returned.
        :return: None; fixture state or the supplied destination is updated in place.
        """
        header[offset : offset + 2] = pack(">H", value)

    put(0, version)
    put(8, non_text_offset)
    return bytes(header)


def build_ereader_image_record(
    *,
    name: bytes | str,
    payload: bytes | str = b"image-payload",
    prefix: bytes = b"PNG ",
) -> bytes:
    """
    Build ereader image record for deterministic fixture consumers.

    Example:
        Exercise build ereader image record through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_source.py


    :param name: Stable fixture, profile, member or field name.
    :param payload: Binary or structured payload encoded into the fixture.
    :param prefix: Value supplied for prefix under the deterministic fixture contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    raw_name = _as_bytes(name, encoding="ascii")[:32].ljust(32, b"\0")
    return prefix + raw_name + (b"\0" * (62 - len(prefix) - len(raw_name))) + _as_bytes(payload)


def build_ereader_metadata_record(
    *,
    title: str,
    author: str = "Fixture Author",
    publisher: str = "",
    isbn: str = "",
    encoding: str = "utf-8",
) -> bytes:
    """
    Build ereader metadata record for deterministic fixture consumers.

    Example:
        Exercise build ereader metadata record through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_source.py


    :param title: Value supplied for title under the deterministic fixture contract.
    :param author: Value supplied for author under the deterministic fixture contract.
    :param publisher: Value supplied for publisher under the deterministic fixture
        contract.
    :param isbn: Value supplied for isbn under the deterministic fixture contract.
    :param encoding: Character encoding used for deterministic fixture bytes.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    payload = f"{title}\0{author}\0\0{publisher}\0{isbn}\0"
    return payload.encode(encoding)


def build_minimal_ereader_pdb(
    *,
    title: str = "eReader Wrapper",
    metadata_title: str = "eReader Fixture",
    author: str = "Fixture Author",
    publisher: str = "",
    isbn: str = "",
) -> bytes:
    """
    Build minimal ereader pdb for deterministic fixture consumers.

    Example:
        Exercise build minimal ereader pdb through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_source.py


    :param title: Value supplied for title under the deterministic fixture contract.
    :param metadata_title: Value supplied for metadata title under the deterministic
        fixture contract.
    :param author: Value supplied for author under the deterministic fixture contract.
    :param publisher: Value supplied for publisher under the deterministic fixture
        contract.
    :param isbn: Value supplied for isbn under the deterministic fixture contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    header = build_ereader_header_record(metadata_offset=1, last_data_offset=2)
    metadata = build_ereader_metadata_record(
        title=metadata_title,
        author=author,
        publisher=publisher,
        isbn=isbn,
    )
    return build_pdb([header, metadata, b"MeTaInFo\0"], title=title, ident="PNPdPPrs")


def build_haodoo_legacy_header_record(
    *,
    title: str = "Haodoo Legacy",
    record_count: int = 1,
    chapter_titles: Sequence[str] = ("Chapter One",),
    encoding: str = "cp950",
) -> bytes:
    """
    Build haodoo legacy header record for deterministic fixture consumers.

    Example:
        Exercise build haodoo legacy header record through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_source.py


    :param title: Value supplied for title under the deterministic fixture contract.
    :param record_count: Value supplied for record count under the deterministic fixture
        contract.
    :param chapter_titles: Value supplied for chapter titles under the deterministic
        fixture contract.
    :param encoding: Character encoding used for deterministic fixture bytes.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    fields = [
        title.encode(encoding),
        str(record_count).encode("ascii"),
        *[chapter.encode(encoding) for chapter in chapter_titles],
    ]
    return b"\x1b".join(fields)


def build_haodoo_unicode_header_record(
    *,
    title: str = "Haodoo Unicode",
    record_count: int = 1,
    chapter_titles: Sequence[str] = ("Chapter One",),
) -> bytes:
    """
    Build haodoo unicode header record for deterministic fixture consumers.

    Example:
        Exercise build haodoo unicode header record through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_source.py


    :param title: Value supplied for title under the deterministic fixture contract.
    :param record_count: Value supplied for record count under the deterministic fixture
        contract.
    :param chapter_titles: Value supplied for chapter titles under the deterministic
        fixture contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    title_field = title.encode("utf_16_le")
    count_field = str(record_count).encode("ascii")
    chapters_field = b"\r\x00\n\x00".join(
        chapter.encode("utf_16_le") for chapter in chapter_titles
    )
    return b"\x1b\x00".join((title_field, count_field, chapters_field))


def build_minimal_haodoo_pdb(
    *,
    title: str = "Haodoo Wrapper",
    book_title: str = "Haodoo Fixture",
    chapter_title: str = "Chapter One",
    body_text: str = "Chapter One\nHaodoo body.",
    unicode_variant: bool = False,
) -> bytes:
    """
    Build minimal haodoo pdb for deterministic fixture consumers.

    Example:
        Exercise build minimal haodoo pdb through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_source.py


    :param title: Value supplied for title under the deterministic fixture contract.
    :param book_title: Value supplied for book title under the deterministic fixture
        contract.
    :param chapter_title: Value supplied for chapter title under the deterministic
        fixture contract.
    :param body_text: Value supplied for body text under the deterministic fixture
        contract.
    :param unicode_variant: Value supplied for unicode variant under the deterministic
        fixture contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    if unicode_variant:
        record0 = build_haodoo_unicode_header_record(
            title=book_title,
            record_count=1,
            chapter_titles=(chapter_title,),
        )
        body = body_text.encode("utf_16_le")
        ident = "BOOKMTIU"
    else:
        record0 = build_haodoo_legacy_header_record(
            title=book_title,
            record_count=1,
            chapter_titles=(chapter_title,),
        )
        body = body_text.encode("cp950")
        ident = "BOOKMTIT"
    return build_pdb([record0, body], title=title, ident=ident)


def build_plucker_record(record_type: int, payload: bytes, *, length_words: int | None = None) -> bytes:
    """
    Build plucker record for deterministic fixture consumers.

    Example:
        Exercise build plucker record through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_source.py


    :param record_type: Value supplied for record type under the deterministic fixture
        contract.
    :param payload: Binary or structured payload encoded into the fixture.
    :param length_words: Value supplied for length words under the deterministic fixture
        contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    if length_words is None:
        if len(payload) % 2:
            payload += b"\0"
        length_words = (4 + len(payload)) // 2
    return pack(">HH", record_type, length_words) + payload


def build_plucker_header_record(
    *,
    uid: int = 1,
    compression: int = 2,
    records: Sequence[tuple[int, int]] = ((0, 1),),
) -> bytes:
    """
    Build plucker header record for deterministic fixture consumers.

    Example:
        Exercise build plucker header record through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_source.py


    :param uid: Value supplied for uid under the deterministic fixture contract.
    :param compression: Value supplied for compression under the deterministic fixture
        contract.
    :param records: Value supplied for records under the deterministic fixture contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    return pack(">HHH", uid, compression, len(records)) + b"".join(pack(">HH", name, local_id) for name, local_id in records)


def build_plucker_section(
    *,
    uid: int,
    datatype: int,
    payload: bytes,
    paragraphs: int = 0,
    size: int | None = None,
    flags: int = 0,
) -> bytes:
    """
    Build plucker section for deterministic fixture consumers.

    Example:
        Exercise build plucker section through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_source.py


    :param uid: Value supplied for uid under the deterministic fixture contract.
    :param datatype: Value supplied for datatype under the deterministic fixture
        contract.
    :param payload: Binary or structured payload encoded into the fixture.
    :param paragraphs: Value supplied for paragraphs under the deterministic fixture
        contract.
    :param size: Value supplied for size under the deterministic fixture contract.
    :param flags: Value supplied for flags under the deterministic fixture contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    declared_size = len(payload) if size is None else size
    return pack(">HHHBB", uid, paragraphs, declared_size, datatype, flags) + payload


def build_plucker_text_section(
    *,
    uid: int,
    data: bytes,
    datatype: int = 0,
    paragraph_sizes: Sequence[int] = (),
    attributes: Sequence[int] | None = None,
) -> bytes:
    """
    Build plucker text section for deterministic fixture consumers.

    Example:
        Exercise build plucker text section through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_source.py


    :param uid: Value supplied for uid under the deterministic fixture contract.
    :param data: Bytes or structured data consumed by the operation.
    :param datatype: Value supplied for datatype under the deterministic fixture
        contract.
    :param paragraph_sizes: Value supplied for paragraph sizes under the deterministic
        fixture contract.
    :param attributes: Value supplied for attributes under the deterministic fixture
        contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    attrs = list(attributes) if attributes is not None else [0] * len(paragraph_sizes)
    if len(attrs) != len(paragraph_sizes):
        raise ValueError("paragraph_sizes and attributes must have the same length")
    paragraph_table = b"".join(pack(">HH", size, attr) for size, attr in zip(paragraph_sizes, attrs))
    return build_plucker_section(
        uid=uid,
        datatype=datatype,
        payload=paragraph_table + data,
        paragraphs=len(paragraph_sizes),
    )


def build_plucker_composite_image_section(
    *,
    uid: int,
    columns: int,
    rows: int,
    image_uids: Sequence[int],
) -> bytes:
    """
    Build plucker composite image section for deterministic fixture consumers.

    Example:
        Exercise build plucker composite image section through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_source.py


    :param uid: Value supplied for uid under the deterministic fixture contract.
    :param columns: Value supplied for columns under the deterministic fixture contract.
    :param rows: Value supplied for rows under the deterministic fixture contract.
    :param image_uids: Value supplied for image uids under the deterministic fixture
        contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    payload = pack(">HH", columns, rows) + b"".join(pack(">H", image_uid) for image_uid in image_uids)
    from LiuXin_alpha.file_formats.pdb.plucker.reader import DATATYPE_COMPOSITE_IMAGE

    return build_plucker_section(uid=uid, datatype=DATATYPE_COMPOSITE_IMAGE, payload=payload)


def build_plucker_metadata_section(
    *,
    title: str,
    author: str,
    pubdate: int = 0,
    encoding_mib: int = 106,
) -> bytes:
    """
    Build plucker metadata section for deterministic fixture consumers.

    Example:
        Exercise build plucker metadata section through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_pdb_metadata_source.py


    :param title: Value supplied for title under the deterministic fixture contract.
    :param author: Value supplied for author under the deterministic fixture contract.
    :param pubdate: Value supplied for pubdate under the deterministic fixture contract.
    :param encoding_mib: Value supplied for encoding mib under the deterministic fixture
        contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    from LiuXin_alpha.file_formats.pdb.plucker.reader import DATATYPE_METADATA

    records = [
        build_plucker_record(1, pack(">H", encoding_mib)),
        build_plucker_record(4, author.encode("utf-8") + b"\0"),
        build_plucker_record(5, title.encode("utf-8") + b"\0"),
    ]
    if pubdate:
        records.append(build_plucker_record(6, pack(">I", pubdate)))
    payload = pack(">H", len(records)) + b"".join(records)
    section_header = pack(">HHHBB", 1, 0, len(payload), DATATYPE_METADATA, 0)
    return section_header + payload
