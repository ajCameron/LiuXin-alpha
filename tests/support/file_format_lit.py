"""
Build deterministic LIT fixtures and test doubles.

The module keeps generated data, ordering and failure modes explicit so consumers
can assert stable behavior.

Example:
    Exercise file format lit through a consuming regression::

        python -m pytest -q tests/metadata/file_sources/test_lit_metadata_source.py
"""
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
    """
    Record or discard LitLog messages without requiring the production logging stack.

    Example:
        Exercise LitLog through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_lit_metadata_source.py
    """
    def __init__(self) -> None:
        """
        Initialize and validate the LitLog test-support state.

        Example:
            Exercise LitLog.  init   through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_lit_metadata_source.py


        :return: None; completion is expressed through state changes or assertions.
        """
        self.messages: list[str] = []

    def _record(self, *parts) -> None:
        """
        Perform the record step with deterministic fixture inputs.

        Example:
            Exercise LitLog. record through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_lit_metadata_source.py


        :param parts: Message fragments joined for the test logger.
        :return: None; fixture state or the supplied destination is updated in place.
        """
        self.messages.append(" ".join(str(x) for x in parts))

    def __call__(self, *parts) -> None:
        """
        Execute the configured fixture builder or test double operation.

        Example:
            Exercise LitLog.  call   through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_lit_metadata_source.py


        :param parts: Message fragments joined for the test logger.
        :return: None; fixture state or the supplied destination is updated in place.
        """
        self._record(*parts)

    def debug(self, *parts) -> None:
        """
        Record or discard a debug message for assertions without external logging.

        Example:
            Exercise LitLog.debug through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_lit_metadata_source.py


        :param parts: Message fragments joined for the test logger.
        :return: None; fixture state or the supplied destination is updated in place.
        """
        self._record(*parts)

    def info(self, *parts) -> None:
        """
        Record or discard a info message for assertions without external logging.

        Example:
            Exercise LitLog.info through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_lit_metadata_source.py


        :param parts: Message fragments joined for the test logger.
        :return: None; fixture state or the supplied destination is updated in place.
        """
        self._record(*parts)

    def warn(self, *parts) -> None:
        """
        Record or discard a warn message for assertions without external logging.

        Example:
            Exercise LitLog.warn through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_lit_metadata_source.py


        :param parts: Message fragments joined for the test logger.
        :return: None; fixture state or the supplied destination is updated in place.
        """
        self._record(*parts)

    def warning(self, *parts) -> None:
        """
        Record or discard a warning message for assertions without external logging.

        Example:
            Exercise LitLog.warning through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_lit_metadata_source.py


        :param parts: Message fragments joined for the test logger.
        :return: None; fixture state or the supplied destination is updated in place.
        """
        self._record(*parts)

    def error(self, *parts) -> None:
        """
        Record or discard a error message for assertions without external logging.

        Example:
            Exercise LitLog.error through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_lit_metadata_source.py


        :param parts: Message fragments joined for the test logger.
        :return: None; fixture state or the supplied destination is updated in place.
        """
        self._record(*parts)

    def exception(self, *parts) -> None:
        """
        Record or discard a exception message for assertions without external logging.

        Example:
            Exercise LitLog.exception through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_lit_metadata_source.py


        :param parts: Message fragments joined for the test logger.
        :return: None; fixture state or the supplied destination is updated in place.
        """
        self._record(*parts)


@dataclass(frozen=True)
class LitManifestRecord:
    """
    Carry the deterministic LitManifestRecord inputs and expected values used by format tests.

    Example:
        Exercise LitManifestRecord through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_lit_metadata_source.py
    """
    internal: str
    original: str
    mime_type: str
    offset: int = 0
    state: str = "spine"


def lit_options(**overrides):
    """
    Perform the lit options step with deterministic fixture inputs.

    Example:
        Exercise lit options through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_lit_metadata_source.py


    :param overrides: Value supplied for overrides under the deterministic fixture
        contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    values = {"pretty_print": False}
    values.update(overrides)
    return SimpleNamespace(**values)


def lit_stream(payload: bytes, *, name: str = "fixture.lit") -> io.BytesIO:
    """
    Perform the lit stream step with deterministic fixture inputs.

    Example:
        Exercise lit stream through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_lit_metadata_source.py


    :param payload: Binary or structured payload encoded into the fixture.
    :param name: Stable fixture, profile, member or field name.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
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
    """
    Build lit header payload for deterministic fixture consumers.

    Example:
        Exercise build lit header payload through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_lit_metadata_source.py


    :param magic: Value supplied for magic under the deterministic fixture contract.
    :param version: Value supplied for version under the deterministic fixture contract.
    :param hdr_len: Value supplied for hdr len under the deterministic fixture contract.
    :param num_pieces: Value supplied for num pieces under the deterministic fixture
        contract.
    :param sec_hdr_len: Value supplied for sec hdr len under the deterministic fixture
        contract.
    :param guid: Value supplied for guid under the deterministic fixture contract.
    :param secondary_header: Value supplied for secondary header under the deterministic
        fixture contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
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
    """
    Build lit secondary header for deterministic fixture consumers.

    Example:
        Exercise build lit secondary header through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_lit_metadata_source.py


    :param blocks: Value supplied for blocks under the deterministic fixture contract.
    :param start_offset: Value supplied for start offset under the deterministic fixture
        contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    return b"\0\0\0\0" + pack("<l", start_offset) + b"".join(blocks)


def lit_sized_utf8(value: str, *, zpad: bool = False) -> bytes:
    """
    Perform the lit sized utf8 step with deterministic fixture inputs.

    Example:
        Exercise lit sized utf8 through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_lit_metadata_source.py


    :param value: Fixture value normalized, encoded, stored or returned.
    :param zpad: Value supplied for zpad under the deterministic fixture contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
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
    """
    Build lit manifest payload for deterministic fixture consumers.

    Example:
        Exercise build lit manifest payload through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_lit_metadata_source.py


    :param records: Value supplied for records under the deterministic fixture contract.
    :param root: Root directory containing the fixture corpus or generated tree.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
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
    """
    Build lit namelist payload for deterministic fixture consumers.

    Example:
        Exercise build lit namelist payload through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_lit_metadata_source.py


    :param section_names: Value supplied for section names under the deterministic
        fixture contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
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
    """
    Perform the in memory lit file step with deterministic fixture inputs.

    Example:
        Exercise in memory lit file through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_lit_metadata_source.py


    :param files: Files included in the generated fixture or assertion.
    :param opf_path: Value supplied for opf path under the deterministic fixture
        contract.
    :param log: Value supplied for log under the deterministic fixture contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
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
        """
        Return file under the fixture contract.

        Example:
            Exercise in memory lit file. get file through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_lit_metadata_source.py


        :param self: Value supplied for self under the deterministic fixture contract.
        :param name: Stable fixture, profile, member or field name.
        :return: The deterministic fixture value, path, bytes, record or collection
            described above.
        """
        return payloads[name]

    lit.get_file = MethodType(_get_file, lit)
    return lit


def read_manifest_from_payload(
    payload: bytes,
    *,
    opf_path: str = "content.opf",
    log: LitLog | None = None,
):
    """
    Read manifest from payload under the fixture contract.

    Example:
        Exercise read manifest from payload through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_lit_metadata_source.py


    :param payload: Binary or structured payload encoded into the fixture.
    :param opf_path: Value supplied for opf path under the deterministic fixture
        contract.
    :param log: Value supplied for log under the deterministic fixture contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    lit = in_memory_lit_file({"/manifest": payload}, opf_path=opf_path, log=log)
    LitFile.read_manifest(lit)
    return lit


def read_namelist_from_payload(payload: bytes, *, log: LitLog | None = None):
    """
    Read namelist from payload under the fixture contract.

    Example:
        Exercise read namelist from payload through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_lit_metadata_source.py


    :param payload: Binary or structured payload encoded into the fixture.
    :param log: Value supplied for log under the deterministic fixture contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
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
    """
    Perform the lit manifest item step with deterministic fixture inputs.

    Example:
        Exercise lit manifest item through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_lit_metadata_source.py


    :param original: Value supplied for original under the deterministic fixture
        contract.
    :param internal: Value supplied for internal under the deterministic fixture
        contract.
    :param mime_type: Value supplied for mime type under the deterministic fixture
        contract.
    :param offset: Value supplied for offset under the deterministic fixture contract.
    :param root: Root directory containing the fixture corpus or generated tree.
    :param state: Value supplied for state under the deterministic fixture contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    return ManifestItem(original, internal, mime_type, offset, root, state)


def lit_html_tag_id(tag_name: str) -> int:
    """
    Perform the lit html tag id step with deterministic fixture inputs.

    Example:
        Exercise lit html tag id through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_lit_metadata_source.py


    :param tag_name: Value supplied for tag name under the deterministic fixture
        contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    tags = HTML_MAP[0]
    for index, tag in enumerate(tags):
        if tag == tag_name:
            return index
    raise KeyError(tag_name)


def lit_html_attr_id(tag_name: str, attr_name: str) -> int:
    """
    Perform the lit html attr id step with deterministic fixture inputs.

    Example:
        Exercise lit html attr id through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_lit_metadata_source.py


    :param tag_name: Value supplied for tag name under the deterministic fixture
        contract.
    :param attr_name: Value supplied for attr name under the deterministic fixture
        contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    tag_id = lit_html_tag_id(tag_name)
    local_attrs = HTML_MAP[2][tag_id] or {}
    for attrs in (local_attrs, HTML_MAP[1]):
        for attr_id, attr in attrs.items():
            if attr == attr_name:
                return attr_id
    raise KeyError(f"{tag_name}@{attr_name}")


def lit_internal_href(target: str) -> str:
    """
    Perform the lit internal href step with deterministic fixture inputs.

    Example:
        Exercise lit internal href through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_lit_metadata_source.py


    :param target: Value supplied for target under the deterministic fixture contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    return "\x02" + target


def lit_external_href(target: str) -> str:
    """
    Perform the lit external href step with deterministic fixture inputs.

    Example:
        Exercise lit external href through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_lit_metadata_source.py


    :param target: Value supplied for target under the deterministic fixture contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    return "\x03" + target


def _codepoint(value: int) -> bytes:
    """
    Perform the codepoint step with deterministic fixture inputs.

    Example:
        Exercise  codepoint through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_lit_metadata_source.py


    :param value: Fixture value normalized, encoded, stored or returned.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    return chr(value).encode("utf-8")


def _text_payload(value: str | bytes) -> bytes:
    """
    Perform the text payload step with deterministic fixture inputs.

    Example:
        Exercise  text payload through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_lit_metadata_source.py


    :param value: Fixture value normalized, encoded, stored or returned.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
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
    """
    Perform the lit binary element step with deterministic fixture inputs.

    Example:
        Exercise lit binary element through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_lit_metadata_source.py


    :param tag_name: Value supplied for tag name under the deterministic fixture
        contract.
    :param text: Text encoded, parsed or embedded in the fixture.
    :param attrs: Value supplied for attrs under the deterministic fixture contract.
    :param children: Value supplied for children under the deterministic fixture
        contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
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
    """
    Perform the render unbinary html step with deterministic fixture inputs.

    Example:
        Exercise render unbinary html through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_lit_metadata_source.py


    :param payload: Binary or structured payload encoded into the fixture.
    :param path: Filesystem path read, written or validated by the fixture operation.
    :param manifest: Value supplied for manifest under the deterministic fixture
        contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    unbin = UnBinary(payload, path, dict(manifest or {}))
    return unbin.unicode_representation
