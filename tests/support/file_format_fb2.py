"""
Build deterministic FB2 fixtures and test doubles.

The module keeps generated data, ordering and failure modes explicit so consumers
can assert stable behavior.

Example:
    Exercise file format fb2 through a consuming regression::

        python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py
"""
from __future__ import annotations

import base64
import binascii
import struct
import textwrap
import zipfile
import zlib
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Sequence
from xml.etree import ElementTree as ET
from xml.sax.saxutils import escape

from tests.support.file_format_unicode import COMMON_TEXT_FRAGMENTS, MULTISCRIPT_TEXT
from tests.support.file_format_zip import (
    read_zip_member,
    rewrite_zip_archive,
    write_zip_archive,
    zip_archive_bytes,
    zip_member_names,
)


FB2_NS = "http://www.gribuser.ru/xml/fictionbook/2.0"
XLINK_NS = "http://www.w3.org/1999/xlink"

FB2_TITLE = "FB2 Καλημέρα 世界"
FB2_AUTHORS = (
    ("José", "María", "Niño"),
    ("Иван", "", "Петров"),
)
FB2_DESCRIPTION = "FB2 description: مرحبا שלום नमस्ते 你好 cafe\u0301"
FB2_KEYWORDS = "fictionbook, unicode, Κατηγορία, タグ"
FB2_PUBLISHER = "Éditions Δ"
FB2_COVER_ID = "cover_世界"
FB2_EXTRA_BINARY_ID = "illustration_cafe\u0301"
FB2_ZIP_MEMBER = "fictionbook/Καλημέρα_世界/book.fb2"


@dataclass(frozen=True)
class FB2Fixture:
    """
    Carry the deterministic FB2Fixture inputs and expected values used by format tests.

    Example:
        Exercise FB2Fixture through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py
    """
    path: Path
    encoding: str
    binary_ids: tuple[str, ...]
    cover_id: str | None
    text_fragments: tuple[str, ...]


@dataclass(frozen=True)
class FB2ZipFixture:
    """
    Carry the deterministic FB2ZipFixture inputs and expected values used by format tests.

    Example:
        Exercise FB2ZipFixture through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py
    """
    path: Path
    fb2_member: str
    encoding: str
    binary_ids: tuple[str, ...]
    cover_id: str | None
    extra_members: tuple[str, ...]
    text_fragments: tuple[str, ...]


class NullLog:
    """
    Record or discard NullLog messages without requiring the production logging stack.

    Example:
        Exercise NullLog through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py
    """
    def __init__(self) -> None:
        """
        Initialize and validate the NullLog test-support state.

        Example:
            Exercise NullLog.  init   through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


        :return: None; completion is expressed through state changes or assertions.
        """
        self.messages: list[str] = []

    def __call__(self, message: str = "", *args) -> None:
        """
        Execute the configured fixture builder or test double operation.

        Example:
            Exercise NullLog.  call   through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


        :param message: Diagnostic message recorded or discarded by the test logger.
        :param args: Positional arguments forwarded to the bounded test double.
        :return: None; fixture state or the supplied destination is updated in place.
        """
        self.messages.append(message % args if args else message)

    def debug(self, message: str = "", *args) -> None:
        """
        Record or discard a debug message for assertions without external logging.

        Example:
            Exercise NullLog.debug through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


        :param message: Diagnostic message recorded or discarded by the test logger.
        :param args: Positional arguments forwarded to the bounded test double.
        :return: None; fixture state or the supplied destination is updated in place.
        """
        self(message, *args)

    def info(self, message: str = "", *args) -> None:
        """
        Record or discard a info message for assertions without external logging.

        Example:
            Exercise NullLog.info through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


        :param message: Diagnostic message recorded or discarded by the test logger.
        :param args: Positional arguments forwarded to the bounded test double.
        :return: None; fixture state or the supplied destination is updated in place.
        """
        self(message, *args)

    def warning(self, message: str = "", *args) -> None:
        """
        Record or discard a warning message for assertions without external logging.

        Example:
            Exercise NullLog.warning through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


        :param message: Diagnostic message recorded or discarded by the test logger.
        :param args: Positional arguments forwarded to the bounded test double.
        :return: None; fixture state or the supplied destination is updated in place.
        """
        self(message, *args)

    warn = warning

    def error(self, message: str = "", *args) -> None:
        """
        Record or discard a error message for assertions without external logging.

        Example:
            Exercise NullLog.error through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


        :param message: Diagnostic message recorded or discarded by the test logger.
        :param args: Positional arguments forwarded to the bounded test double.
        :return: None; fixture state or the supplied destination is updated in place.
        """
        self(message, *args)

    def exception(self, message: str = "", *args) -> None:
        """
        Record or discard a exception message for assertions without external logging.

        Example:
            Exercise NullLog.exception through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


        :param message: Diagnostic message recorded or discarded by the test logger.
        :param args: Positional arguments forwarded to the bounded test double.
        :return: None; fixture state or the supplied destination is updated in place.
        """
        self(message, *args)


def png_bytes(width: int = 16, height: int = 16, rgb: tuple[int, int, int] = (95, 120, 175)) -> bytes:
    """
    Return deterministic PNG bytes for the requested dimensions and colour.

    Example:
        Exercise png bytes through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


    :param width: Image width in pixels.
    :param height: Image height in pixels.
    :param rgb: RGB colour embedded in the generated image.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    signature = b"\x89PNG\r\n\x1a\n"

    def chunk(tag: bytes, payload: bytes) -> bytes:
        """
        Return the encoded binary chunk required by the fixture container.

        Example:
            Exercise png bytes.chunk through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


        :param tag: Value supplied for tag under the deterministic fixture contract.
        :param payload: Binary or structured payload encoded into the fixture.
        :return: The deterministic fixture value, path, bytes, record or collection
            described above.
        """
        return (
            struct.pack(">I", len(payload))
            + tag
            + payload
            + struct.pack(">I", binascii.crc32(tag + payload) & 0xFFFFFFFF)
        )

    ihdr = struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0)
    row = bytes([0]) + bytes(rgb) * width
    raw = row * height
    return signature + chunk(b"IHDR", ihdr) + chunk(b"IDAT", zlib.compress(raw, 9)) + chunk(b"IEND", b"")


def _xml_text(text: str) -> str:
    """
    Perform the xml text step with deterministic fixture inputs.

    Example:
        Exercise  xml text through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


    :param text: Text encoded, parsed or embedded in the fixture.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    return escape(text, {'"': "&quot;"})


def _author_markup(author: tuple[str, str, str]) -> str:
    """
    Perform the author markup step with deterministic fixture inputs.

    Example:
        Exercise  author markup through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


    :param author: Value supplied for author under the deterministic fixture contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    first, middle, last = author
    middle_markup = f"<middle-name>{_xml_text(middle)}</middle-name>" if middle else ""
    return (
        "<author>"
        f"<first-name>{_xml_text(first)}</first-name>"
        f"{middle_markup}"
        f"<last-name>{_xml_text(last)}</last-name>"
        "</author>"
    )


def _paragraph_markup(lines: Sequence[str]) -> str:
    """
    Perform the paragraph markup step with deterministic fixture inputs.

    Example:
        Exercise  paragraph markup through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


    :param lines: Value supplied for lines under the deterministic fixture contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    return "\n".join(f"<p>{_xml_text(line)}</p>" for line in lines)


def _binary_markup(binary_id: str, content_type: str, payload: bytes) -> str:
    """
    Perform the binary markup step with deterministic fixture inputs.

    Example:
        Exercise  binary markup through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


    :param binary_id: Value supplied for binary id under the deterministic fixture
        contract.
    :param content_type: Value supplied for content type under the deterministic fixture
        contract.
    :param payload: Binary or structured payload encoded into the fixture.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    encoded = base64.b64encode(payload).decode("ascii")
    wrapped = "\n".join(textwrap.wrap(encoded, width=76))
    return (
        f'<binary id="{_xml_text(binary_id)}" '
        f'content-type="{_xml_text(content_type)}">\n{wrapped}\n</binary>'
    )


def fb2_bytes(
    *,
    lines: Sequence[str] | None = None,
    encoding: str = "utf-8",
    title: str = FB2_TITLE,
    authors: Sequence[tuple[str, str, str]] = FB2_AUTHORS,
    include_cover: bool = True,
    cover_id: str = FB2_COVER_ID,
    cover_data: bytes | None = None,
    extra_binaries: Mapping[str, tuple[str, bytes]] | None = None,
) -> bytes:
    """
    Perform the fb2 bytes step with deterministic fixture inputs.

    Example:
        Exercise fb2 bytes through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


    :param lines: Value supplied for lines under the deterministic fixture contract.
    :param encoding: Character encoding used for deterministic fixture bytes.
    :param title: Value supplied for title under the deterministic fixture contract.
    :param authors: Value supplied for authors under the deterministic fixture contract.
    :param include_cover: Value supplied for include cover under the deterministic
        fixture contract.
    :param cover_id: Value supplied for cover id under the deterministic fixture
        contract.
    :param cover_data: Value supplied for cover data under the deterministic fixture
        contract.
    :param extra_binaries: Value supplied for extra binaries under the deterministic
        fixture contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    body_lines = tuple(lines or MULTISCRIPT_TEXT.splitlines())
    extra_binaries = dict(extra_binaries or {})
    cover_payload = png_bytes() if cover_data is None else cover_data

    binary_entries: list[tuple[str, str, bytes]] = []
    if include_cover:
        binary_entries.append((cover_id, "image/png", cover_payload))
    binary_entries.extend(
        (binary_id, content_type, payload)
        for binary_id, (content_type, payload) in extra_binaries.items()
    )

    cover_markup = (
        f'<coverpage><image l:href="#{_xml_text(cover_id)}"/></coverpage>'
        if include_cover
        else ""
    )
    section_image = (
        f'<image l:href="#{_xml_text(cover_id)}"/>'
        if include_cover
        else ""
    )
    authors_markup = "\n      ".join(_author_markup(author) for author in authors)
    body_markup = _paragraph_markup(body_lines)
    binary_markup = "\n".join(
        _binary_markup(binary_id, content_type, payload)
        for binary_id, content_type, payload in binary_entries
    )

    xml = f"""<?xml version="1.0" encoding="{_xml_text(encoding)}"?>
<FictionBook xmlns="{FB2_NS}" xmlns:l="{XLINK_NS}">
  <stylesheet type="text/css">
    body {{ font-family: serif; }}
    section.title {{ color: #24476b; }}
  </stylesheet>
  <description>
    <title-info>
      <genre>sf</genre>
      {authors_markup}
      <book-title>{_xml_text(title)}</book-title>
      <annotation><p>{_xml_text(FB2_DESCRIPTION)}</p></annotation>
      <keywords>{_xml_text(FB2_KEYWORDS)}</keywords>
      <date value="2026-05-21">2026</date>
      {cover_markup}
      <lang>en</lang>
    </title-info>
    <document-info>
      <author><first-name>Fixture</first-name><last-name>Builder</last-name></author>
      <program-used>LiuXin test fixture</program-used>
      <date value="2026-05-21">2026-05-21</date>
      <id>urn:uuid:22222222-3333-4444-5555-666666666666</id>
      <version>1.0</version>
    </document-info>
    <publish-info>
      <book-name>{_xml_text(title)}</book-name>
      <publisher>{_xml_text(FB2_PUBLISHER)}</publisher>
      <city>Montréal</city>
      <year>2026</year>
      <isbn>978-1-4028-9462-6</isbn>
    </publish-info>
  </description>
  <body>
    <title><p>{_xml_text(title)}</p></title>
    <section id="intro">
      <title><p>Intro Καλημέρα</p></title>
      {body_markup}
      {section_image}
    </section>
    <section id="notes">
      <title><p>Notes 世界</p></title>
      <p>Fixture tail text keeps nested sections visible.</p>
    </section>
  </body>
  {binary_markup}
</FictionBook>
"""
    return xml.encode(encoding)


def fb2_zip_bytes(
    *,
    member_name: str = FB2_ZIP_MEMBER,
    lines: Sequence[str] | None = None,
    encoding: str = "utf-8",
    title: str = FB2_TITLE,
    authors: Sequence[tuple[str, str, str]] = FB2_AUTHORS,
    include_cover: bool = True,
    cover_id: str = FB2_COVER_ID,
    cover_data: bytes | None = None,
    extra_binaries: Mapping[str, tuple[str, bytes]] | None = None,
    extra_members: Mapping[str, bytes] | None = None,
    compression: int = zipfile.ZIP_DEFLATED,
) -> bytes:
    """
    Perform the fb2 zip bytes step with deterministic fixture inputs.

    Example:
        Exercise fb2 zip bytes through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


    :param member_name: Value supplied for member name under the deterministic fixture
        contract.
    :param lines: Value supplied for lines under the deterministic fixture contract.
    :param encoding: Character encoding used for deterministic fixture bytes.
    :param title: Value supplied for title under the deterministic fixture contract.
    :param authors: Value supplied for authors under the deterministic fixture contract.
    :param include_cover: Value supplied for include cover under the deterministic
        fixture contract.
    :param cover_id: Value supplied for cover id under the deterministic fixture
        contract.
    :param cover_data: Value supplied for cover data under the deterministic fixture
        contract.
    :param extra_binaries: Value supplied for extra binaries under the deterministic
        fixture contract.
    :param extra_members: Value supplied for extra members under the deterministic
        fixture contract.
    :param compression: Value supplied for compression under the deterministic fixture
        contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    members = {
        member_name: fb2_bytes(
            lines=lines,
            encoding=encoding,
            title=title,
            authors=authors,
            include_cover=include_cover,
            cover_id=cover_id,
            cover_data=cover_data,
            extra_binaries=extra_binaries,
        ),
    }
    members.update(dict(extra_members or {}))
    return zip_archive_bytes(members, default_compression=compression)


def build_unicode_fb2(
    path: Path,
    *,
    lines: Sequence[str] | None = None,
    encoding: str = "utf-8",
    include_cover: bool = True,
    cover_id: str = FB2_COVER_ID,
    cover_data: bytes | None = None,
    extra_binaries: Mapping[str, tuple[str, bytes]] | None = None,
) -> FB2Fixture:
    """
    Build unicode fb2 for deterministic fixture consumers.

    Example:
        Exercise build unicode fb2 through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


    :param path: Filesystem path read, written or validated by the fixture operation.
    :param lines: Value supplied for lines under the deterministic fixture contract.
    :param encoding: Character encoding used for deterministic fixture bytes.
    :param include_cover: Value supplied for include cover under the deterministic
        fixture contract.
    :param cover_id: Value supplied for cover id under the deterministic fixture
        contract.
    :param cover_data: Value supplied for cover data under the deterministic fixture
        contract.
    :param extra_binaries: Value supplied for extra binaries under the deterministic
        fixture contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    extra_binaries = dict(extra_binaries or {})
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(
        fb2_bytes(
            lines=lines,
            encoding=encoding,
            include_cover=include_cover,
            cover_id=cover_id,
            cover_data=cover_data,
            extra_binaries=extra_binaries,
        )
    )
    binary_ids = ((cover_id,) if include_cover else ()) + tuple(extra_binaries)
    return FB2Fixture(
        path=path,
        encoding=encoding,
        binary_ids=binary_ids,
        cover_id=cover_id if include_cover else None,
        text_fragments=tuple(COMMON_TEXT_FRAGMENTS),
    )


def build_zipped_fb2(
    path: Path,
    *,
    member_name: str = FB2_ZIP_MEMBER,
    lines: Sequence[str] | None = None,
    encoding: str = "utf-8",
    include_cover: bool = True,
    cover_id: str = FB2_COVER_ID,
    cover_data: bytes | None = None,
    extra_binaries: Mapping[str, tuple[str, bytes]] | None = None,
    extra_members: Mapping[str, bytes] | None = None,
    compression: int = zipfile.ZIP_DEFLATED,
) -> FB2ZipFixture:
    """
    Build zipped fb2 for deterministic fixture consumers.

    Example:
        Exercise build zipped fb2 through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


    :param path: Filesystem path read, written or validated by the fixture operation.
    :param member_name: Value supplied for member name under the deterministic fixture
        contract.
    :param lines: Value supplied for lines under the deterministic fixture contract.
    :param encoding: Character encoding used for deterministic fixture bytes.
    :param include_cover: Value supplied for include cover under the deterministic
        fixture contract.
    :param cover_id: Value supplied for cover id under the deterministic fixture
        contract.
    :param cover_data: Value supplied for cover data under the deterministic fixture
        contract.
    :param extra_binaries: Value supplied for extra binaries under the deterministic
        fixture contract.
    :param extra_members: Value supplied for extra members under the deterministic
        fixture contract.
    :param compression: Value supplied for compression under the deterministic fixture
        contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    extra_binaries = dict(extra_binaries or {})
    extra_members = dict(extra_members or {})
    members = {
        member_name: fb2_bytes(
            lines=lines,
            encoding=encoding,
            include_cover=include_cover,
            cover_id=cover_id,
            cover_data=cover_data,
            extra_binaries=extra_binaries,
        ),
    }
    members.update(extra_members)
    write_zip_archive(path, members, default_compression=compression)
    binary_ids = ((cover_id,) if include_cover else ()) + tuple(extra_binaries)
    return FB2ZipFixture(
        path=path,
        fb2_member=member_name,
        encoding=encoding,
        binary_ids=binary_ids,
        cover_id=cover_id if include_cover else None,
        extra_members=tuple(extra_members),
        text_fragments=tuple(COMMON_TEXT_FRAGMENTS),
    )


def zipped_fb2_members(path: Path) -> tuple[str, ...]:
    """
    Perform the zipped fb2 members step with deterministic fixture inputs.

    Example:
        Exercise zipped fb2 members through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


    :param path: Filesystem path read, written or validated by the fixture operation.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    return zip_member_names(path)


def read_zipped_fb2_member(path: Path, member: str) -> bytes:
    """
    Read zipped fb2 member under the fixture contract.

    Example:
        Exercise read zipped fb2 member through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


    :param path: Filesystem path read, written or validated by the fixture operation.
    :param member: Archive or container member addressed by the operation.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    return read_zip_member(path, member)


def rewrite_zipped_fb2(
    src: Path,
    dst: Path,
    *,
    remove: Sequence[str] = (),
    replace: Mapping[str, bytes] | None = None,
    add: Mapping[str, bytes] | None = None,
    add_compression: int = zipfile.ZIP_STORED,
) -> None:
    """
    Perform the rewrite zipped fb2 step with deterministic fixture inputs.

    Example:
        Exercise rewrite zipped fb2 through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


    :param src: Source path or value copied into the fixture.
    :param dst: Destination path or object receiving generated fixture data.
    :param remove: Value supplied for remove under the deterministic fixture contract.
    :param replace: Value supplied for replace under the deterministic fixture contract.
    :param add: Value supplied for add under the deterministic fixture contract.
    :param add_compression: Value supplied for add compression under the deterministic
        fixture contract.
    :return: None; fixture state or the supplied destination is updated in place.
    """
    rewrite_zip_archive(
        src,
        dst,
        remove=remove,
        replace=replace,
        add=add,
        add_compression=add_compression,
    )


def parse_fb2_bytes(payload: bytes) -> ET.Element:
    """
    Parse fb2 bytes under the fixture contract.

    Example:
        Exercise parse fb2 bytes through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


    :param payload: Binary or structured payload encoded into the fixture.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    return ET.fromstring(payload)


def parse_zipped_fb2(path: Path, member: str = FB2_ZIP_MEMBER) -> ET.Element:
    """
    Parse zipped fb2 under the fixture contract.

    Example:
        Exercise parse zipped fb2 through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


    :param path: Filesystem path read, written or validated by the fixture operation.
    :param member: Archive or container member addressed by the operation.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    return parse_fb2_bytes(read_zipped_fb2_member(path, member))


def parse_fb2(path: Path) -> ET.Element:
    """
    Parse fb2 under the fixture contract.

    Example:
        Exercise parse fb2 through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


    :param path: Filesystem path read, written or validated by the fixture operation.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    return parse_fb2_bytes(path.read_bytes())


def fb2_body_text(path: Path) -> str:
    """
    Perform the fb2 body text step with deterministic fixture inputs.

    Example:
        Exercise fb2 body text through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


    :param path: Filesystem path read, written or validated by the fixture operation.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    root = parse_fb2(path)
    body = root.find(f"{{{FB2_NS}}}body")
    if body is None:
        return ""
    return "\n".join(text for text in body.itertext() if text)


def read_fb2_binary(path: Path, binary_id: str) -> bytes:
    """
    Read fb2 binary under the fixture contract.

    Example:
        Exercise read fb2 binary through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


    :param path: Filesystem path read, written or validated by the fixture operation.
    :param binary_id: Value supplied for binary id under the deterministic fixture
        contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    root = parse_fb2(path)
    for elem in root.iter(f"{{{FB2_NS}}}binary"):
        if elem.attrib.get("id") != binary_id:
            continue
        encoded = "".join((elem.text or "").split())
        return base64.b64decode(encoded)
    raise KeyError(binary_id)


def rewrite_fb2_text(
    source: Path,
    target: Path,
    *,
    remove: Sequence[str] = (),
    replace: Mapping[str, str] | None = None,
    append: str = "",
    encoding: str = "utf-8",
) -> bytes:
    """
    Perform the rewrite fb2 text step with deterministic fixture inputs.

    Example:
        Exercise rewrite fb2 text through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_fb2_metadata_source.py


    :param source: Value supplied for source under the deterministic fixture contract.
    :param target: Value supplied for target under the deterministic fixture contract.
    :param remove: Value supplied for remove under the deterministic fixture contract.
    :param replace: Value supplied for replace under the deterministic fixture contract.
    :param append: Value supplied for append under the deterministic fixture contract.
    :param encoding: Character encoding used for deterministic fixture bytes.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    text = source.read_bytes().decode(encoding)
    for fragment in remove:
        text = text.replace(fragment, "")
    for old, new in dict(replace or {}).items():
        text = text.replace(old, new)
    if append:
        text += append
    payload = text.encode(encoding)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(payload)
    return payload
