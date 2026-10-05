"""
Build deterministic ODT fixtures and test doubles.

The module keeps generated data, ordering and failure modes explicit so consumers
can assert stable behavior.

Example:
    Exercise file format odt through a consuming regression::

        python -m pytest -q tests/metadata/file_sources/test_odt_metadata_source.py
"""
from __future__ import annotations

import binascii
import struct
import zipfile
import zlib
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Sequence

from tests.support.file_format_unicode import COMMON_TEXT_FRAGMENTS, MULTISCRIPT_TEXT


ODT_TITLE = "ODT Καλημέρα 世界"
ODT_AUTHORS = "José and Иван"
ODT_DESCRIPTION = "ODT container description: مرحبا שלום नमस्ते 你好 cafe\u0301"
ODT_IMAGE_BYTES = b"odt-nested-image-\xce\xa9-\xe4\xb8\x96\xe7\x95\x8c"


@dataclass(frozen=True)
class ODTFixture:
    """
    Carry the deterministic ODTFixture inputs and expected values used by format tests.

    Example:
        Exercise ODTFixture through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_odt_metadata_source.py
    """
    path: Path
    text_fragments: tuple[str, ...]
    picture_members: tuple[str, ...]


class NullLog:
    """
    Record or discard NullLog messages without requiring the production logging stack.

    Example:
        Exercise NullLog through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_odt_metadata_source.py
    """
    def __init__(self) -> None:
        """
        Initialize and validate the NullLog test-support state.

        Example:
            Exercise NullLog.  init   through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_odt_metadata_source.py


        :return: None; completion is expressed through state changes or assertions.
        """
        self.messages: list[str] = []

    def __call__(self, message: str = "", *args) -> None:
        """
        Execute the configured fixture builder or test double operation.

        Example:
            Exercise NullLog.  call   through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_odt_metadata_source.py


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

                python -m pytest -q tests/metadata/file_sources/test_odt_metadata_source.py


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

                python -m pytest -q tests/metadata/file_sources/test_odt_metadata_source.py


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

                python -m pytest -q tests/metadata/file_sources/test_odt_metadata_source.py


        :param message: Diagnostic message recorded or discarded by the test logger.
        :param args: Positional arguments forwarded to the bounded test double.
        :return: None; fixture state or the supplied destination is updated in place.
        """
        self(message, *args)

    warn = warning

    def exception(self, message: str = "", *args) -> None:
        """
        Record or discard a exception message for assertions without external logging.

        Example:
            Exercise NullLog.exception through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_odt_metadata_source.py


        :param message: Diagnostic message recorded or discarded by the test logger.
        :param args: Positional arguments forwarded to the bounded test double.
        :return: None; fixture state or the supplied destination is updated in place.
        """
        self(message, *args)


def png_bytes(width: int = 16, height: int = 16, rgb: tuple[int, int, int] = (90, 120, 180)) -> bytes:
    """
    Return deterministic PNG bytes for the requested dimensions and colour.

    Example:
        Exercise png bytes through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_odt_metadata_source.py


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

                python -m pytest -q tests/metadata/file_sources/test_odt_metadata_source.py


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


def build_unicode_odt(
    path: Path,
    *,
    lines: Sequence[str] | None = None,
    include_image: bool = False,
) -> ODTFixture:
    """
    Build unicode odt for deterministic fixture consumers.

    Example:
        Exercise build unicode odt through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_odt_metadata_source.py


    :param path: Filesystem path read, written or validated by the fixture operation.
    :param lines: Value supplied for lines under the deterministic fixture contract.
    :param include_image: Value supplied for include image under the deterministic
        fixture contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    from LiuXin_alpha.file_formats.odf.dc import Creator, Description, Language, Subject, Title
    from LiuXin_alpha.file_formats.odf.meta import Keyword, UserDefined
    from LiuXin_alpha.file_formats.odf.opendocument import OpenDocumentText
    from LiuXin_alpha.file_formats.odf.teletype import addTextToElement
    from LiuXin_alpha.file_formats.odf.text import P

    doc = OpenDocumentText()
    doc.meta.addElement(Title(text=ODT_TITLE))
    doc.meta.addElement(Creator(text=ODT_AUTHORS))
    doc.meta.addElement(Description(text=ODT_DESCRIPTION))
    doc.meta.addElement(Subject(text="containers, unicode"))
    doc.meta.addElement(Keyword(text="Κατηγορία;タグ"))
    doc.meta.addElement(Language(text="en"))
    doc.meta.addElement(UserDefined(name="opf.publisher", valuetype="string", text="Éditions Δ"))

    body_lines = tuple(lines or MULTISCRIPT_TEXT.splitlines())
    for line in body_lines:
        para = P()
        addTextToElement(para, line)
        doc.text.addElement(para)

    if include_image:
        para = P()
        addTextToElement(para, "Archive image holder 画像")
        doc.addPictureFromString(png_bytes(), "image/png")
        doc.text.addElement(para)

    doc.save(path)
    picture_members = tuple(name for name in zip_members(path) if name.startswith("Pictures/") and not name.endswith("/"))
    return ODTFixture(path=path, text_fragments=tuple(COMMON_TEXT_FRAGMENTS), picture_members=picture_members)


def zip_members(path: Path) -> tuple[str, ...]:
    """
    Return the normalized members stored in the generated archive fixture.

    Example:
        Exercise zip members through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_odt_metadata_source.py


    :param path: Filesystem path read, written or validated by the fixture operation.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    with zipfile.ZipFile(path, "r") as zf:
        return tuple(info.filename for info in zf.infolist())


def rewrite_odt_zip(
    src: Path,
    dst: Path,
    *,
    remove: Sequence[str] = (),
    replace: Mapping[str, bytes] | None = None,
    add: Mapping[str, bytes] | None = None,
    add_compression: int = zipfile.ZIP_STORED,
) -> None:
    """
    Perform the rewrite odt zip step with deterministic fixture inputs.

    Example:
        Exercise rewrite odt zip through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_odt_metadata_source.py


    :param src: Source path or value copied into the fixture.
    :param dst: Destination path or object receiving generated fixture data.
    :param remove: Value supplied for remove under the deterministic fixture contract.
    :param replace: Value supplied for replace under the deterministic fixture contract.
    :param add: Value supplied for add under the deterministic fixture contract.
    :param add_compression: Value supplied for add compression under the deterministic
        fixture contract.
    :return: None; fixture state or the supplied destination is updated in place.
    """
    replacements = dict(replace or {})
    additions = dict(add or {})
    removed = set(remove)
    with zipfile.ZipFile(src, "r") as zin, zipfile.ZipFile(dst, "w") as zout:
        for info in zin.infolist():
            if info.filename in removed:
                continue
            data = replacements.pop(info.filename, zin.read(info.filename))
            zout.writestr(info, data)
        for name, data in {**replacements, **additions}.items():
            info = zipfile.ZipInfo(name)
            info.compress_type = add_compression
            zout.writestr(info, data)
