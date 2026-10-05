# -*- coding: utf-8 -*-

"""
Serialize normalized content and metadata into the target format.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise writer through a consuming regression::

        python -m pytest -q tests/file_formats/rb/test_rb_modernized.py
"""
from __future__ import annotations

import typing as _typing

import io
import struct
import zlib
from collections.abc import Iterable
from typing import BinaryIO, Protocol, cast

from LiuXin_alpha.constants import __appname__, __version__
from LiuXin_alpha.file_formats.rb import HEADER, unique_name
from LiuXin_alpha.file_formats.rb.rbml import RBMLizer
from LiuXin_alpha.metadata.utils import authors_to_string

try:
    from PIL import Image as _PILImage  # pyright: ignore[reportMissingImports]
except Exception:  # pragma: no cover - optional dependency
    _PILImage = None

__license__ = "GPL 3"
__copyright__ = "2009, John Schember <john@nachtimwald.com>"
__docformat__ = "restructuredtext en"


TEXT_RECORD_SIZE = 4096


class _Logger(Protocol):
    """
    Provide the logger contract for validated ebook processing.

    Example:
        Exercise  Logger through a consuming regression::

            python -m pytest -q tests/file_formats/rb/test_rb_modernized.py
    """
    def debug(self: _typing.Self, message: object) -> object:
        """
        Perform the debug operation under explicit file-format and conversion rules.

        Example:
            Exercise  Logger.debug through a consuming regression::

                python -m pytest -q tests/file_formats/rb/test_rb_modernized.py


        :param message: Value supplied for message under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    def error(self: _typing.Self, message: object) -> object:
        """
        Perform the error operation under explicit file-format and conversion rules.

        Example:
            Exercise  Logger.error through a consuming regression::

                python -m pytest -q tests/file_formats/rb/test_rb_modernized.py


        :param message: Value supplied for message under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    def info(self: _typing.Self, message: object) -> object:
        """
        Perform the info operation under explicit file-format and conversion rules.

        Example:
            Exercise  Logger.info through a consuming regression::

                python -m pytest -q tests/file_formats/rb/test_rb_modernized.py


        :param message: Value supplied for message under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    def warn(self: _typing.Self, message: object) -> object:
        """
        Perform the warn operation under explicit file-format and conversion rules.

        Example:
            Exercise  Logger.warn through a consuming regression::

                python -m pytest -q tests/file_formats/rb/test_rb_modernized.py


        :param message: Value supplied for message under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    def warning(self: _typing.Self, message: object) -> object:
        """
        Perform the warning operation under explicit file-format and conversion rules.

        Example:
            Exercise  Logger.warning through a consuming regression::

                python -m pytest -q tests/file_formats/rb/test_rb_modernized.py


        :param message: Value supplied for message under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...


class _ManifestItem(Protocol):
    """
    Provide the manifestitem contract for validated ebook processing.

    Example:
        Exercise  ManifestItem through a consuming regression::

            python -m pytest -q tests/file_formats/rb/test_rb_modernized.py
    """
    href: str
    media_type: str
    data: object


class _ReadablePayload(Protocol):
    """
    Provide the readablepayload contract for validated ebook processing.

    Example:
        Exercise  ReadablePayload through a consuming regression::

            python -m pytest -q tests/file_formats/rb/test_rb_modernized.py
    """
    def read(self: _typing.Self) -> bytes | str:
        """
        Perform the read operation under explicit file-format and conversion rules.

        Example:
            Exercise  ReadablePayload.read through a consuming regression::

                python -m pytest -q tests/file_formats/rb/test_rb_modernized.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    def seek(self: _typing.Self, offset: int) -> object:
        """
        Perform the seek operation under explicit file-format and conversion rules.

        Example:
            Exercise  ReadablePayload.seek through a consuming regression::

                python -m pytest -q tests/file_formats/rb/test_rb_modernized.py


        :param offset: Value supplied for offset under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    def tell(self: _typing.Self) -> int:
        """
        Perform the tell operation under explicit file-format and conversion rules.

        Example:
            Exercise  ReadablePayload.tell through a consuming regression::

                python -m pytest -q tests/file_formats/rb/test_rb_modernized.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...


class TocItem(object):
    """
    Provide the tocitem contract for validated ebook processing.

    Example:
        Exercise TocItem through a consuming regression::

            python -m pytest -q tests/file_formats/rb/test_rb_modernized.py
    """
    def __init__(self: _typing.Self, name: bytes, size: int, flags: int) -> None:
        """
        Initialize and validate the tocitem state.

        Example:
            Exercise TocItem.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/rb/test_rb_modernized.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param size: Value supplied for size under the utility contract.
        :param flags: Value supplied for flags under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.name = name
        self.size = size
        self.flags = flags


class RBWriter(object):
    """
    Provide the rbwriter contract for validated ebook processing.

    Example:
        Exercise RBWriter through a consuming regression::

            python -m pytest -q tests/file_formats/rb/test_rb_modernized.py
    """
    def __init__(
        self: _typing.Self,
        opts: _typing.Any,
        log: _Logger,
    ) -> None:
        """
        Initialize and validate the rbwriter state.

        Example:
            Exercise RBWriter.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/rb/test_rb_modernized.py


        :param opts: Value supplied for opts under the utility contract.
        :param log: Value supplied for log under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.opts = opts
        self.log = log
        self.name_map: dict[str, str] = {}

    def write_content(
        self: _typing.Self,
        oeb_book: _typing.Any,
        out_stream: BinaryIO,
        metadata: object | None = None,
    ) -> None:
        """
        Write content under the format's safety and compatibility rules.

        Example:
            Exercise RBWriter.write content through a consuming regression::

                python -m pytest -q tests/file_formats/rb/test_rb_modernized.py


        :param oeb_book: Value supplied for oeb book under the utility contract.
        :param out_stream: Value supplied for out stream under the utility contract.
        :param metadata: Value supplied for metadata under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        info_data = self._info_section(metadata)
        hidx_data = b" "
        images = self._images(oeb_book.manifest)
        text_size, chunks = self._text(oeb_book)
        chunk_sizes = [len(x) for x in chunks]

        sections = [
            ("info.info", info_data, "info"),
            ("index.html", chunks, "text"),
            ("index.hidx", hidx_data, "binary"),
        ]
        for image_name, image_data in images:
            sections.append((image_name, image_data, "binary"))

        toc_items = []
        page_count = 0
        text_blob_size = 8 + (len(chunk_sizes) * 4) + sum(chunk_sizes)
        for name, data, section_type in sections:
            page_count += 1
            if section_type == "text":
                flags = 8
                size = text_blob_size
            elif section_type == "info":
                flags = 2
                size = len(data)
            else:
                flags = 0
                size = len(data)
            toc_items.append(TocItem(self._toc_name(name), size, flags))

        self.log.debug("Writing file header...")
        out_stream.write(HEADER)
        out_stream.write(struct.pack("<I", 0))
        out_stream.write(struct.pack("<IH", 0, 0))
        out_stream.write(struct.pack("<I", 0x128))
        out_stream.write(struct.pack("<I", 0))

        for _ in range(0x20, 0x128, 4):
            out_stream.write(struct.pack("<I", 0))

        out_stream.write(struct.pack("<I", page_count))
        offset = out_stream.tell() + (len(toc_items) * 44)

        for item in toc_items:
            out_stream.write(item.name)
            out_stream.write(struct.pack("<I", item.size))
            out_stream.write(struct.pack("<I", offset))
            out_stream.write(struct.pack("<I", item.flags))
            offset += item.size

        out_stream.write(info_data)

        self.log.debug("Writing compressed RB HTML...")
        out_stream.write(struct.pack("<I", len(chunks)))
        out_stream.write(struct.pack("<I", text_size))
        for size in chunk_sizes:
            out_stream.write(struct.pack("<I", size))
        for chunk in chunks:
            out_stream.write(chunk)

        self.log.debug("Writing images...")
        out_stream.write(hidx_data)
        for _name, image_data in images:
            out_stream.write(image_data)

        total_size = out_stream.tell()
        out_stream.seek(0x1C)
        out_stream.write(struct.pack("<I", total_size))

    def _toc_name(self: _typing.Self, name: str) -> bytes:
        """
        Perform the toc name operation under explicit file-format and conversion rules.

        Example:
            Exercise RBWriter. toc name through a consuming regression::

                python -m pytest -q tests/file_formats/rb/test_rb_modernized.py


        :param name: Field, file, function or resource name addressed by the operation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return name.encode("utf-8", "replace")[:32].ljust(32, b"\x00")

    def _text(
        self: _typing.Self,
        oeb_book: _typing.Any,
    ) -> tuple[int, list[bytes]]:
        """
        Perform the text operation under explicit file-format and conversion rules.

        Example:
            Exercise RBWriter. text through a consuming regression::

                python -m pytest -q tests/file_formats/rb/test_rb_modernized.py


        :param oeb_book: Value supplied for oeb book under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        rbmlizer = RBMLizer(self.log, name_map=self.name_map)
        text = rbmlizer.extract_content(oeb_book, self.opts).encode("cp1252", "xmlcharrefreplace")
        size = len(text)

        pages = []
        page_count = (len(text) + TEXT_RECORD_SIZE - 1) // TEXT_RECORD_SIZE
        for i in range(0, page_count):
            zobj = zlib.compressobj(9, zlib.DEFLATED, 13, 8, 0)
            start = i * TEXT_RECORD_SIZE
            end = start + TEXT_RECORD_SIZE
            pages.append(zobj.compress(text[start:end]) + zobj.flush())

        return size, pages

    def _images(
        self: _typing.Self,
        manifest: Iterable[_ManifestItem],
    ) -> list[tuple[str, bytes]]:
        """
        Perform the images operation under explicit file-format and conversion rules.

        Example:
            Exercise RBWriter. images through a consuming regression::

                python -m pytest -q tests/file_formats/rb/test_rb_modernized.py


        :param manifest: Value supplied for manifest under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        from LiuXin_alpha.file_formats.oeb.base import OEB_RASTER_IMAGES

        if _PILImage is None:
            warn = getattr(self.log, "warning", None) or getattr(self.log, "warn", None)
            if warn is not None:
                warn("Pillow is not installed, RB output will skip embedded raster images.")
            return []

        images = []
        used_names = []

        for item in manifest:
            if item.media_type not in OEB_RASTER_IMAGES:
                continue
            try:
                payload = self._as_bytes(item.data)
                im = _PILImage.open(io.BytesIO(payload)).convert("L")
                out = io.BytesIO()
                im.save(out, "PNG")
                data = out.getvalue()

                name = unique_name("%s.png" % len(used_names), used_names)
                used_names.append(name)
                self.name_map[item.href] = name

                images.append((name, data))
            except Exception as err:
                self.log.error("Error: Could not include file %s because %s." % (item.href, err))

        return images

    def _as_bytes(self: _typing.Self, payload: object) -> bytes:
        """
        Perform the as bytes operation under explicit file-format and conversion rules.

        Example:
            Exercise RBWriter. as bytes through a consuming regression::

                python -m pytest -q tests/file_formats/rb/test_rb_modernized.py


        :param payload: Value supplied for payload under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if isinstance(payload, bytes):
            return payload
        if isinstance(payload, bytearray):
            return bytes(payload)
        if isinstance(payload, str):
            return payload.encode("utf-8", "replace")
        if hasattr(payload, "read"):
            readable = cast(_ReadablePayload, payload)
            current_pos = None
            if hasattr(readable, "tell"):
                try:
                    current_pos = readable.tell()
                except Exception:
                    current_pos = None
            try:
                if hasattr(readable, "seek"):
                    readable.seek(0)
            except Exception:
                pass
            raw = readable.read()
            if current_pos is not None and hasattr(readable, "seek"):
                try:
                    readable.seek(current_pos)
                except Exception:
                    pass
            if isinstance(raw, bytes):
                return raw
            if isinstance(raw, str):
                return raw.encode("utf-8", "replace")
        return bytes(cast(_typing.Any, payload))

    def _info_section(
        self: _typing.Self,
        metadata: object | None,
    ) -> bytes:
        """
        Perform the info section operation under explicit file-format and conversion rules.

        Example:
            Exercise RBWriter. info section through a consuming regression::

                python -m pytest -q tests/file_formats/rb/test_rb_modernized.py


        :param metadata: Value supplied for metadata under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        lines = ["TYPE=2"]
        if metadata:
            title_items = getattr(metadata, "title", ())
            if len(title_items) >= 1:
                title_value = getattr(title_items[0], "value", title_items[0])
                lines.append("TITLE=%s" % title_value)
            creator_items = getattr(metadata, "creator", ())
            if len(creator_items) >= 1:
                authors = [getattr(item, "value", item) for item in creator_items]
                lines.append("AUTHOR=%s" % authors_to_string(authors))
        lines.append("GENERATOR=%s - %s" % (__appname__, __version__))
        lines.append("PARSE=1")
        lines.append("OUTPUT=1")
        lines.append("BODY=index.html")
        text = "\n".join(lines) + "\n"
        return text.encode("cp1252", "replace")
