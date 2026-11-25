"""
Read a PDF's Info dictionary and XMP packet with a bounded, pure-Python object reader.

The reader follows the file's cross-reference data as a PDF viewer does: classic tables
and cross-reference streams, hybrid files, incremental updates chained by ``/Prev``,
object streams, and files with junk before the header. When that data is missing or
wrong it indexes the objects with one linear scan instead. It decrypts files that open
with an empty password (see :mod:`LiuXin_alpha.file_formats.pdf.security`; an AES file
goes to poppler's ``pdfinfo`` when the ``aes`` utils plugin says so), decodes
text strings as the PDF specification says (UTF-16 with a byte order mark, otherwise
PDFDocEncoding, with raw UTF-8 accepted because many tools write it), and reads only
the objects metadata needs, from a file or from bytes, without loading the whole file.

Every step is bounded: windows grow only to :attr:`PdfLimits.object_bytes`, decoded
streams stop at :attr:`PdfLimits.decoded_bytes`, all decoding for one document stops at
:attr:`PdfLimits.total_decoded_bytes`, and nesting, cross-reference sections and object
loads are counted. Scans use bounded patterns, so no input makes them quadratic.

Example:
    Read a minimal PDF::

        >>> pdf = minimal_pdf(b"<< /Title (Example) /Author (Ann) >>")
        >>> read_metadata(pdf).info["Title"]
        'Example'
"""

from __future__ import annotations

import bisect
import re
from collections import deque
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import IO, Any, NamedTuple, Self, TypeVar, cast

from LiuXin_alpha.file_formats.pdf.errors import (
    PdfEncryptedError,
    PdfNeedsPoppler,
    PdfReadError,
)
from LiuXin_alpha.file_formats.pdf.filters import FILTER_NAMES, apply_filter
from LiuXin_alpha.file_formats.pdf.security import StandardSecurityHandler
from LiuXin_alpha.file_formats.pdf.syntax import (
    WHITESPACE,
    Lexer,
    NeedMore,
    PdfName,
    PdfRef,
    PdfStream,
    decode_text,
    parse_value,
)

__all__ = [
    "PdfDocument",
    "PdfEncryptedError",
    "PdfLimits",
    "PdfMetadata",
    "PdfName",
    "PdfNeedsPoppler",
    "PdfReadError",
    "PdfRef",
    "PdfStream",
    "decode_text",
    "minimal_pdf",
    "read_metadata",
]

_T = TypeVar("_T")

_ENTRY20 = re.compile(rb"\d{10} \d{5} [nf](?: [\r\n]|\r\n)")
_ENTRY = re.compile(rb"[\x00\t\n\x0c\r ]*(\d{1,10})[ ]{1,8}(\d{1,5})[ ]{1,8}([nf])")
_ENDSTREAM_NEXT = re.compile(rb"[\x00\t\n\x0c\r ]{0,8}endstream")
_HEADER = re.compile(rb"%PDF-(\d\.\d)")
_DELIMITED = rb"(?![^\x00\t\n\x0c\r ()<>\[\]{}/%])"
_SCAN = re.compile(
    rb"(?<![0-9])(\d{1,10})[\x00\t\n\x0c\r ]{1,16}(\d{1,5})[\x00\t\n\x0c\r ]{1,16}obj" + _DELIMITED
    + rb"|(?<![A-Za-z0-9])(trailer)" + _DELIMITED
    + rb"|/(XRef|ObjStm|Catalog|Encrypt|Title|Author|Producer|Creator)" + _DELIMITED
)
_INFO_KEYS = frozenset({"Title", "Author", "Subject", "Keywords", "Creator", "Producer", "CreationDate", "ModDate"})
_STRUCTURE_KEYS = frozenset({"Type", "Parent", "Dest", "A", "Next", "Prev", "First", "Last", "Count", "Kids", "Subtype"})
_SCAN_CHUNK = 1 << 20
_SCAN_MARGIN = 64

@dataclass(frozen=True)
class PdfLimits:
    """
    Bound the work one document may cause.

    Example:
        >>> PdfLimits().depth
        64
    """

    #: The largest object outside a stream's data, such as a dictionary or a string.
    object_bytes: int = 1 << 20
    #: The most raw bytes read for one stream.
    stream_bytes: int = 64 << 20
    #: The most bytes one stream may decode to.
    decoded_bytes: int = 32 << 20
    #: The most bytes all streams of one document may decode or decrypt to.
    total_decoded_bytes: int = 128 << 20
    #: The most bytes decrypted with RC4 at once, which runs in pure Python.
    rc4_stream_bytes: int = 4 << 20
    #: The deepest nesting of arrays and dictionaries.
    depth: int = 64
    #: The most cross-reference sections followed through ``/Prev``.
    xref_sections: int = 256
    #: The most subsections in one cross-reference table.
    xref_subsections: int = 65536
    #: The most bytes of a cross-reference table read entry by entry when its lines are irregular.
    irregular_table_bytes: int = 8 << 20
    #: The most objects loaded for one document.
    objects: int = 4096
    #: The largest XMP packet returned.
    xmp_bytes: int = 4 << 20


@dataclass(frozen=True)
class PdfMetadata:
    """
    The metadata a PDF stores: its Info entries as text and its raw XMP packet.

    Example:
        >>> PdfMetadata({"Title": "T"}, None, False, "1.4").info
        {'Title': 'T'}
    """

    #: The Info dictionary's entries as text, or lists of text for arrays, keyed by name.
    info: dict[str, str | list[str]]
    #: The catalog's metadata stream, decoded, or ``None``.
    xmp: bytes | None
    #: Whether the file is encrypted (and was opened with an empty password).
    encrypted: bool
    #: The version in the file's header, such as ``"1.7"``.
    version: str | None
    #: Problems that cost part of the metadata, such as an unreadable XMP stream.
    problems: tuple[str, ...] = ()
    #: What read the file: this reader, or poppler for AES files without ``cryptography``.
    read_by: str = "LiuXin's object reader"


class _Entry(NamedTuple):
    """
    One cross-reference entry: free (0), in the file at an offset (1), or in an object stream (2).

    Example:
        >>> _Entry(1, 1024, 0).kind
        1
    """

    kind: int
    field2: int
    field3: int


def minimal_pdf(
    info: bytes,
    *,
    extra: Mapping[int, bytes] | None = None,
    catalog: bytes = b"<< /Type /Catalog >>",
    trailer: bytes = b"",
) -> bytes:
    """
    Write a small PDF with a classic cross-reference table, for examples and tests.

    Object 1 is the catalog and object 2 the Info dictionary; ``extra`` adds objects from 3.

    Example:
        >>> minimal_pdf(b"<< /Title (T) >>").startswith(b"%PDF-1.4")
        True


    :param info: The Info dictionary's source.
    :param extra: More objects' sources, by object number.
    :param catalog: The catalog's source.
    :param trailer: More trailer entries, such as ``/Encrypt`` and ``/ID``.
    :return: The PDF's bytes.
    """
    objects = {1: catalog, 2: info, **(extra or {})}
    out = bytearray(b"%PDF-1.4\n")
    offsets: dict[int, int] = {}
    for number in sorted(objects):
        offsets[number] = len(out)
        out += b"%d 0 obj\n" % number + objects[number] + b"\nendobj\n"
    size = max(objects) + 1
    table = len(out)
    out += b"xref\n0 %d\n0000000000 65535 f \n" % size
    for number in range(1, size):
        out += b"%010d 00000 n \n" % offsets[number] if number in offsets else b"0000000000 00001 f \n"
    out += b"trailer\n<< /Size %d /Root 1 0 R /Info 2 0 R %s>>\nstartxref\n%d\n%%%%EOF\n" % (size, trailer, table)
    return bytes(out)


class _Source:
    """
    Read byte ranges from bytes or a seekable binary file.

    Example:
        >>> _Source(b"0123456789").read(3, 4)
        b'3456'
    """

    def __init__(self: Self, target: bytes | bytearray | memoryview | IO[bytes]) -> None:
        """
        Wrap bytes, or a seekable binary file the caller keeps open.

        Example:
            >>> _Source(b"abc").size
            3


        :param target: The PDF's bytes, or a seekable binary file.
        :return: None.
        """
        self._data: bytes | None = None
        self._stream: IO[bytes] | None = None
        if isinstance(target, (bytes, bytearray, memoryview)):
            self._data = bytes(target)
            self.size = len(self._data)
        else:
            self._stream = target
            self.size = target.seek(0, 2)

    def read(self: Self, offset: int, size: int) -> bytes:
        """
        Read up to ``size`` bytes from an offset, clamped to the file.

        Example:
            >>> _Source(b"abc").read(-1, 3), _Source(b"abc").read(5, 2)
            (b'ab', b'')


        :param offset: Where to start.
        :param size: How many bytes to read at most.
        :return: The bytes.
        """
        if offset < 0:
            size += offset
            offset = 0
        size = min(size, self.size - offset)
        if size <= 0:
            return b""
        if self._data is not None:
            return self._data[offset : offset + size]
        assert self._stream is not None
        self._stream.seek(offset)
        return self._stream.read(size)

    def find(self: Self, needle: bytes, start: int, limit: int) -> int:
        """
        Find the first occurrence of ``needle`` within ``limit`` bytes of ``start``, in chunks.

        Example:
            >>> _Source(b"xx endstream").find(b"endstream", 0, 100)
            3


        :param needle: The bytes to find.
        :param start: Where to start.
        :param limit: How far to look.
        :return: The occurrence's offset, or -1.
        """
        end = min(self.size, start + limit)
        position = start
        while position < end:
            chunk = self.read(position, min(_SCAN_CHUNK, end - position) + len(needle) - 1)
            found = chunk.find(needle)
            if found >= 0:
                return position + found
            position += _SCAN_CHUNK
        return -1


class _TableSection:
    """
    A classic cross-reference table, read entry by entry from the file when its lines are regular.

    Example:
        >>> section = _TableSection(_Source(b"0000000017 00000 n \\n"), [(4, 1, 0)], {})
        >>> section.lookup(4)
        _Entry(kind=1, field2=17, field3=0)
    """

    def __init__(self: Self, source: _Source, subsections: list[tuple[int, int, int]], irregular: dict[int, _Entry]) -> None:
        """
        Keep the subsections of 20-byte lines and the entries read from irregular ones.

        Example:
            >>> _TableSection(_Source(b""), [], {}).xrefstm is None
            True


        :param source: The file.
        :param subsections: ``(first number, count, offset of the first line)`` for each regular subsection.
        :param irregular: Entries already read from irregular subsections.
        :return: None.
        """
        self.source = source
        self.subsections = subsections
        self.irregular = irregular
        self.xrefstm: _StreamSection | None = None

    def lookup(self: Self, number: int) -> _Entry | None:
        """
        Find an object's entry.

        Example:
            >>> _TableSection(_Source(b""), [], {}).lookup(1) is None
            True


        :param number: The object number.
        :return: The entry, or ``None`` when this table does not list the object.
        """
        entry = self.irregular.get(number)
        if entry is not None:
            return entry
        for start, count, position in self.subsections:
            if start <= number < start + count:
                match = _ENTRY.match(self.source.read(position + (number - start) * 20, 22))
                if match:
                    return _Entry(1 if match.group(3) == b"n" else 0, int(match.group(1)), int(match.group(2)))
                return None
        return None


class _StreamSection:
    """
    A decoded cross-reference stream.

    Example:
        >>> _StreamSection(bytes([2, 0, 4, 0]), (1, 2, 1), [(5, 1)]).lookup(5)
        _Entry(kind=2, field2=4, field3=0)
    """

    def __init__(self: Self, data: bytes, widths: tuple[int, int, int], index: list[tuple[int, int]]) -> None:
        """
        Keep the stream's rows and where each subsection's rows start.

        Example:
            >>> _StreamSection(b"", (1, 1, 1), []).row
            3


        :param data: The decoded rows.
        :param widths: The ``/W`` field widths.
        :param index: The ``/Index`` pairs of first number and count.
        :return: None.
        """
        self.data = data
        self.widths = widths
        self.row = sum(widths)
        self.ranges: list[tuple[int, int, int]] = []
        first_row = 0
        for start, count in index:
            self.ranges.append((start, count, first_row))
            first_row += count

    def lookup(self: Self, number: int) -> _Entry | None:
        """
        Find an object's entry.

        Example:
            >>> _StreamSection(bytes([1, 0, 9, 0]), (1, 2, 1), [(3, 1)]).lookup(3)
            _Entry(kind=1, field2=9, field3=0)


        :param number: The object number.
        :return: The entry, or ``None`` when the stream does not list the object or the row is
            missing or of an unknown type.
        """
        for start, count, first_row in self.ranges:
            if start <= number < start + count:
                at = (first_row + number - start) * self.row
                raw = self.data[at : at + self.row]
                if len(raw) < self.row:
                    return None
                width1, width2, _ = self.widths
                kind = int.from_bytes(raw[:width1], "big") if width1 else 1
                field2 = int.from_bytes(raw[width1 : width1 + width2], "big")
                field3 = int.from_bytes(raw[width1 + width2 :], "big")
                return _Entry(kind, field2, field3) if kind in (0, 1, 2) else None
        return None


@dataclass
class _Index:
    """
    What one linear scan of the file found: each object's newest definition and a few landmarks.

    Example:
        >>> _Index().offsets
        {}
    """

    offsets: dict[int, tuple[int, int]] = field(default_factory=dict)
    starts: list[int] = field(default_factory=list)
    trailers: deque[int] = field(default_factory=lambda: deque(maxlen=16))
    landmarks: dict[str, deque[int]] = field(default_factory=dict)
    saw_encrypt: bool = False

    def landmark(self: Self, name: str) -> deque[int]:
        """
        Return the positions of one landmark name, newest last.

        Example:
            >>> list(_Index().landmark("Catalog"))
            []


        :param name: ``XRef``, ``ObjStm``, ``Catalog`` or ``Info``.
        :return: The positions.
        """
        return self.landmarks.setdefault(name, deque(maxlen=4096 if name == "ObjStm" else 32))

    def containing(self: Self, position: int) -> int | None:
        """
        Find where the object that contains a position starts.

        Example:
            >>> index = _Index(starts=[10, 50])
            >>> index.containing(60), index.containing(5)
            (50, None)


        :param position: A position in the file.
        :return: The object's start, or ``None``.
        """
        at = bisect.bisect_right(self.starts, position) - 1
        return self.starts[at] if at >= 0 else None


class PdfDocument:
    """
    Read objects from a PDF through its cross-reference data, decrypting them if needed.

    Example:
        >>> document = PdfDocument(minimal_pdf(b"<< /Title (T) >>"))
        >>> document.resolve(document.trailer["Info"])
        {'Title': b'T'}
    """

    def __init__(self: Self, target: bytes | bytearray | memoryview | IO[bytes], *, limits: PdfLimits | None = None) -> None:
        """
        Find the header, load the cross-reference data and open the security handler.

        Example:
            >>> PdfDocument(b"not a pdf")
            Traceback (most recent call last):
            ...
            LiuXin_alpha.file_formats.pdf.errors.PdfReadError: no %PDF- header in the first 1024 bytes


        :param target: The PDF's bytes, or a seekable binary file the caller keeps open.
        :param limits: The bounds on work; the defaults suit metadata reading.
        :return: None.
        :raises PdfReadError: The file is not a PDF, or no objects can be found in it.
        :raises PdfEncryptedError: The file is encrypted and cannot be opened.
        """
        self.source = _Source(target)
        self.limits = limits or PdfLimits()
        header = _HEADER.search(self.source.read(0, 1024))
        if header is None:
            raise PdfReadError("no %PDF- header in the first 1024 bytes")
        self.header_offset = header.start()
        self.version: str | None = header.group(1).decode("ascii")
        self.shift = 0
        self.sections: list[_TableSection | _StreamSection] = []
        self.security: StandardSecurityHandler | None = None
        self._loaded = False
        self._cache: dict[PdfRef, Any] = {}
        self._resolving: set[PdfRef] = set()
        self._loads = 0
        self._decoded = 0
        self._objstms: dict[int, tuple[bytes, int, list[tuple[int, int]], dict[int, int]]] = {}
        self._compressed: dict[int, tuple[int, int]] = {}
        self._index: _Index | None = None
        self._trailer_found = True
        self._encrypt_ref: PdfRef | None = None
        self.trailer = self._load_trailer()
        self._loaded = True
        self.security = self._open_security()

    # --- windows and objects ----------------------------------------------------------------------

    def _window(self: Self, offset: int, parse: Callable[[Lexer], _T], *, initial: int = 4096) -> _T:
        """
        Parse from an offset, reading a larger window each time the parse needs more bytes.

        Example:
            >>> document = PdfDocument(minimal_pdf(b"<< >>"))
            >>> document._window(9, lambda lexer: lexer.token())
            ('int', 1)


        :param offset: Where to start.
        :param parse: The parse to run on a lexer over the window.
        :param initial: The first window's size.
        :return: What ``parse`` returns.
        :raises PdfReadError: The parse needs more than :attr:`PdfLimits.object_bytes`.
        """
        size = min(initial, self.limits.object_bytes)
        while True:
            data = self.source.read(offset, size)
            complete = offset + len(data) >= self.source.size
            try:
                return parse(Lexer(data, 0, complete))
            except NeedMore:
                if size >= self.limits.object_bytes:
                    raise PdfReadError("an object is larger than the reading limit") from None
                size = min(size * 8, self.limits.object_bytes)

    def _value(self: Self, lexer: Lexer) -> Any:
        """
        Parse the next object from a lexer.

        Example:
            >>> PdfDocument(minimal_pdf(b"<< >>"))._value(Lexer(b"[1 2]"))
            [1, 2]


        :param lexer: The lexer.
        :return: The object.
        """
        kind, value = lexer.token()
        return parse_value(lexer, kind, value, 0, self.limits.depth)

    def _object_at(self: Self, offset: int) -> tuple[PdfRef, Any]:
        """
        Parse the indirect object that starts at an offset, keeping a stream's data in the file.

        Example:
            >>> pdf = minimal_pdf(b"<< /Title (T) >>")
            >>> PdfDocument(pdf)._object_at(pdf.index(b"2 0 obj"))
            (PdfRef(number=2, generation=0), {'Title': b'T'})


        :param offset: Where ``N G obj`` starts.
        :return: The object's reference and value, a :class:`PdfStream` for a stream.
        :raises PdfReadError: No object starts there, or it cannot be parsed.
        """

        def parse(lexer: Lexer) -> tuple[PdfRef, Any, int]:
            """
            Parse ``N G obj``, the value and, for a dictionary, a following ``stream`` keyword.

            Example:
                >>> parse(Lexer(b"1 0 obj 5 endobj"))  # doctest: +SKIP
                (PdfRef(number=1, generation=0), 5, -1)


            :param lexer: The lexer over a window at the offset.
            :return: The reference, the value and where the stream's data starts in the window, or -1.
            """
            first, second, third = lexer.token(), lexer.token(), lexer.token()
            if first[0] != "int" or second[0] != "int" or third != ("keyword", b"obj"):
                raise PdfReadError(f"no object at offset {offset}")
            value = self._value(lexer)
            if isinstance(value, dict):
                saved = lexer.pos
                if lexer.token() == ("keyword", b"stream"):
                    start = lexer.pos
                    if start + 2 > len(lexer.data) and not lexer.complete:
                        raise NeedMore
                    if lexer.data[start : start + 1] == b"\r":
                        start += 1
                    if lexer.data[start : start + 1] == b"\n":
                        start += 1
                    return PdfRef(first[1], second[1]), value, start
                lexer.pos = saved
            return PdfRef(first[1], second[1]), value, -1

        ref, value, start = self._window(offset, parse)
        if start >= 0:
            return ref, PdfStream(value, offset + start, ref)
        return ref, value

    def _stream_raw(self: Self, stream: PdfStream) -> bytes:
        """
        Read a stream's raw data, using ``/Length`` when it is right and ``endstream`` otherwise.

        Example:
            >>> pdf = minimal_pdf(b"<< >>", extra={3: b"<< /Length 99 >>\\nstream\\nabc\\nendstream"})
            >>> document = PdfDocument(pdf)
            >>> document._stream_raw(document.resolve(PdfRef(3, 0)))
            b'abc'


        :param stream: The stream.
        :return: The raw data.
        :raises PdfReadError: The stream has no end within :attr:`PdfLimits.stream_bytes`.
        """
        length = stream.dictionary.get("Length")
        if isinstance(length, PdfRef) and self._loaded:
            try:
                length = self.resolve(length)
            except PdfEncryptedError:
                raise
            except PdfReadError:
                length = None
        if isinstance(length, int) and 0 <= length <= self.limits.stream_bytes:
            if _ENDSTREAM_NEXT.match(self.source.read(stream.offset + length, 32)):
                return self.source.read(stream.offset, length)
        end = self.source.find(b"endstream", stream.offset, self.limits.stream_bytes + 2)
        if end < 0:
            raise PdfReadError("a stream has no endstream within the reading limit")
        data = self.source.read(stream.offset, end - stream.offset)
        if data.endswith(b"\r\n"):
            return data[:-2]
        if data.endswith((b"\n", b"\r")):
            return data[:-1]
        return data

    # --- decoding ---------------------------------------------------------------------------------

    def _spend(self: Self, size: int) -> int:
        """
        Check a decoded size against the per-document budget, and return what remains for one stream.

        Example:
            >>> PdfDocument(minimal_pdf(b"<< >>"))._spend(0) == PdfLimits().decoded_bytes
            True


        :param size: Bytes just decoded.
        :return: How many bytes the next stream may decode to.
        :raises PdfReadError: The document has decoded more than :attr:`PdfLimits.total_decoded_bytes`.
        """
        self._decoded += size
        if self._decoded > self.limits.total_decoded_bytes:
            raise PdfReadError("the document decodes to more than the reading limit")
        return min(self.limits.decoded_bytes, self.limits.total_decoded_bytes - self._decoded)

    def _filters(self: Self, dictionary: Mapping[str, Any]) -> list[tuple[str, Mapping[str, Any]]]:
        """
        List a stream's filters with their parameters.

        Example:
            >>> PdfDocument(minimal_pdf(b"<< >>"))._filters({"Filter": PdfName("Fl")})
            [('FlateDecode', {})]


        :param dictionary: The stream's dictionary.
        :return: ``(filter name, parameters)`` pairs, in the order to apply them.
        """
        names = self.resolve(dictionary.get("Filter"))
        parms = self.resolve(dictionary.get("DecodeParms", dictionary.get("DP")))
        names = names if isinstance(names, list) else [names] if names is not None else []
        parms = parms if isinstance(parms, list) else [parms]
        out = []
        for position, name in enumerate(names):
            name = self.resolve(name)
            parm = self.resolve(parms[position]) if position < len(parms) else None
            out.append((FILTER_NAMES.get(str(name), str(name)), parm if isinstance(parm, Mapping) else {}))
        return out

    def _decode(self: Self, stream: PdfStream, *, decrypt: bool = True) -> bytes:
        """
        Read, decrypt and decode a stream.

        Example:
            >>> import zlib
            >>> data = zlib.compress(b"hello")
            >>> pdf = minimal_pdf(b"<< >>", extra={3: b"<< /Filter /FlateDecode /Length %d >>\\nstream\\n%s\\nendstream" % (len(data), data)})
            >>> document = PdfDocument(pdf)
            >>> document._decode(document.resolve(PdfRef(3, 0)))
            b'hello'


        :param stream: The stream.
        :param decrypt: Decrypt the data when the file is encrypted.
        :return: The decoded data.
        :raises PdfReadError: A filter is unknown or its data corrupt, or a limit is reached.
        """
        data = self._stream_raw(stream)
        filters = self._filters(stream.dictionary)
        if decrypt and self.security is not None and self._encrypted_stream(stream, filters):
            if self.security.stream_method == "V2" and len(data) > self.limits.rc4_stream_bytes:
                raise PdfReadError("an RC4-encrypted stream is larger than the reading limit")
            self._spend(len(data))
            data = self.security.decrypt_stream(data, *stream.ref)
        for name, parms in filters:
            if name == "Crypt":
                continue
            remaining = self._spend(0)
            data = apply_filter(name, parms, data, remaining)
            self._spend(len(data))
        return data

    def jpeg_data(self: Self, stream: PdfStream) -> bytes | None:
        """
        Give an image stream's JPEG: its data decrypted, with every filter undone but a final ``DCTDecode``.

        Example:
            >>> jpeg = b"\\xff\\xd8 scan \\xff\\xd9"
            >>> pdf = minimal_pdf(b"<< >>", extra={3: b"<< /Filter /DCTDecode /Length %d >>\\nstream\\n%s\\nendstream" % (len(jpeg), jpeg)})
            >>> document = PdfDocument(pdf)
            >>> document.jpeg_data(document.resolve(PdfRef(3, 0))) == jpeg
            True


        :param stream: The image stream.
        :return: The JPEG, or ``None`` when the stream is not one.
        :raises PdfReadError: A filter's data is corrupt, or a limit is reached.
        """
        filters = [(name, parms) for name, parms in self._filters(stream.dictionary) if name != "Crypt"]
        if not filters or filters[-1][0] != "DCTDecode":
            return None
        data = self._stream_raw(stream)
        if self.security is not None and self._encrypted_stream(stream, self._filters(stream.dictionary)):
            if self.security.stream_method == "V2" and len(data) > self.limits.rc4_stream_bytes:
                raise PdfReadError("an RC4-encrypted stream is larger than the reading limit")
            self._spend(len(data))
            data = self.security.decrypt_stream(data, *stream.ref)
        for name, parms in filters[:-1]:
            data = apply_filter(name, parms, data, self._spend(0))
            self._spend(len(data))
        return data if data.startswith(b"\xff\xd8") else None

    def _encrypted_stream(self: Self, stream: PdfStream, filters: list[tuple[str, Mapping[str, Any]]]) -> bool:
        """
        Tell whether a stream's data is encrypted.

        Cross-reference streams never are, nor metadata streams when ``/EncryptMetadata`` is
        false, nor streams whose first filter is the Identity crypt filter.

        Example:
            >>> stream = PdfStream({"Type": PdfName("XRef")}, 0, PdfRef(1, 0))
            >>> PdfDocument(minimal_pdf(b"<< >>"))._encrypted_stream(stream, [])
            False


        :param stream: The stream.
        :param filters: Its filters.
        :return: Whether to decrypt it.
        """
        kind = stream.dictionary.get("Type")
        if kind == "XRef":
            return False
        if kind == "Metadata" and self.security is not None and not self.security.encrypt_metadata:
            return False
        if filters and filters[0][0] == "Crypt":
            return bool(filters[0][1].get("Name", "Identity") != "Identity")
        return True

    # --- cross-reference data ---------------------------------------------------------------------

    def _startxref(self: Self) -> int | None:
        """
        Find the offset that the last ``startxref`` in the file's final 64 KiB gives.

        Example:
            >>> pdf = minimal_pdf(b"<< >>")
            >>> PdfDocument(pdf)._startxref() == pdf.index(b"xref\\n0")
            True


        :return: The offset, or ``None``.
        """
        start = max(0, self.source.size - 65536)
        tail = self.source.read(start, 65536)
        found = tail.rfind(b"startxref")
        if found < 0:
            return None
        lexer = Lexer(tail, found + 9)
        kind, value = lexer.token()
        return value if kind == "int" and value >= 0 else None

    def _read_section(self: Self, offset: int) -> tuple[_TableSection | _StreamSection, dict[str, Any]]:
        """
        Read the cross-reference table or stream at an offset, with its trailer.

        Example:
            >>> pdf = minimal_pdf(b"<< >>")
            >>> PdfDocument(pdf)._read_section(pdf.index(b"xref\\n0"))[1]["Size"]
            3


        :param offset: Where the section starts.
        :return: The section and its trailer dictionary.
        :raises PdfReadError: No section starts there.
        """
        kind, value = self._window(offset, lambda lexer: lexer.token(), initial=64)
        if (kind, value) == ("keyword", b"xref"):
            return self._read_table(offset)
        if kind == "int":
            return self._read_xref_stream(offset)
        raise PdfReadError(f"no cross-reference section at offset {offset}")

    def _regular(self: Self, position: int, count: int) -> bool:
        """
        Tell whether a subsection's lines are all 20 bytes long, so entries can be read by position.

        Example:
            >>> PdfDocument(minimal_pdf(b"<< >>"))._regular(0, 0)
            True


        :param position: Where the first line starts.
        :param count: How many lines the subsection header promises.
        :return: Whether the first and last lines are well formed and a header or ``trailer`` follows.
        """
        if count == 0:
            return True
        end = position + count * 20
        if end > self.source.size:
            return False
        if not _ENTRY20.match(self.source.read(position, 20)) or not _ENTRY20.match(self.source.read(end - 20, 20)):
            return False
        after = self.source.read(end, 64).lstrip(WHITESPACE)
        return after.startswith(b"trailer") or after[:1].isdigit()

    def _irregular_entries(self: Self, position: int, start: int, count: int) -> tuple[dict[int, _Entry], int]:
        """
        Read a subsection whose lines are not all 20 bytes, entry by entry.

        Example:
            >>> document = PdfDocument(minimal_pdf(b"<< >>"))
            >>> document._irregular_entries(0, 0, 1)[0]  # doctest: +SKIP


        :param position: Where the first line starts.
        :param start: The first object number.
        :param count: How many entries the header promises.
        :return: The entries and where the subsection ends.
        :raises PdfReadError: The subsection is larger than :attr:`PdfLimits.irregular_table_bytes`.
        """
        window = self.source.read(position, min(count * 22 + 64, self.limits.irregular_table_bytes))
        entries: dict[int, _Entry] = {}
        at = 0
        for offset in range(count):
            match = _ENTRY.match(window, at)
            if match is None:
                if at + 64 >= len(window) and position + len(window) < self.source.size:
                    raise PdfReadError("a cross-reference table is larger than the reading limit")
                break
            kind = 1 if match.group(3) == b"n" else 0
            entries.setdefault(start + offset, _Entry(kind, int(match.group(1)), int(match.group(2))))
            at = match.end()
        return entries, position + at

    def _read_table(self: Self, offset: int) -> tuple[_TableSection, dict[str, Any]]:
        """
        Read a classic cross-reference table and its trailer.

        Example:
            >>> pdf = minimal_pdf(b"<< >>")
            >>> section, trailer = PdfDocument(pdf)._read_table(pdf.index(b"xref\\n0"))
            >>> section.lookup(2).kind, trailer["Info"]
            (1, PdfRef(number=2, generation=0))


        :param offset: Where ``xref`` starts.
        :return: The section and its trailer dictionary.
        :raises PdfReadError: The table or trailer is malformed or too large.
        """

        def after_keyword(lexer: Lexer) -> int:
            """
            Move past ``xref``.

            Example:
                >>> after_keyword(Lexer(b"xref 0 1"))  # doctest: +SKIP
                4


            :param lexer: The lexer at the table.
            :return: The position after the keyword.
            """
            lexer.token()
            return lexer.pos

        def next_part(lexer: Lexer) -> tuple[Any, ...]:
            """
            Read a subsection header, or ``trailer`` and its dictionary.

            Example:
                >>> next_part(Lexer(b"0 3\\n"))  # doctest: +SKIP
                ('subsection', 0, 3, 4)


            :param lexer: The lexer at the part.
            :return: ``("subsection", start, count, entries position)`` or ``("trailer", dictionary)``.
            """
            kind, value = lexer.token()
            if (kind, value) == ("keyword", b"trailer"):
                return "trailer", self._value(lexer)
            if kind == "int":
                count_kind, count = lexer.token()
                if count_kind == "int" and value >= 0 and count >= 0:
                    lexer.skip()
                    return "subsection", value, count, lexer.pos
            raise PdfReadError("malformed cross-reference table")

        position = offset + self._window(offset, after_keyword, initial=64)
        subsections: list[tuple[int, int, int]] = []
        irregular: dict[int, _Entry] = {}
        for _ in range(self.limits.xref_subsections):
            part = self._window(position, next_part, initial=256)
            if part[0] == "trailer":
                trailer = part[1]
                if not isinstance(trailer, dict):
                    raise PdfReadError("a cross-reference trailer is not a dictionary")
                return _TableSection(self.source, subsections, irregular), trailer
            _, start, count, relative = part
            entries_at = position + relative
            if self._regular(entries_at, count):
                subsections.append((start, count, entries_at))
                position = entries_at + count * 20
            else:
                entries, position = self._irregular_entries(entries_at, start, count)
                for number, entry in entries.items():
                    irregular.setdefault(number, entry)
        raise PdfReadError("a cross-reference table has too many subsections")

    def _read_xref_stream(self: Self, offset: int) -> tuple[_StreamSection, dict[str, Any]]:
        """
        Read a cross-reference stream; its dictionary is also its section's trailer.

        Example:
            >>> PdfDocument(minimal_pdf(b"<< >>"))._read_xref_stream(9)
            Traceback (most recent call last):
            ...
            LiuXin_alpha.file_formats.pdf.errors.PdfReadError: no cross-reference stream at offset 9


        :param offset: Where the stream object starts.
        :return: The section and the stream's dictionary.
        :raises PdfReadError: The object is not a well-formed cross-reference stream.
        """
        _, value = self._object_at(offset)
        if not isinstance(value, PdfStream) or value.dictionary.get("Type") != "XRef":
            raise PdfReadError(f"no cross-reference stream at offset {offset}")
        dictionary = value.dictionary
        widths = dictionary.get("W")
        size = dictionary.get("Size")
        if (
            not isinstance(widths, list)
            or len(widths) < 3
            or not all(isinstance(width, int) and 0 <= width <= 8 for width in widths[:3])
            or not isinstance(size, int)
            or size < 0
        ):
            raise PdfReadError("a cross-reference stream has malformed /W or /Size")
        index = dictionary.get("Index", [0, size])
        if not isinstance(index, list) or len(index) % 2 or not all(isinstance(item, int) and item >= 0 for item in index):
            raise PdfReadError("a cross-reference stream has a malformed /Index")
        triple = (widths[0], widths[1], widths[2])
        if sum(triple) == 0:
            raise PdfReadError("a cross-reference stream has empty rows")
        data = self._decode(value, decrypt=False)
        pairs = [(index[at], index[at + 1]) for at in range(0, len(index), 2)]
        return _StreamSection(data, triple, pairs), dictionary

    def _read_chain(self: Self) -> list[dict[str, Any]]:
        """
        Read the cross-reference sections from ``startxref`` back through ``/Prev``.

        Example:
            >>> len(PdfDocument(minimal_pdf(b"<< >>"))._read_chain())
            1


        :return: The trailers, newest first.
        :raises PdfReadError: The newest section cannot be read.
        """
        start = self._startxref()
        if start is None:
            raise PdfReadError("no startxref")
        shifts = (0, self.header_offset) if self.header_offset else (0,)
        error: PdfReadError | None = None
        for shift in shifts:
            try:
                section, trailer = self._read_section(start + shift)
            except PdfReadError as err:
                error = err
                continue
            self.shift = shift
            break
        else:
            assert error is not None
            raise error
        sections: list[_TableSection | _StreamSection] = [section]
        trailers = [trailer]
        self._attach_xrefstm(section, trailer)
        seen = {start}
        while len(sections) < self.limits.xref_sections:
            previous = trailer.get("Prev")
            if not isinstance(previous, int) or previous < 0 or previous in seen:
                break
            seen.add(previous)
            try:
                section, trailer = self._read_section(previous + self.shift)
            except PdfReadError:
                break
            self._attach_xrefstm(section, trailer)
            sections.append(section)
            trailers.append(trailer)
        self.sections = sections
        return trailers

    def _attach_xrefstm(self: Self, section: _TableSection | _StreamSection, trailer: Mapping[str, Any]) -> None:
        """
        Read the ``/XRefStm`` stream of a hybrid file's table, if there is one.

        Example:
            >>> document = PdfDocument(minimal_pdf(b"<< >>"))
            >>> document._attach_xrefstm(document.sections[0], {})


        :param section: The section the trailer belongs to.
        :param trailer: Its trailer.
        :return: None.
        """
        offset = trailer.get("XRefStm")
        if isinstance(section, _TableSection) and isinstance(offset, int) and offset >= 0:
            try:
                section.xrefstm = self._read_xref_stream(offset + self.shift)[0]
            except PdfReadError:
                section.xrefstm = None

    def _load_trailer(self: Self) -> dict[str, Any]:
        """
        Merge the trailers newest first, rebuilding them from a scan when the chain is broken.

        Example:
            >>> sorted(PdfDocument(minimal_pdf(b"<< >>")).trailer)
            ['Info', 'Root', 'Size']


        :return: The merged trailer.
        :raises PdfReadError: Neither the chain nor a scan finds a trailer or any object.
        """
        try:
            trailers = self._read_chain()
        except PdfReadError:
            trailers = []
        if not any("Root" in trailer or "Info" in trailer for trailer in trailers):
            trailers = self._rebuilt_trailers()
        merged: dict[str, Any] = {}
        for trailer in trailers:
            for key, value in trailer.items():
                merged.setdefault(key, value)
        return merged

    # --- the scan ---------------------------------------------------------------------------------

    def _scan(self: Self) -> _Index:
        """
        Index every object definition and a few landmarks with one linear scan.

        Example:
            >>> sorted(PdfDocument(minimal_pdf(b"<< >>"))._scan().offsets)
            [1, 2]


        :return: The index, built once and kept.
        """
        if self._index is not None:
            return self._index
        index = _Index()
        for start in range(0, self.source.size, _SCAN_CHUNK):
            base = max(0, start - _SCAN_MARGIN)
            window = self.source.read(base, start - base + _SCAN_CHUNK + _SCAN_MARGIN)
            for match in _SCAN.finditer(window, start - base):
                at = base + match.start()
                if at >= start + _SCAN_CHUNK:
                    break
                if match.group(1) is not None:
                    number = int(match.group(1))
                    index.offsets[number] = (at, int(match.group(2)))
                    index.starts.append(at)
                elif match.group(3) is not None:
                    index.trailers.append(at)
                else:
                    name = match.group(4).decode("ascii")
                    if name == "Encrypt":
                        index.saw_encrypt = True
                    elif name in ("XRef", "ObjStm", "Catalog"):
                        index.landmark(name).append(at)
                    else:
                        index.landmark("Info").append(at)
        self._index = index
        return index

    def _containing_object(self: Self, index: _Index, position: int) -> tuple[PdfRef, Any] | None:
        """
        Parse the object whose definition contains a position.

        Example:
            >>> pdf = minimal_pdf(b"<< /Title (T) >>")
            >>> document = PdfDocument(pdf)
            >>> document._containing_object(document._scan(), pdf.index(b"/Title"))[0]
            PdfRef(number=2, generation=0)


        :param index: The scan's index.
        :param position: A position in the file.
        :return: The object's reference and value, or ``None`` when it cannot be parsed.
        """
        start = index.containing(position)
        if start is None:
            return None
        try:
            return self._object_at(start)
        except PdfReadError:
            return None

    def _rebuilt_trailers(self: Self) -> list[dict[str, Any]]:
        """
        Find trailers from a scan: ``trailer`` keywords, cross-reference streams, then a catalog and an Info-like dictionary.

        Example:
            >>> pdf = b"%PDF-1.4\\n1 0 obj << /Type /Catalog >> endobj\\ntrailer << /Root 1 0 R >>\\n"
            >>> PdfDocument(pdf).trailer
            {'Root': PdfRef(number=1, generation=0)}


        :return: The trailers, newest first.
        :raises PdfReadError: The scan finds no objects at all.
        """
        index = self._scan()
        if not index.offsets:
            raise PdfReadError("no objects found in the file")
        found: list[tuple[int, dict[str, Any]]] = []
        for position in index.trailers:

            def parse(lexer: Lexer) -> Any:
                """
                Parse ``trailer`` and the dictionary after it.

                Example:
                    >>> parse(Lexer(b"trailer << >>"))  # doctest: +SKIP
                    {}


                :param lexer: The lexer at the keyword.
                :return: The dictionary.
                """
                lexer.token()
                return self._value(lexer)

            try:
                trailer = self._window(position, parse, initial=1024)
            except PdfReadError:
                continue
            if isinstance(trailer, dict):
                found.append((position, trailer))
        for position in index.landmark("XRef"):
            parsed = self._containing_object(index, position)
            if parsed is not None and isinstance(parsed[1], PdfStream) and parsed[1].dictionary.get("Type") == "XRef":
                found.append((position, parsed[1].dictionary))
        found.sort(key=lambda item: item[0], reverse=True)
        trailers = [trailer for _, trailer in found if "Root" in trailer or "Info" in trailer or "Encrypt" in trailer]
        self._trailer_found = bool(trailers)
        if not any("Root" in trailer for trailer in trailers):
            for position in reversed(index.landmark("Catalog")):
                parsed = self._containing_object(index, position)
                if parsed is not None and isinstance(parsed[1], dict) and parsed[1].get("Type") == "Catalog":
                    trailers.append({"Root": parsed[0]})
                    break
        if not self._trailer_found:
            for position in reversed(index.landmark("Info")):
                parsed = self._containing_object(index, position)
                if parsed is not None and _looks_like_info(parsed[1]):
                    trailers.append({"Info": parsed[0]})
                    break
        return trailers

    # --- objects ----------------------------------------------------------------------------------

    def _locate(self: Self, number: int, *, compressed: bool = True) -> _Entry | None:
        """
        Find an object's entry: in the sections newest first, then in the scan's index.

        Example:
            >>> pdf = minimal_pdf(b"<< >>")
            >>> PdfDocument(pdf)._locate(2) == _Entry(1, pdf.index(b"2 0 obj"), 0)
            True


        :param number: The object number.
        :param compressed: Also look for it in object streams.
        :return: The entry, with an absolute offset for objects in the file, or ``None``.
        """
        for section in self.sections:
            parts = [section, section.xrefstm] if isinstance(section, _TableSection) else [section]
            for part in parts:
                entry = part.lookup(number) if part is not None else None
                if entry is None or entry.kind == 0:
                    continue
                if entry.kind == 2 and not compressed:
                    continue
                if entry.kind == 1:
                    return _Entry(1, entry.field2 + self.shift, entry.field3)
                return entry
        if self._index is not None:
            located = self._index.offsets.get(number)
            if located is not None:
                return _Entry(1, located[0], located[1])
            if compressed:
                found = self._find_compressed(number)
                if found is not None:
                    return _Entry(2, *found)
        return None

    def _find_compressed(self: Self, number: int) -> tuple[int, int] | None:
        """
        Look for an object in the object streams a scan found, newest first.

        Example:
            >>> PdfDocument(minimal_pdf(b"<< >>"))._find_compressed(9) is None
            True


        :param number: The object number.
        :return: ``(object stream number, index)`` or ``None``.
        """
        if number in self._compressed:
            return self._compressed[number]
        index = self._index
        if index is None:
            return None
        for position in reversed(index.landmark("ObjStm")):
            start = index.containing(position)
            if start is None:
                continue
            try:
                ref, value = self._object_at(start)
            except PdfReadError:
                continue
            if ref.number in self._objstms or not isinstance(value, PdfStream):
                continue
            try:
                _, _, entries, _ = self._objstm(ref.number)
            except PdfReadError:
                continue
            for at, (member, _) in enumerate(entries):
                self._compressed.setdefault(member, (ref.number, at))
            if number in self._compressed:
                return self._compressed[number]
        return None

    def _objstm(self: Self, number: int) -> tuple[bytes, int, list[tuple[int, int]], dict[int, int]]:
        """
        Decode an object stream and read its header of object numbers and offsets.

        Example:
            >>> PdfDocument(minimal_pdf(b"<< >>"))._objstm(1)
            Traceback (most recent call last):
            ...
            LiuXin_alpha.file_formats.pdf.errors.PdfReadError: object 1 is not an object stream


        :param number: The object stream's object number.
        :return: The decoded data, ``/First``, the ``(number, offset)`` pairs and offsets by number.
        :raises PdfReadError: The object is not a well-formed object stream.
        """
        if number in self._objstms:
            return self._objstms[number]
        entry = self._locate(number, compressed=False)
        if entry is None or entry.kind != 1:
            raise PdfReadError(f"object {number} is not an object stream")
        stream = self.get_object(PdfRef(number, entry.field3))
        if not isinstance(stream, PdfStream) or stream.dictionary.get("Type") != "ObjStm":
            raise PdfReadError(f"object {number} is not an object stream")
        count, first = stream.dictionary.get("N"), stream.dictionary.get("First")
        if not isinstance(count, int) or not isinstance(first, int) or count < 0 or first < 0:
            raise PdfReadError(f"object stream {number} has a malformed /N or /First")
        data = self._decode(stream)
        if first > len(data):
            raise PdfReadError(f"object stream {number} is shorter than its /First")
        lexer = Lexer(data[:first])
        entries: list[tuple[int, int]] = []
        offsets: dict[int, int] = {}
        for _ in range(count):
            member, offset = lexer.token(), lexer.token()
            if member[0] != "int" or offset[0] != "int":
                break
            entries.append((member[1], offset[1]))
            offsets.setdefault(member[1], offset[1])
        self._objstms[number] = (data, first, entries, offsets)
        return self._objstms[number]

    def _load_compressed(self: Self, ref: PdfRef, stream_number: int, position: int) -> Any:
        """
        Parse an object from an object stream.

        Example:
            >>> PdfDocument(minimal_pdf(b"<< >>"))._load_compressed(PdfRef(5, 0), 1, 0)
            Traceback (most recent call last):
            ...
            LiuXin_alpha.file_formats.pdf.errors.PdfReadError: object 1 is not an object stream


        :param ref: The object's reference.
        :param stream_number: The object stream's object number.
        :param position: The object's index in the stream.
        :return: The object, or ``None`` when the stream does not hold it.
        """
        data, first, entries, offsets = self._objstm(stream_number)
        if 0 <= position < len(entries) and entries[position][0] == ref.number:
            offset: int | None = entries[position][1]
        else:
            offset = offsets.get(ref.number)
        if offset is None or first + offset >= len(data):
            return None
        return self._value(Lexer(data, first + offset))

    def _load(self: Self, ref: PdfRef) -> Any:
        """
        Load an object from where the cross-reference data says, or from where a scan finds it.

        Example:
            >>> PdfDocument(minimal_pdf(b"<< /T 1 >>"))._load(PdfRef(2, 0))
            {'T': 1}


        :param ref: The reference.
        :return: The object, decrypted, or ``None`` when the file does not hold it.
        :raises PdfReadError: The object is where the file says but cannot be parsed, and a
            scan finds it nowhere else.
        """
        entry = self._locate(ref.number)
        if entry is not None and entry.kind == 2:
            try:
                value = self._load_compressed(ref, entry.field2, entry.field3)
            except PdfEncryptedError:
                raise
            except PdfReadError:
                value = None
            if value is not None:
                return value
        tried: int | None = None
        error: PdfReadError | None = None
        if entry is not None and entry.kind == 1:
            tried = entry.field2
            try:
                found, value = self._object_at(entry.field2)
            except PdfEncryptedError:
                raise
            except PdfReadError as err:
                found, error = None, err
            if found is not None and found.number == ref.number:
                return self._decrypt_object(found, value)
        if self._index is None:
            # The cross-reference data is missing or wrong: index the file and look again.
            self._scan()
            return self._load(ref)
        located = self._index.offsets.get(ref.number)
        if located is None or located[0] == tried:
            if error is not None:
                raise error
            return None
        found, value = self._object_at(located[0])
        return self._decrypt_object(found, value) if found.number == ref.number else None

    def _decrypt_object(self: Self, ref: PdfRef, value: Any) -> Any:
        """
        Decrypt the strings of an object read from the file, except in the Encrypt dictionary.

        Example:
            >>> PdfDocument(minimal_pdf(b"<< >>"))._decrypt_object(PdfRef(1, 0), b"x")
            b'x'


        :param ref: The object's reference.
        :param value: The object.
        :return: The object with its strings decrypted.
        """
        if self.security is None or ref == self._encrypt_ref:
            return value
        return self._decrypt_value(value, ref)

    def _decrypt_value(self: Self, value: Any, ref: PdfRef) -> Any:
        """
        Decrypt every string in a value, including those in a stream's dictionary.

        Example:
            >>> document = PdfDocument(minimal_pdf(b"<< >>"))
            >>> document.security = StandardSecurityHandler.for_test_vector()
            >>> secret = document.security.encrypt_string(b"text", 1, 0)
            >>> document._decrypt_value([1, PdfName("N"), secret], PdfRef(1, 0))
            [1, 'N', b'text']


        :param value: The value.
        :param ref: The object it belongs to, which keys the decryption.
        :return: The decrypted value.
        """
        assert self.security is not None
        if isinstance(value, bytes):
            return self.security.decrypt_string(value, *ref)
        if isinstance(value, list):
            return [self._decrypt_value(item, ref) for item in value]
        if isinstance(value, dict):
            return {key: self._decrypt_value(item, ref) for key, item in value.items()}
        if isinstance(value, PdfStream):
            return PdfStream(self._decrypt_value(value.dictionary, ref), value.offset, value.ref)
        return value

    def get_object(self: Self, ref: PdfRef) -> Any:
        """
        Load an object by reference, once.

        Example:
            >>> PdfDocument(minimal_pdf(b"<< /T 1 >>")).get_object(PdfRef(2, 0))
            {'T': 1}


        :param ref: The reference.
        :return: The object, or ``None`` when the file does not hold it.
        :raises PdfReadError: The reference refers to itself through other objects, or the
            document loads more objects than :attr:`PdfLimits.objects`.
        """
        if ref in self._cache:
            return self._cache[ref]
        if ref in self._resolving:
            raise PdfReadError(f"object {ref.number} refers to itself")
        self._loads += 1
        if self._loads > self.limits.objects:
            raise PdfReadError("the document loads more objects than the reading limit")
        self._resolving.add(ref)
        try:
            value = self._load(ref)
        finally:
            self._resolving.discard(ref)
        self._cache[ref] = value
        return value

    def resolve(self: Self, value: Any) -> Any:
        """
        Follow references until a value is direct.

        Example:
            >>> PdfDocument(minimal_pdf(b"<< /T 1 >>")).resolve(PdfRef(2, 0))
            {'T': 1}


        :param value: A value or a reference.
        :return: The direct value.
        """
        for _ in range(32):
            if not isinstance(value, PdfRef):
                return value
            value = self.get_object(value)
        raise PdfReadError("a chain of references is longer than the reading limit")

    # --- encryption -------------------------------------------------------------------------------

    def _open_security(self: Self) -> StandardSecurityHandler | None:
        """
        Open the security handler the trailer names, with an empty password.

        Example:
            >>> PdfDocument(minimal_pdf(b"<< >>")).security is None
            True


        :return: The handler, or ``None`` for an unencrypted file.
        :raises PdfEncryptedError: The file is encrypted and cannot be opened.
        """
        encrypt = self.trailer.get("Encrypt")
        if encrypt is None:
            if self._index is not None and self._index.saw_encrypt and not self._trailer_found:
                raise PdfEncryptedError("this PDF looks encrypted, but its trailer is missing")
            return None
        if isinstance(encrypt, PdfRef):
            self._encrypt_ref = encrypt
        try:
            dictionary = self.resolve(encrypt)
            if not isinstance(dictionary, dict):
                raise PdfEncryptedError("this PDF's Encrypt dictionary is missing")
            dictionary = {key: self.resolve(value) for key, value in dictionary.items()}
            identifiers = self.resolve(self.trailer.get("ID"))
        except PdfEncryptedError:
            raise
        except PdfReadError as err:
            raise PdfEncryptedError(f"this PDF's Encrypt dictionary cannot be read: {err}") from err
        file_id = identifiers[0] if isinstance(identifiers, list) and identifiers and isinstance(identifiers[0], bytes) else b""
        handler = StandardSecurityHandler(dictionary, file_id)
        # Objects loaded before the handler existed are still ciphertext: forget them.
        self._cache = {ref: value for ref, value in self._cache.items() if ref == self._encrypt_ref}
        self._objstms.clear()
        self._compressed.clear()
        return handler

    # --- metadata ---------------------------------------------------------------------------------

    def text(self: Self, value: Any) -> str | list[str] | None:
        """
        Turn an Info value into text: strings decoded, names and numbers as written, arrays item by item.

        Example:
            >>> document = PdfDocument(minimal_pdf(b"<< >>"))
            >>> document.text(b"Caf\\xe9"), document.text(PdfName("Name")), document.text([b"a", 2])
            ('Café', 'Name', ['a', '2'])


        :param value: The value.
        :return: The text, a list of texts, or ``None`` for dictionaries, streams and nulls.
        """
        value = self.resolve(value)
        if isinstance(value, bytes):
            return decode_text(value)
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, (PdfName, int, float)):
            return str(value)
        if isinstance(value, list):
            items = []
            for item in value[:256]:
                text = self.text(item)
                if isinstance(text, str):
                    items.append(text)
            return items
        return None

    def info(self: Self) -> dict[str, str | list[str]]:
        """
        Read the Info dictionary as text.

        Example:
            >>> PdfDocument(minimal_pdf(b"<< /Title (T) /Pages 2 >>")).info()
            {'Title': 'T', 'Pages': '2'}


        :return: Its entries, without empty values.
        """
        dictionary = self.resolve(self.trailer.get("Info"))
        if not isinstance(dictionary, dict):
            return {}
        out: dict[str, str | list[str]] = {}
        for key, value in dictionary.items():
            text = self.text(value)
            if text:
                out[str(key)] = text
        return out

    def xmp(self: Self) -> bytes | None:
        """
        Read the catalog's metadata stream.

        Example:
            >>> PdfDocument(minimal_pdf(b"<< >>")).xmp() is None
            True


        :return: The decoded XMP packet, or ``None``.
        :raises PdfReadError: The stream cannot be decoded, or is larger than :attr:`PdfLimits.xmp_bytes`.
        """
        root = self.resolve(self.trailer.get("Root"))
        if not isinstance(root, dict):
            return None
        stream = self.resolve(root.get("Metadata"))
        if not isinstance(stream, PdfStream):
            return None
        data = self._decode(stream)
        if len(data) > self.limits.xmp_bytes:
            raise PdfReadError("the XMP packet is larger than the reading limit")
        return data

    def metadata(self: Self) -> PdfMetadata:
        """
        Read the Info dictionary and the XMP packet, keeping what can be read of each.

        Example:
            >>> PdfDocument(minimal_pdf(b"<< /Title (T) >>")).metadata().info
            {'Title': 'T'}


        :return: The metadata.
        :raises PdfEncryptedError: Decryption fails part way.
        """
        problems: list[str] = []
        try:
            info = self.info()
        except PdfEncryptedError:
            raise
        except PdfReadError as err:
            info = {}
            problems.append(f"Info dictionary: {err}")
        try:
            xmp = self.xmp()
        except PdfEncryptedError:
            raise
        except PdfReadError as err:
            xmp = None
            problems.append(f"XMP packet: {err}")
        return PdfMetadata(info, xmp, self.security is not None, self.version, tuple(problems))

    @property
    def repaired(self: Self) -> bool:
        """
        Tell whether the cross-reference data was wrong for what has been read, so a scan found it.

        A writer appending an update to such a file would chain it to the broken data.

        Example:
            >>> document = PdfDocument(minimal_pdf(b"<< /Title (T) >>"))
            >>> _ = document.metadata()
            >>> document.repaired
            False
            >>> broken = PdfDocument(minimal_pdf(b"<< /Title (T) >>").replace(b"startxref\\n", b"startxref\\n9"))
            >>> _ = broken.metadata()
            >>> broken.repaired
            True


        :return: Whether a scan was needed.
        """
        return self._index is not None


def _looks_like_info(value: Any) -> bool:
    """
    Tell whether a dictionary found by a scan could be an Info dictionary rather than an outline item or page.

    Example:
        >>> _looks_like_info({"Title": b"T"}), _looks_like_info({"Title": b"T", "Parent": 1})
        (True, False)


    :param value: The object.
    :return: Whether it has Info keys and none of the keys structural dictionaries have.
    """
    return isinstance(value, dict) and bool(value.keys() & _INFO_KEYS) and not value.keys() & _STRUCTURE_KEYS


def read_metadata(target: bytes | bytearray | memoryview | IO[bytes], *, limits: PdfLimits | None = None) -> PdfMetadata:
    """
    Read a PDF's Info dictionary and XMP packet.

    Example:
        >>> metadata = read_metadata(minimal_pdf(b"<< /Title <FEFF00540069> /Keywords [(a) (b)] >>"))
        >>> metadata.info
        {'Title': 'Ti', 'Keywords': ['a', 'b']}


    :param target: The PDF's bytes, or a seekable binary file the caller keeps open.
    :param limits: The bounds on work.
    :return: The metadata.
    :raises PdfReadError: The file is not a PDF or holds no readable objects.
    :raises PdfEncryptedError: The file is encrypted and cannot be opened with an empty password.
    """
    try:
        return PdfDocument(target, limits=limits).metadata()
    except PdfNeedsPoppler:
        # The aes plugin's poppler layer serves: pdfinfo decrypts the file itself.
        from LiuXin_alpha.file_formats.pdf.poppler import read_with_poppler

        return cast("PdfMetadata", read_with_poppler(target))
