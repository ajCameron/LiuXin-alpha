#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai

"""
Compress and decompress PalmDOC byte streams with bounded backreferences.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise palmdoc through a consuming regression::

        python -m pytest -q tests/file_formats/compression/test_compression_modernized.py
"""

from __future__ import annotations

import typing as _typing

from struct import pack
from collections.abc import Callable
from typing import Protocol, TypeAlias, cast

from LiuXin_alpha.utils.logging import default_log
from LiuXin_alpha.utils.plugins import plugins

from LiuXin_alpha.utils.libraries.liuxin_six import six_BytesIO

__license__ = "GPL v3"
__copyright__ = "2008, Kovid Goyal <kovid at kovidgoyal.net>"


PalmDocInput: TypeAlias = str | bytes | bytearray | memoryview | None


class _PalmDocCodec(Protocol):
    """
    Provide the palmdoccodec contract for validated ebook processing.

    Example:
        Exercise  PalmDocCodec through a consuming regression::

            python -m pytest -q tests/file_formats/compression/test_compression_modernized.py
    """
    def compress(self: _typing.Self, data: bytes) -> bytes:
        """
        Encode the supplied bytes into the format's compressed representation.

        Example:
            Exercise  PalmDocCodec.compress through a consuming regression::

                python -m pytest -q tests/file_formats/compression/test_compression_modernized.py


        :param data: Value supplied for data under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...

    def decompress(self: _typing.Self, data: bytes) -> bytes:
        """
        Decode the supplied format payload and return its uncompressed bytes.

        Example:
            Exercise  PalmDocCodec.decompress through a consuming regression::

                python -m pytest -q tests/file_formats/compression/test_compression_modernized.py


        :param data: Value supplied for data under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        ...


class _PalmDocModuleAdapter:
    """
    Give compiled or Python plugin modules one checked codec interface.

    Example:
        Exercise  PalmDocModuleAdapter through a consuming regression::

            python -m pytest -q tests/file_formats/compression/test_compression_modernized.py
    """

    def __init__(self: _typing.Self, module: object) -> None:
        """
        Initialize and validate the palmdocmoduleadapter state.

        Example:
            Exercise  PalmDocModuleAdapter.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/compression/test_compression_modernized.py


        :param module: Value supplied for module under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self._compress = cast(
            Callable[[bytes], bytes],
            getattr(module, "compress"),
        )
        self._decompress = cast(
            Callable[[bytes], bytes],
            getattr(module, "decompress"),
        )

    def compress(self: _typing.Self, data: bytes) -> bytes:
        """
        Encode the supplied bytes into the format's compressed representation.

        Example:
            Exercise  PalmDocModuleAdapter.compress through a consuming regression::

                python -m pytest -q tests/file_formats/compression/test_compression_modernized.py


        :param data: Value supplied for data under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self._compress(data)

    def decompress(self: _typing.Self, data: bytes) -> bytes:
        """
        Decode the supplied format payload and return its uncompressed bytes.

        Example:
            Exercise  PalmDocModuleAdapter.decompress through a consuming regression::

                python -m pytest -q tests/file_formats/compression/test_compression_modernized.py


        :param data: Value supplied for data under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self._decompress(data)


def _as_bytes(data: PalmDocInput) -> bytes:
    """
    Perform the as bytes operation under explicit file-format and conversion rules.

    Example:
        Exercise  as bytes through a consuming regression::

            python -m pytest -q tests/file_formats/compression/test_compression_modernized.py


    :param data: Value supplied for data under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if data is None:
        return b""
    if isinstance(data, bytes):
        return data
    if isinstance(data, bytearray):
        return bytes(data)
    if isinstance(data, memoryview):
        return data.tobytes()
    if isinstance(data, str):
        # Keep Py2-era behavior where text often represented raw 0..255 bytes.
        return data.encode("latin-1", "replace")
    raise TypeError(f"Expected bytes-like data, got {type(data)!r}")


def _load_cpalmdoc() -> _PalmDocCodec:
    """
    Perform the load cpalmdoc operation under explicit file-format and conversion rules.

    Example:
        Exercise  load cpalmdoc through a consuming regression::

            python -m pytest -q tests/file_formats/compression/test_compression_modernized.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if plugins.plugin_okay("cPalmdoc"):
        module, _err = plugins["cPalmdoc"]
        if module is not None:
            return _PalmDocModuleAdapter(module)

    default_log.info("cPalmdoc plugin unavailable, using bundled Python fallback.")
    from LiuXin_alpha.utils.plugins.fallbacks import cPalmdoc

    return _PalmDocModuleAdapter(cPalmdoc)


_CPALMDOC = _load_cpalmdoc()


def decompress_doc(data: PalmDocInput) -> bytes:
    """
    Decompress PalmDOC data into raw bytes.

    Example:
        Exercise decompress doc through a consuming regression::

            python -m pytest -q tests/file_formats/compression/test_compression_modernized.py


    :param data: Value supplied for data under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return _CPALMDOC.decompress(_as_bytes(data))


def compress_doc(data: PalmDocInput) -> bytes:
    """
    Compress raw bytes into PalmDOC format.

    Example:
        Exercise compress doc through a consuming regression::

            python -m pytest -q tests/file_formats/compression/test_compression_modernized.py


    :param data: Value supplied for data under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    payload = _as_bytes(data)
    if not payload:
        return b""
    return _CPALMDOC.compress(payload)


def py_compress_doc(data: PalmDocInput) -> bytes:
    """
    Pure-python PalmDOC compressor (reference implementation).

    Example:
        Exercise py compress doc through a consuming regression::

            python -m pytest -q tests/file_formats/compression/test_compression_modernized.py


    :param data: Value supplied for data under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    data = _as_bytes(data)
    out = six_BytesIO()
    i = 0
    ldata = len(data)
    while i < ldata:
        if i > 10 and (ldata - i) > 10:
            chunk = b""
            match = -1
            for j in range(10, 2, -1):
                chunk = data[i : i + j]
                try:
                    match = data.rindex(chunk, 0, i)
                except ValueError:
                    continue
                if (i - match) <= 2047:
                    break
                match = -1
            if match >= 0:
                n = len(chunk)
                m = i - match
                code = 0x8000 + ((m << 3) & 0x3FF8) + (n - 3)
                out.write(pack(">H", code))
                i += n
                continue

        ch = data[i : i + 1]
        och = ch[0]
        i += 1

        if ch == b" " and (i + 1) < ldata:
            onch = data[i : i + 1][0]
            if 0x40 <= onch < 0x80:
                out.write(pack(">B", onch ^ 0x80))
                i += 1
                continue

        if och == 0 or (8 < och < 0x80):
            out.write(ch)
        else:
            j = i
            binseq = [ch]
            while j < ldata and len(binseq) < 8:
                ch = data[j : j + 1]
                och = ch[0]
                if och == 0 or (8 < och < 0x80):
                    break
                binseq.append(ch)
                j += 1
            out.write(pack(">B", len(binseq)))
            out.write(b"".join(binseq))
            i += len(binseq) - 1

    return out.getvalue()


def test() -> None:
    """
    Perform the test operation under explicit file-format and conversion rules.

    Example:
        Exercise test through a consuming regression::

            python -m pytest -q tests/file_formats/compression/test_compression_modernized.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    tests: list[bytes] = [
        b"abc\x03\x04\x05\x06ms",
        b"a b c \xfed ",
        b"0123456789axyz2bxyz2cdfgfo9iuyerh",
        b"0123456789asd0123456789asd|yyzzxxffhhjjkk",
        (
            b"ciewacnaq eiu743 r787q 0w%  ; sa fd\xef\ffdxosac wocjp acoiecowei "
            b"owaic jociowapjcivcjpoivjporeivjpoavca; p9aw8743y6r74%$^$^%8 "
        ),
    ]

    for case in tests:
        good = py_compress_doc(case)
        actual = compress_doc(case)
        assert actual == good
        assert decompress_doc(actual) == case
