#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai

from __future__ import unicode_literals, division, absolute_import, print_function
from __future__ import annotations

import typing as _typing

"""
Decompress MOBI files compressed with the Huff/cdic algorithm. Code thanks to darkninja
and igorsk.
"""

import struct

from LiuXin_alpha.file_formats.mobi import MobiError

__license__ = "GPL v3"
__copyright__ = "2011, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"


def _require_bytes(data: _typing.Any, length: _typing.Any, context: _typing.Any) -> None:
    if len(data) < length:
        raise MobiError("Truncated %s" % context)


class Reader(object):
    def __init__(self: _typing.Self) -> None:
        self.q = struct.Struct(b">Q").unpack_from
        self.dict1 = ()
        self.mincode = ()
        self.maxcode = ()
        self.dictionary = []

    def load_huff(self: _typing.Self, huff: _typing.Any) -> None:
        _require_bytes(huff, 16, "HUFF header")
        if huff[0:8] != b"HUFF\x00\x00\x00\x18":
            raise MobiError("Invalid HUFF header")
        off1, off2 = struct.unpack_from(b">LL", huff, 8)
        _require_bytes(huff, off1 + 256 * 4, "HUFF dictionary table")
        _require_bytes(huff, off2 + 64 * 4, "HUFF code table")

        def dict1_unpack(v: _typing.Any) -> tuple[_typing.Any, ...]:
            local_codelen, term, local_maxcode = v & 0x1F, v & 0x80, v >> 8
            if local_codelen == 0:
                raise MobiError("Invalid HUFF code length")
            if local_codelen <= 8:
                if not term:
                    raise MobiError("Invalid HUFF terminal marker")
            local_maxcode = ((local_maxcode + 1) << (32 - local_codelen)) - 1
            return local_codelen, term, local_maxcode

        self.dict1 = tuple(map(dict1_unpack, struct.unpack_from(b">256L", huff, off1)))

        dict2 = struct.unpack_from(b">64L", huff, off2)
        self.mincode, self.maxcode = (), ()
        for codelen, mincode in enumerate((0,) + dict2[0::2]):
            self.mincode += (mincode << (32 - codelen),)
        for codelen, maxcode in enumerate((0,) + dict2[1::2]):
            self.maxcode += (((maxcode + 1) << (32 - codelen)) - 1,)

        self.dictionary = []

    def load_cdic(self: _typing.Self, cdic: _typing.Any) -> None:
        _require_bytes(cdic, 16, "CDIC header")
        if cdic[0:8] != b"CDIC\x00\x00\x00\x10":
            raise MobiError("Invalid CDIC header")
        phrases, bits = struct.unpack_from(b">LL", cdic, 8)
        if bits > 16:
            raise MobiError("Invalid CDIC bit width")
        if phrases < len(self.dictionary):
            raise MobiError("Invalid CDIC phrase count")
        n = min(1 << bits, phrases - len(self.dictionary))
        _require_bytes(cdic, 16 + 2 * n, "CDIC offset table")
        h = struct.Struct(b">H").unpack_from

        def getslice(off: _typing.Any) -> tuple[_typing.Any, ...]:
            _require_bytes(cdic, 16 + off + 2, "CDIC phrase length")
            (blen,) = h(cdic, 16 + off)
            _require_bytes(cdic, 18 + off + (blen & 0x7FFF), "CDIC phrase data")
            local_slice = cdic[18 + off : 18 + off + (blen & 0x7FFF)]
            return local_slice, blen & 0x8000

        offsets = struct.unpack_from(b">%dH" % n, cdic, 16) if n else ()
        self.dictionary += map(getslice, offsets)

    def unpack(self: _typing.Self, data: _typing.Any, max_output_size: _typing.Any = None) -> _typing.Any:
        if not self.dict1 or not self.dictionary:
            raise MobiError("HUFF/CDIC tables are not loaded")
        q = self.q

        bitsleft = len(data) * 8
        data += b"\x00\x00\x00\x00\x00\x00\x00\x00"
        pos = 0
        (x,) = q(data, pos)
        n = 32

        s = []
        output_size = 0
        while True:
            if n <= 0:
                pos += 4
                (x,) = q(data, pos)
                n += 32
            code = (x >> n) & ((1 << 32) - 1)

            codelen, term, maxcode = self.dict1[code >> 24]
            if not term:
                while code < self.mincode[codelen]:
                    codelen += 1
                    if codelen >= len(self.mincode):
                        raise MobiError("Invalid HUFF code")
                maxcode = self.maxcode[codelen]

            n -= codelen
            bitsleft -= codelen
            if bitsleft < 0:
                break

            r = (maxcode - code) >> (32 - codelen)
            try:
                slice_, flag = self.dictionary[r]
            except (IndexError, TypeError):
                raise MobiError("HUFF dictionary reference is out of range")
            if not flag:
                self.dictionary[r] = None
                slice_ = self.unpack(slice_, max_output_size=max_output_size)
                self.dictionary[r] = (slice_, 1)
            output_size += len(slice_)
            if max_output_size is not None and output_size > max_output_size:
                raise MobiError(
                    "HUFF/CDIC text record expands beyond limit: %d > %d bytes"
                    % (output_size, max_output_size)
                )
            s.append(slice_)
        return b"".join(s)


class HuffReader(object):
    def __init__(self: _typing.Self, huffs: _typing.Any, max_output_size: _typing.Any = None) -> None:
        if not huffs:
            raise MobiError("Missing HUFF record")
        self.max_output_size = max_output_size
        self.reader = Reader()
        self.reader.load_huff(huffs[0])
        for cdic in huffs[1:]:
            self.reader.load_cdic(cdic)

    def unpack(self: _typing.Self, section: _typing.Any) -> _typing.Any:
        return self.reader.unpack(section, max_output_size=self.max_output_size)
