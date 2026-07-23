# -*- coding: utf-8 -*-

"""TCR compressor/decompressor."""

from __future__ import annotations

import typing as _typing

import io
import re
from typing import BinaryIO, TypeAlias

__license__ = "GPL 3"
__copyright__ = "2009, John Schember <john@nachtimwald.com>"
__docformat__ = "restructuredtext en"

TCRInput: TypeAlias = str | bytes | bytearray | memoryview


def _int_to_byte(value: int) -> bytes:
    return bytes([value & 0xFF])


def _to_bytes(data: TCRInput) -> bytes:
    if isinstance(data, bytes):
        return data
    if isinstance(data, bytearray):
        return bytes(data)
    if isinstance(data, memoryview):
        return data.tobytes()
    if isinstance(data, str):
        return data.encode("utf-8")
    raise TypeError(f"Unsupported payload type: {type(data)!r}")


class TCRCompressor:
    """Encode byte content into TCR format."""

    def __init__(self: _typing.Self) -> None:
        self.unused_codes: set[int]
        self.coded_txt: bytes
        self.codes: list[bytes]
        self._reset()

    def _reset(self: _typing.Self) -> None:
        self.unused_codes = set()
        self.coded_txt = b""
        self.codes = []

    def _combine_codes(self: _typing.Self) -> None:
        possible_codes = []
        a_code = set(re.findall(br"(?ms).", self.coded_txt))

        for code in sorted(a_code):
            single_code = set(re.findall(b"(?ms)%s." % re.escape(code), self.coded_txt))
            if len(single_code) == 1:
                possible_codes.append(single_code.pop())

        for code in sorted(possible_codes):
            self.coded_txt = self.coded_txt.replace(code, code[0:1])
            self.codes[code[0]] = b"%s%s" % (self.codes[code[0]], self.codes[code[1]])

    def _free_unused_codes(self: _typing.Self) -> None:
        for i in range(256):
            if i not in self.unused_codes and _int_to_byte(i) not in self.coded_txt:
                self.unused_codes.add(i)

    def _new_codes(self: _typing.Self) -> list[bytes]:
        possible_new_codes = sorted(set(re.findall(br"(?ms)..", self.coded_txt)))
        new_codes_count = []

        for c in possible_new_codes:
            count = self.coded_txt.count(c)
            if count > 2:
                new_codes_count.append((c, count))

        return [x[0] for x in sorted(new_codes_count, key=lambda local_c: (local_c[1], local_c[0]))]

    def compress(self: _typing.Self, txt: TCRInput) -> bytes:
        txt = _to_bytes(txt)
        self._reset()

        self.codes = sorted(set(re.findall(br"(?ms).", txt)))

        index_by_code = {code[0]: i for i, code in enumerate(self.codes)}
        encoded_bytes = bytearray()
        for c in txt:
            encoded_bytes.append(index_by_code[c])
        self.coded_txt = bytes(encoded_bytes)

        for i in range(len(self.codes), 256):
            self.codes.append(b"")
            self.unused_codes.add(i)

        self._combine_codes()
        possible_codes = self._new_codes()

        while possible_codes and self.unused_codes:
            while possible_codes and self.unused_codes:
                unused_code = min(self.unused_codes)
                self.unused_codes.remove(unused_code)
                code = possible_codes.pop()
                self.codes[unused_code] = b"%s%s" % (
                    self.codes[ord(code[0:1])],
                    self.codes[ord(code[1:2])],
                )
                self.coded_txt = self.coded_txt.replace(code, _int_to_byte(unused_code))
            self._combine_codes()
            self._free_unused_codes()
            possible_codes = self._new_codes()

        self._free_unused_codes()

        code_dict = []
        for i in range(256):
            if i in self.unused_codes:
                code_dict.append(b"\0")
            else:
                code_dict.append(_int_to_byte(len(self.codes[i])) + self.codes[i])

        return b"!!8-Bit!!" + b"".join(code_dict) + self.coded_txt


def decompress(stream: BinaryIO) -> bytes:
    """Decompress a TCR stream into bytes."""
    stream.seek(0)
    if stream.read(9) != b"!!8-Bit!!":
        name = getattr(stream, "name", "<stream>")
        raise ValueError(f"File {name} contains an invalid TCR header.")

    entries = []
    for i in range(256):
        entry_len_raw = stream.read(1)
        if len(entry_len_raw) != 1:
            raise ValueError(f"TCR dictionary is truncated at entry {i}.")
        entry_len = entry_len_raw[0]
        entry = stream.read(entry_len)
        if len(entry) != entry_len:
            raise ValueError(f"TCR dictionary entry {i} payload is truncated.")
        entries.append(entry)

    txt = []
    entry_loc = stream.read(1)
    while entry_loc != b"":
        txt.append(entries[entry_loc[0]])
        entry_loc = stream.read(1)

    return b"".join(txt)


def compress(txt: TCRInput) -> bytes:
    payload = _to_bytes(txt)
    encoded = TCRCompressor().compress(payload)
    # The historical algorithm occasionally produces non-reversible output for
    # some repetitive streams. Guard by falling back to an identity dictionary.
    if decompress(io.BytesIO(encoded)) == payload:
        return encoded

    present = set(payload)
    identity_dict = b"".join(
        (_int_to_byte(1) + _int_to_byte(i)) if i in present else b"\0" for i in range(256)
    )
    return b"!!8-Bit!!" + identity_dict + payload
