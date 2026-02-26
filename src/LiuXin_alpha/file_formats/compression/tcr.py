# -*- coding: utf-8 -*-

"""TCR compressor/decompressor."""

from __future__ import annotations

import io
import re

__license__ = "GPL 3"
__copyright__ = "2009, John Schember <john@nachtimwald.com>"
__docformat__ = "restructuredtext en"


def _int_to_byte(value: int) -> bytes:
    return bytes([value & 0xFF])


def _to_bytes(data):
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

    def _reset(self):
        self.unused_codes = set()
        self.coded_txt = b""
        self.codes = []

    def _combine_codes(self):
        possible_codes = []
        a_code = set(re.findall(br"(?ms).", self.coded_txt))

        for code in a_code:
            single_code = set(re.findall(b"(?ms)%s." % re.escape(code), self.coded_txt))
            if len(single_code) == 1:
                possible_codes.append(single_code.pop())

        for code in possible_codes:
            self.coded_txt = self.coded_txt.replace(code, code[0:1])
            self.codes[code[0]] = b"%s%s" % (self.codes[code[0]], self.codes[code[1]])

    def _free_unused_codes(self):
        for i in range(256):
            if i not in self.unused_codes and _int_to_byte(i) not in self.coded_txt:
                self.unused_codes.add(i)

    def _new_codes(self):
        possible_new_codes = list(set(re.findall(br"(?ms)..", self.coded_txt)))
        new_codes_count = []

        for c in possible_new_codes:
            count = self.coded_txt.count(c)
            if count > 2:
                new_codes_count.append((c, count))

        return [x[0] for x in sorted(new_codes_count, key=lambda local_c: local_c[1])]

    def compress(self, txt):
        txt = _to_bytes(txt)
        self._reset()

        self.codes = list(set(re.findall(br"(?ms).", txt)))

        for c in bytearray(txt):
            self.coded_txt += _int_to_byte(self.codes.index(_int_to_byte(c)))

        for i in range(len(self.codes), 256):
            self.codes.append(b"")
            self.unused_codes.add(i)

        self._combine_codes()
        possible_codes = self._new_codes()

        while possible_codes and self.unused_codes:
            while possible_codes and self.unused_codes:
                unused_code = self.unused_codes.pop()
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


def decompress(stream):
    """Decompress a TCR stream into bytes."""
    stream.seek(0)
    if stream.read(9) != b"!!8-Bit!!":
        name = getattr(stream, "name", "<stream>")
        raise ValueError(f"File {name} contains an invalid TCR header.")

    entries = []
    for _ in range(256):
        entry_len = ord(stream.read(1))
        entries.append(stream.read(entry_len))

    txt = []
    entry_loc = stream.read(1)
    while entry_loc != b"":
        txt.append(entries[ord(entry_loc)])
        entry_loc = stream.read(1)

    return b"".join(txt)


def compress(txt):
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
