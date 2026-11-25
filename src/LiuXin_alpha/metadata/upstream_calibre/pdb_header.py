# License: GPLv3 Copyright: 2009, John Schember <john@nachtimwald.com>
# Ported from calibre (https://calibre-ebook.com) 9.18: src/calibre/ebooks/pdb/header.py
# Changes from upstream: calibre imports point at upstream_calibre and its _compat.
# LiuXin fixes are marked '# LiuXin:'.
# SPDX-License-Identifier: GPL-3.0-only

"""
Read the header data from a pdb file.
"""

import re
import struct
import time
from typing import IO, TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence


class PdbHeaderReader:
    def __init__(self, stream: IO[bytes]) -> None:
        self.stream = stream
        self.ident = self.identity()
        self.num_sections = self.section_count()
        self.title = self.name()

    def identity(self) -> str:
        self.stream.seek(60)
        ident = self.stream.read(8)
        return ident.decode('utf-8')

    def section_count(self) -> int:
        self.stream.seek(76)
        return struct.unpack('>H', self.stream.read(2))[0]

    def name(self) -> bytes:
        self.stream.seek(0)
        return re.sub(rb'[^-A-Za-z0-9 ]+', b'_', self.stream.read(32).replace(b'\x00', b''))

    def full_section_info(self, number: int) -> tuple[int, int, int]:
        if not (0 <= number < self.num_sections):
            raise ValueError(f'Not a valid section number {number}')

        self.stream.seek(78 + number * 8)
        offset, a1, a2, a3, a4 = struct.unpack('>LBBBB', self.stream.read(8))
        flags, val = a1, a2 << 16 | a3 << 8 | a4
        return offset, flags, val

    def section_offset(self, number: int) -> int:
        if not (0 <= number < self.num_sections):
            raise ValueError(f'Not a valid section number {number}')

        self.stream.seek(78 + number * 8)
        return struct.unpack('>LBBBB', self.stream.read(8))[0]

    def check_sections(self) -> None:  # LiuXin:
        # LiuXin: a section runs to the next one's offset, and when that offset
        # goes back, read() of the negative length reads to the end of the file.
        # A table whose offsets go back and forth names the whole file once per
        # section, up to 65,535 times, and the Haodoo reader keeps every
        # section. Refuse sections that together hold more than twice the file,
        # as the MOBI reader does.
        if getattr(self, 'sections_checked', False):
            return
        self.stream.seek(0, 2)
        size = self.stream.tell()
        self.stream.seek(78)
        table = self.stream.read(8 * self.num_sections)
        offsets = [struct.unpack_from('>L', table, 8 * i)[0] for i in range(len(table) // 8)]
        held = 0
        for i, start in enumerate(offsets):
            end = offsets[i + 1] if i + 1 < len(offsets) else size
            held += (size if end < start else min(end, size)) - min(start, size)
        if held > 2 * size:
            raise ValueError('PDB sections overlap')
        self.sections_checked = True

    def section_data(self, number: int) -> bytes:
        if not (0 <= number < self.num_sections):
            raise ValueError(f'Not a valid section number {number}')
        self.check_sections()  # LiuXin:

        start = self.section_offset(number)
        if number == self.num_sections - 1:
            self.stream.seek(0, 2)
            end = self.stream.tell()
        else:
            end = self.section_offset(number + 1)
        self.stream.seek(start)
        return self.stream.read(end - start)


class PdbHeaderBuilder:
    def __init__(self, identity: str, title: str | bytes) -> None:
        self.identity = identity.ljust(3, '\x00')[:8].encode('utf-8')
        if isinstance(title, str):
            title = title.encode('ascii', 'replace')
        self.title = b'%s\x00' % re.sub(rb'[^-A-Za-z0-9 ]+', b'_', title).ljust(31, b'\x00')[:31]

    def build_header(self, section_lengths: Sequence[int], out_stream: IO[bytes]) -> None:
        """
        section_lengths = Length of each section in file.
        """

        now = int(time.time())
        nrecords = len(section_lengths)

        out_stream.write(self.title + struct.pack('>HHIIIIII', 0, 0, now, now, 0, 0, 0, 0))
        out_stream.write(self.identity + struct.pack('>IIH', nrecords, 0, nrecords))

        offset = 78 + (8 * nrecords) + 2
        for id, record in enumerate(section_lengths):
            out_stream.write(struct.pack('>LBBBB', int(offset), 0, 0, 0, 0))
            offset += record
        out_stream.write(b'\x00\x00')
