# License: GPLv3 Copyright: 2008, Ashish Kulkarni <kulkarni.ashish@gmail.com>
# Ported from calibre (https://calibre-ebook.com) 9.18: src/calibre/ebooks/metadata/rb.py
# Changes from upstream: calibre imports point at upstream_calibre and its _compat.
# SPDX-License-Identifier: GPL-3.0-only

"""Read meta information from RB files"""

import struct
import sys
from typing import IO, TYPE_CHECKING

from LiuXin_alpha.metadata.upstream_calibre._compat import prints
from LiuXin_alpha.metadata.upstream_calibre.metadata_base import MetaInformation, string_to_authors

if TYPE_CHECKING:
    from LiuXin_alpha.metadata.upstream_calibre.book_base import Metadata

MAGIC = b'\xb0\x0c\xb0\x0c\x02\x00NUVO\x00\x00\x00\x00'


def get_metadata(stream: IO[bytes]) -> Metadata:
    """Return metadata as a L{MetaInfo} object"""
    title = 'Unknown'
    mi = MetaInformation(title, ['Unknown'])
    stream.seek(0)
    try:
        if stream.read(14) != MAGIC:
            print("Couldn't read RB header from file", file=sys.stderr)
            return mi
        stream.read(10)

        def read_i32() -> int:
            return struct.unpack('<I', stream.read(4))[0]

        stream.seek(read_i32())
        toc_count = read_i32()

        for i in range(toc_count):
            stream.read(32)
            length, offset, flag = read_i32(), read_i32(), read_i32()
            if flag == 2:
                break
        else:
            print("Couldn't find INFO from RB file", file=sys.stderr)
            return mi

        stream.seek(offset)
        info = stream.read(length).decode('utf-8', 'replace').splitlines()
        for line in info:
            if '=' not in line:
                continue
            key, value = line.split('=')
            if key.strip() == 'TITLE':
                mi.title = value.strip()
            elif key.strip() == 'AUTHOR':
                mi.authors = string_to_authors(value)
    except Exception as err:
        msg = f"Couldn't read metadata from rb: {mi.title} with error {err!s}"
        prints(msg, file=sys.stderr)
        raise
    return mi
