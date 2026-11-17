# License: GPLv3 Copyright: 2008, Ashish Kulkarni <kulkarni.ashish@gmail.com>
# Ported from calibre (https://calibre-ebook.com) 9.18: src/calibre/ebooks/metadata/imp.py
# Changes from upstream: calibre imports point at upstream_calibre._compat, and
# 'from __future__ import annotations' is added. SPDX-License-Identifier: GPL-3.0-only

"""Read meta information from IMP files"""

from __future__ import annotations

import sys
from typing import IO, TYPE_CHECKING

from LiuXin_alpha.metadata.upstream_calibre._compat import MetaInformation, string_to_authors

if TYPE_CHECKING:
    from LiuXin_alpha.metadata.upstream_calibre._compat import Metadata

MAGIC = (b'\x00\x01BOOKDOUG', b'\x00\x02BOOKDOUG')


def get_metadata(stream: IO[bytes]) -> Metadata:
    """Return metadata as a L{MetaInfo} object"""
    title = 'Unknown'
    mi = MetaInformation(title, ['Unknown'])
    stream.seek(0)
    try:
        if stream.read(10) not in MAGIC:
            print("Couldn't read IMP header from file", file=sys.stderr)
            return mi

        def cString(skip: int = 0) -> str:
            result = b''
            while 1:
                data = stream.read(1)
                if data == b'\x00':
                    if not skip:
                        return result.decode('utf-8')
                    skip -= 1
                    result, data = b'', b''
                result += data

        stream.read(38)  # skip past some uninteresting headers
        cString()
        category, title, author = cString(), cString(1), cString(2)

        if title:
            mi.title = title
        if author:
            mi.authors = string_to_authors(author)
            mi.author = author
        if category:
            mi.category = category
    except Exception as err:
        msg = f"Couldn't read metadata from imp: {mi.title} with error {err!s}"
        print(msg.encode('utf8'), file=sys.stderr)
    return mi
