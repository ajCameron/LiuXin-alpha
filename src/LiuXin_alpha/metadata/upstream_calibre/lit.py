# License: GPLv3 Copyright: 2008, Kovid Goyal <kovid at kovidgoyal.net>
# Ported from calibre (https://calibre-ebook.com) 9.18: src/calibre/ebooks/metadata/lit.py
# Changes from upstream: calibre imports point at upstream_calibre and its _compat.
# SPDX-License-Identifier: GPL-3.0-only

"""
Support for reading the metadata from a LIT file.
"""

import io
import os
from typing import IO, TYPE_CHECKING

from LiuXin_alpha.metadata.upstream_calibre.opf2 import OPF

if TYPE_CHECKING:
    from LiuXin_alpha.metadata.upstream_calibre.book_base import Metadata


def get_metadata(stream: IO[bytes]) -> Metadata:
    from LiuXin_alpha.metadata.upstream_calibre.lit_reader import LitContainer
    from LiuXin_alpha.metadata.upstream_calibre._compat import Log

    litfile = LitContainer(stream, Log())
    src = litfile.get_metadata().encode('utf-8')
    litfile = litfile._litfile
    opf = OPF(io.BytesIO(src), os.getcwd())
    mi = opf.to_book_metadata()
    covers = []
    for item in opf.iterguide():
        if 'cover' not in item.get('type', '').lower():
            continue
        ctype = item.get('type', '')  # always present, see the check above
        href = item.get('href', '')
        candidates = [href, href.replace('&', '%26')]
        for item in litfile.manifest.values():
            if item.path in candidates:
                try:
                    covers.append((litfile.get_file('/data/' + item.internal), ctype))
                except Exception:
                    pass
                break
    if covers:
        covers.sort(key=lambda x: len(x[0]), reverse=True)
        idx = 0
        if len(covers) > 1:
            if covers[1][1] == covers[0][1] + '-standard':
                idx = 1
        mi.cover_data = ('jpg', covers[idx][0])
    return mi
