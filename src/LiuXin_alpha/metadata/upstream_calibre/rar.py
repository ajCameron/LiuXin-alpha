#!/usr/bin/env python
# License: GPLv3 Copyright: 2009, Kovid Goyal kovid@kovidgoyal.net
# Ported from calibre (https://calibre-ebook.com) 9.18: src/calibre/ebooks/metadata/rar.py
# Changes from upstream: calibre imports point at upstream_calibre and its _compat, and
# 'from __future__ import annotations' is added.
# calibre.utils.unrar, which wraps the unrardll extension, is _unrar, on rarfile.
# SPDX-License-Identifier: GPL-3.0-only

"""
Read metadata from RAR archives
"""

from __future__ import annotations

import os
from io import BytesIO
from typing import IO, TYPE_CHECKING

from LiuXin_alpha.metadata.upstream_calibre._unrar import extract_member
from LiuXin_alpha.metadata.upstream_calibre._unrar import names

if TYPE_CHECKING:
    from LiuXin_alpha.metadata.upstream_calibre.book_base import Metadata


def get_metadata(stream: IO[bytes]) -> Metadata:
    from LiuXin_alpha.metadata.upstream_calibre.archive import is_comic
    from LiuXin_alpha.metadata.upstream_calibre.meta import get_metadata

    file_names = list(names(stream))
    if is_comic(file_names):
        return get_metadata(stream, 'cbr')
    for f in file_names:
        stream_type = os.path.splitext(f)[1].lower()
        if stream_type:
            stream_type = stream_type[1:]
            if stream_type in {
                'lit',
                'opf',
                'prc',
                'mobi',
                'fb2',
                'epub',
                'rb',
                'imp',
                'pdf',
                'lrf',
                'azw',
                'azw1',
                'azw3',
            }:
                res = extract_member(stream, match=None, name=f)
                if res is None:
                    raise KeyError(f'There is no file named {f} in the archive')
                name, data = res
                stream = BytesIO(data)
                stream.name = os.path.basename(name)
                mi = get_metadata(stream, stream_type)
                mi.timestamp = None
                return mi
    raise ValueError('No ebook found in RAR archive')
