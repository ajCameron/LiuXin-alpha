# License: GPLv3 Copyright: 2009, John Schember <john@nachtimwald.com>
# Ported from calibre (https://calibre-ebook.com) 9.18: src/calibre/ebooks/metadata/txt.py
# Changes from upstream: calibre imports point at upstream_calibre and its _compat, and
# 'from __future__ import annotations' is added. SPDX-License-Identifier: GPL-3.0-only

"""
Read meta information from TXT files
"""

from __future__ import annotations

import os
import re
from typing import IO, TYPE_CHECKING

from LiuXin_alpha.metadata.upstream_calibre.metadata_base import MetaInformation
from LiuXin_alpha.metadata.upstream_calibre._compat import _

if TYPE_CHECKING:
    from LiuXin_alpha.metadata.upstream_calibre.book_base import Metadata


def get_metadata(stream: IO[bytes], extract_cover: bool = True) -> Metadata:
    """
    Return metadata as a L{MetaInfo} object
    """
    name = getattr(stream, 'name', '').rpartition('.')[0]
    if name:
        name = os.path.basename(name)
    mi = MetaInformation(name or _('Unknown'), [_('Unknown')])
    stream.seek(0)

    mdata = ''
    for x in range(4):
        line = stream.readline().decode('utf-8', 'replace')
        if not line:
            break
        else:
            mdata += line

    mdata = mdata[:1024]

    mo = re.search(r'(?u)^[ ]*(?P<title>.+)[ ]*(\n{3}|(\r\n){3}|\r{3})[ ]*(?P<author>.+)[ ]*(\n|\r\n|\r)$', mdata)
    if mo is not None:
        mi.title = mo.group('title')
        mi.authors = mo.group('author').split(',')

    return mi
