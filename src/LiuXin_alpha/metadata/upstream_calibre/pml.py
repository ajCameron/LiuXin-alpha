# License: GPLv3 Copyright: 2009, John Schember <john@nachtimwald.com>
# Ported from calibre (https://calibre-ebook.com) 9.18: src/calibre/ebooks/metadata/pml.py
# Changes from upstream: calibre imports point at upstream_calibre and its _compat, and
# 'from __future__ import annotations' is added.
# SPDX-License-Identifier: GPL-3.0-only

"""
Read meta information from TXT files
"""

from __future__ import annotations

import glob
import os
import re
from typing import IO, TYPE_CHECKING

from LiuXin_alpha.metadata.upstream_calibre._compat import prepare_string_for_xml
from LiuXin_alpha.metadata.upstream_calibre.metadata_base import MetaInformation
from LiuXin_alpha.metadata.upstream_calibre._compat import TemporaryDirectory
from LiuXin_alpha.metadata.upstream_calibre._compat import _
from LiuXin_alpha.metadata.upstream_calibre._compat import ZipFile

if TYPE_CHECKING:
    from LiuXin_alpha.metadata.upstream_calibre.book_base import Metadata


def get_metadata(stream: IO[bytes], extract_cover: bool = True) -> Metadata:
    """Return metadata as a L{MetaInfo} object"""
    mi = MetaInformation(_('Unknown'), [_('Unknown')])
    stream.seek(0)

    pml = b''
    if stream.name.endswith('.pmlz'):
        with TemporaryDirectory('_unpmlz') as tdir:
            zf = ZipFile(stream)
            zf.extractall(tdir)

            pmls = glob.glob(os.path.join(tdir, '*.pml'))
            for p in pmls:
                with open(p, 'r+b') as p_stream:
                    pml += p_stream.read()
            if extract_cover:
                mi.cover_data = get_cover(os.path.splitext(os.path.basename(stream.name))[0], tdir, True)
    else:
        pml = stream.read()
        if extract_cover:
            mi.cover_data = get_cover(os.path.splitext(os.path.basename(stream.name))[0], os.path.abspath(os.path.dirname(stream.name)))

    for comment in re.findall(rb'(?ms)\\v.*?\\v', pml):
        m = re.search(rb'TITLE="(.*?)"', comment)
        if m:
            mi.title = re.sub(r'[\x00-\x1f]', '', prepare_string_for_xml(m.group(1).strip().decode('cp1252', 'replace')))
        m = re.search(rb'AUTHOR="(.*?)"', comment)
        if m:
            if mi.authors == [_('Unknown')]:
                mi.authors = []
            mi.authors.append(re.sub(r'[\x00-\x1f]', '', prepare_string_for_xml(m.group(1).strip().decode('cp1252', 'replace'))))
        m = re.search(rb'PUBLISHER="(.*?)"', comment)
        if m:
            mi.publisher = re.sub(r'[\x00-\x1f]', '', prepare_string_for_xml(m.group(1).strip().decode('cp1252', 'replace')))
        m = re.search(rb'COPYRIGHT="(.*?)"', comment)
        if m:
            mi.rights = re.sub(r'[\x00-\x1f]', '', prepare_string_for_xml(m.group(1).strip().decode('cp1252', 'replace')))
        m = re.search(rb'ISBN="(.*?)"', comment)
        if m:
            mi.isbn = re.sub(r'[\x00-\x1f]', '', prepare_string_for_xml(m.group(1).strip().decode('cp1252', 'replace')))

    return mi


def get_cover(name: str, tdir: str, top_level: bool = False) -> tuple[str, bytes | None]:
    cover_path = ''
    cover_data = None

    if top_level:
        cover_path = os.path.join(tdir, 'cover.png') if os.path.exists(os.path.join(tdir, 'cover.png')) else ''
    if not cover_path:
        cover_path = (
            os.path.join(tdir, name + '_img', 'cover.png')
            if os.path.exists(os.path.join(tdir, name + '_img', 'cover.png'))
            else os.path.join(os.path.join(tdir, 'images'), 'cover.png')
            if os.path.exists(os.path.join(os.path.join(tdir, 'images'), 'cover.png'))
            else ''
        )
    if cover_path:
        with open(cover_path, 'rb') as cstream:
            cover_data = cstream.read()

    return ('png', cover_data)
