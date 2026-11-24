# License: GPLv3 Copyright: 2009, John Schember <john@nachtimwald.com>
# Ported from calibre (https://calibre-ebook.com) 9.18: src/calibre/ebooks/metadata/pdb.py
# Changes from upstream: calibre imports point at upstream_calibre and its _compat.
# SPDX-License-Identifier: GPL-3.0-only

"""
Read meta information from pdb files.
"""

import re
from typing import IO, TYPE_CHECKING

from LiuXin_alpha.metadata.upstream_calibre.metadata_base import MetaInformation
from LiuXin_alpha.metadata.upstream_calibre.ereader import get_metadata as get_eReader
from LiuXin_alpha.metadata.upstream_calibre.haodoo import get_metadata as get_Haodoo
from LiuXin_alpha.metadata.upstream_calibre.plucker import get_metadata as get_plucker
from LiuXin_alpha.metadata.upstream_calibre.pdb_header import PdbHeaderReader
from LiuXin_alpha.metadata.upstream_calibre._compat import _

if TYPE_CHECKING:
    from LiuXin_alpha.metadata.upstream_calibre.book_base import Metadata

MREADER = {
    'PNPdPPrs': get_eReader,
    'PNRdPPrs': get_eReader,
    'DataPlkr': get_plucker,
    'BOOKMTIT': get_Haodoo,
    'BOOKMTIU': get_Haodoo,
}

from LiuXin_alpha.metadata.upstream_calibre.ereader import set_metadata as set_eReader

MWRITER = {
    'PNPdPPrs': set_eReader,
    'PNRdPPrs': set_eReader,
}


def get_metadata(stream: IO[bytes], extract_cover: bool = True) -> Metadata:
    """
    Return metadata as a L{MetaInfo} object
    """

    pheader = PdbHeaderReader(stream)

    MetadataReader = MREADER.get(pheader.ident, None)

    if MetadataReader is None:
        t = pheader.title
        if isinstance(t, bytes):
            t = t.decode('utf-8', 'replace')
        return MetaInformation(t, [_('Unknown')])

    return MetadataReader(stream, extract_cover)


def set_metadata(stream: IO[bytes], mi: Metadata) -> None:
    stream.seek(0)

    pheader = PdbHeaderReader(stream)

    MetadataWriter = MWRITER.get(pheader.ident, None)

    if MetadataWriter:
        MetadataWriter(stream, mi)

    stream.seek(0)
    stream.write(re.sub(r'[^-A-Za-z0-9 ]+', '_', mi.title).ljust(31, '\x00')[:31].encode('ascii', 'replace') + b'\x00')
