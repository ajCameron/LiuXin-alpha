# License: GPLv3 Copyright: 2009, John Schember <john@nachtimwald.com>
# Ported from calibre (https://calibre-ebook.com) 9.18: src/calibre/ebooks/metadata/plucker.py
# Changes from upstream: calibre imports point at upstream_calibre and its _compat.
# SPDX-License-Identifier: GPL-3.0-only

"""
Read meta information from Plucker pdb files.
"""

import struct
from datetime import datetime
from typing import IO, TYPE_CHECKING

from LiuXin_alpha.metadata.upstream_calibre.metadata_base import MetaInformation
from LiuXin_alpha.metadata.upstream_calibre.pdb_header import PdbHeaderReader
from LiuXin_alpha.metadata.upstream_calibre.pdb_plucker_reader import DATATYPE_METADATA
from LiuXin_alpha.metadata.upstream_calibre.pdb_plucker_reader import MIBNUM_TO_NAME
from LiuXin_alpha.metadata.upstream_calibre.pdb_plucker_reader import SectionHeader
from LiuXin_alpha.metadata.upstream_calibre.date import local_tz
from LiuXin_alpha.metadata.upstream_calibre._compat import _

if TYPE_CHECKING:
    from LiuXin_alpha.metadata.upstream_calibre.book_base import Metadata


def get_metadata(stream: IO[bytes], extract_cover: bool = True) -> Metadata:
    """
    Return metadata as a L{MetaInfo} object
    """
    mi = MetaInformation(_('Unknown'), [_('Unknown')])
    stream.seek(0)

    pheader = PdbHeaderReader(stream)
    section_data = None
    for i in range(1, pheader.num_sections):
        raw_data = pheader.section_data(i)
        section_header = SectionHeader(raw_data)
        if section_header.type == DATATYPE_METADATA:
            section_data = raw_data[8:]
            break

    if not section_data:
        return mi

    default_encoding = 'latin-1'
    (record_count,) = struct.unpack('>H', section_data[0:2])
    adv = 0
    title = None
    author = None
    pubdate = 0
    for i in range(record_count):
        try:
            type, length = struct.unpack_from('>HH', section_data, 2 + adv)
        except struct.error:
            break

        # CharSet
        if type == 1:
            (val,) = struct.unpack('>H', section_data[6 + adv : 8 + adv])
            default_encoding = MIBNUM_TO_NAME.get(val, 'latin-1')
        # Author
        elif type == 4:
            author = section_data[6 + adv : 6 + adv + (2 * length)]
        # Title
        elif type == 5:
            title = section_data[6 + adv : 6 + adv + (2 * length)]
        # Publication Date
        elif type == 6:
            (pubdate,) = struct.unpack('>I', section_data[6 + adv : 6 + adv + 4])

        # Each record is a 4 byte header (type, length) followed by length 16-bit words of data
        adv += 4 + 2 * length

    if title:
        mi.title = title.replace(b'\0', b'').decode(default_encoding, 'replace')
    if author:
        mi.authors = author.replace(b'\0', b'').decode(default_encoding, 'replace').split(',')
    mi.pubdate = datetime.fromtimestamp(pubdate, tz=local_tz)

    return mi
