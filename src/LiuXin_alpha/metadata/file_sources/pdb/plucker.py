# -*- coding: utf-8 -*-

from __future__ import unicode_literals, division, absolute_import, print_function

"""
Read meta information from Plucker pdb files.
"""

import struct
from datetime import datetime

from LiuXin.utils.localization import trans as _

from LiuXin.metadata.metadata import MetaData as MetaInformation

from LiuXin.file_formats.pdb.header import PdbHeaderReader
from LiuXin.file_formats.pdb.plucker.reader import (
    SectionHeader,
    DATATYPE_METADATA,
    MIBNUM_TO_NAME,
)

__license__ = "GPL v3"
__copyright__ = "2009, John Schember <john@nachtimwald.com>"
__docformat__ = "restructuredtext en"


def get_metadata(stream, extract_cover=True):
    """
    Return metadata as a L{MetaInfo} object
    :param stream:
    :param extract_cover:
    :return:
    """
    mi = MetaInformation(_("Unknown"), [_("Unknown")])
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

    default_encoding = "latin-1"
    (record_count,) = struct.unpack(">H", section_data[0:2])
    adv = 0
    title = None
    author = None
    pubdate = 0
    for i in xrange(record_count):
        (type,) = struct.unpack(">H", section_data[2 + adv : 4 + adv])
        (length,) = struct.unpack(">H", section_data[4 + adv : 6 + adv])

        # CharSet
        if type == 1:
            (val,) = struct.unpack(">H", section_data[6 + adv : 8 + adv])
            default_encoding = MIBNUM_TO_NAME.get(val, "latin-1")
        # Author
        elif type == 4:
            author = section_data[6 + adv + (2 * length)]
        # Title
        elif type == 5:
            title = section_data[6 + adv + (2 * length)]
        # Publication Date
        elif type == 6:
            (pubdate,) = struct.unpack(">I", section_data[6 + adv : 6 + adv + 4])

        adv += 2 * length

    if title:
        mi.title = title.replace("\0", "").decode(default_encoding, "replace")
    if author:
        author = author.replace("\0", "").decode(default_encoding, "replace")
        mi.author = author.split(",")
    # Todo: Add a validator at the metadata level to make sure that any dates entered are consistent
    mi.pubdate = datetime.fromtimestamp(pubdate)

    return mi
