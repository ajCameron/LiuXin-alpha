# -*- coding: utf-8 -*-

"""
Read meta information from pdb files.
"""

import re

from LiuXin.file_formats.pdb.header import PdbHeaderReader

from LiuXin.metadata.file_sources.pdb.ereader import get_metadata as get_eReader
from LiuXin.metadata.file_sources.pdb.plucker import get_metadata as get_plucker
from LiuXin.metadata.file_sources.pdb.haodoo import get_metadata as get_Haodoo
from LiuXin.metadata.file_sources.pdb.ereader import set_metadata as set_eReader
from LiuXin.metadata.metadata import MetaData as MetaInformation

from LiuXin.utils.localization import trans as _

from past.builtins import basestring


__license__ = "GPL v3"
__copyright__ = "2009, John Schember <john@nachtimwald.com>"
__docformat__ = "restructuredtext en"


# Keyed with the pheader ident and valued with the reader needed to get the metadata from the file
MREADER = {
    "PNPdPPrs": get_eReader,
    "PNRdPPrs": get_eReader,
    "DataPlkr": get_plucker,
    "BOOKMTIT": get_Haodoo,
    "BOOKMTIU": get_Haodoo,
}


# Keyed with the pheader ident and valued with the writer needed to write the metadata out to file
MWRITER = {
    "PNPdPPrs": set_eReader,
    "PNRdPPrs": set_eReader,
}


def get_metadata(stream, extract_cover=True):
    """
    Return metadata as a L{MetaInfo} object.
    Metadata can only be read from certain types of pdb files. In particular 'PNPdPPrs', 'PNRdPPrs', 'DataPlkr',
    'BOOKMTIT' and 'BOOKMTIU'.
    If data can't be read from that type of file then fall back on reading the title - which can be done for all file
    types (as it's included in the header) and set the author to unknown.
    Check the header type using the get_pheader_ident method. If the header ident isn't in the list above then full
    metadata read won't happen.
    :param stream: The PDB encoded stream to extract the metadata from
    :param extract_cover: Should the cover be included in the returned metadata?
    """
    stream_needs_close = False
    if isinstance(stream, basestring):
        stream_needs_close = True
        stream = open(stream, "rb")

    try:
        pheader = PdbHeaderReader(stream)

        MetadataReader = MREADER.get(pheader.ident, None)

        # Falls back on pulling the title out, if nothing else can be read
        if MetadataReader is None:
            return MetaInformation(pheader.title, [_("Unknown")])
        else:
            return MetadataReader(stream, extract_cover)
    finally:
        if stream_needs_close:
            stream.close()


def get_pheader_ident(stream):
    """
    Return the pheader ident for the given pdb stream.
    :param stream: The book the metadata will be written from
    :return:
    """
    stream.seek(0)

    pheader = PdbHeaderReader(stream)

    return pheader.ident


def set_metadata(stream, mi):
    """
    Write the given MetaInformation mi into the given stream.
    Metadata can only be written to certain types of pdb files. In particular 'PNPdPPrs' and 'PNRdPPrs'.
    Check the header type using the get_pheader_ident method. If the header ident isn't in the list above then full
    metadata write won't happen.
    :param stream: The book the metadata will be written into
    :param mi: The metadata for writing
    :return:
    """
    stream.seek(0)

    pheader = PdbHeaderReader(stream)

    MetadataWriter = MWRITER.get(pheader.ident, None)

    if MetadataWriter:
        MetadataWriter(stream, mi)

    stream.seek(0)
    stream.write("%s\x00" % re.sub("[^-A-Za-z0-9 ]+", "_", mi.title).ljust(31, "\x00")[:31].encode("ascii", "replace"))
