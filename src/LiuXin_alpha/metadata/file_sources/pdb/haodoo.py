# -*- coding: utf-8 -*-

"""
Read meta information from Haodoo.net pdb files.
"""

from LiuXin.file_formats.pdb.header import PdbHeaderReader
from LiuXin.file_formats.pdb.haodoo.reader import Reader

__license__ = "GPL v3"
__copyright__ = "2012, Kan-Ru Chen <kanru@kanru.info>"
__docformat__ = "restructuredtext en"


def get_metadata(stream, extract_cover=True):
    """
    Return metadata as a L{MetaInfo} object
    :param stream:
    :param extract_cover:
    :return:
    """
    stream.seek(0)

    pheader = PdbHeaderReader(stream)
    reader = Reader(pheader, stream, None, None)

    # Todo: Check that the reader returns a correct MetaData object
    return reader.get_metadata()
