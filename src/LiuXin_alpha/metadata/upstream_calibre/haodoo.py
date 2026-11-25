# License: GPLv3 Copyright: 2012, Kan-Ru Chen <kanru@kanru.info>
# Ported from calibre (https://calibre-ebook.com) 9.18: src/calibre/ebooks/metadata/haodoo.py
# Changes from upstream: calibre imports point at upstream_calibre and its _compat.
# SPDX-License-Identifier: GPL-3.0-only

"""
Read meta information from Haodoo.net pdb files.
"""

from typing import IO, TYPE_CHECKING

from LiuXin_alpha.metadata.upstream_calibre.pdb_haodoo_reader import Reader
from LiuXin_alpha.metadata.upstream_calibre.pdb_header import PdbHeaderReader

if TYPE_CHECKING:
    from LiuXin_alpha.metadata.upstream_calibre.book_base import Metadata


def get_metadata(stream: IO[bytes], extract_cover: bool = True) -> Metadata:
    """
    Return metadata as a L{MetaInfo} object
    """
    stream.seek(0)

    pheader = PdbHeaderReader(stream)
    reader = Reader(pheader, stream, None, None)

    return reader.get_metadata()
