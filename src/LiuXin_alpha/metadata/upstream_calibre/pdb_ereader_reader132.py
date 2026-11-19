# License: GPLv3 Copyright: 2009, John Schember <john@nachtimwald.com>
# Ported from calibre (https://calibre-ebook.com) 9.18: src/calibre/ebooks/pdb/ereader/reader132.py
# Changes from upstream: calibre imports point at upstream_calibre and its _compat, and
# 'from __future__ import annotations' is added.
# Only HeaderRecord is ported, with the imports it uses;
# the rest is conversion code the metadata readers do not use.
# SPDX-License-Identifier: GPL-3.0-only

"""
Read content from ereader pdb file with a 132 byte header created by Dropbook.
"""

from __future__ import annotations

import struct


class HeaderRecord:
    """
    The first record in the file is always the header record. It holds
    information related to the location of text, images, and so on
    in the file. This is used in conjunction with the sections
    defined in the file header.
    """

    def __init__(self, raw: bytes) -> None:
        (self.compression,) = struct.unpack('>H', raw[0:2])
        (self.non_text_offset,) = struct.unpack('>H', raw[12:14])
        (self.chapter_count,) = struct.unpack('>H', raw[14:16])
        (self.image_count,) = struct.unpack('>H', raw[20:22])
        (self.link_count,) = struct.unpack('>H', raw[22:24])
        (self.has_metadata,) = struct.unpack('>H', raw[24:26])
        (self.footnote_count,) = struct.unpack('>H', raw[28:30])
        (self.sidebar_count,) = struct.unpack('>H', raw[30:32])
        (self.chapter_offset,) = struct.unpack('>H', raw[32:34])
        (self.small_font_page_offset,) = struct.unpack('>H', raw[36:38])
        (self.large_font_page_offset,) = struct.unpack('>H', raw[38:40])
        (self.image_data_offset,) = struct.unpack('>H', raw[40:42])
        (self.link_offset,) = struct.unpack('>H', raw[42:44])
        (self.metadata_offset,) = struct.unpack('>H', raw[44:46])
        (self.footnote_offset,) = struct.unpack('>H', raw[48:50])
        (self.sidebar_offset,) = struct.unpack('>H', raw[50:52])
        (self.last_data_offset,) = struct.unpack('>H', raw[52:54])

        self.num_text_pages = self.non_text_offset - 1
        self.num_image_pages = self.metadata_offset - self.image_data_offset
