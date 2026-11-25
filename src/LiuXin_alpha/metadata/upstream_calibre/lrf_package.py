# License: GPLv3 Copyright: 2008, Kovid Goyal <kovid at kovidgoyal.net>
# Ported from calibre (https://calibre-ebook.com) 9.18: src/calibre/ebooks/lrf/__init__.py
# Changes from upstream: calibre imports point at upstream_calibre and its _compat.
# Only LRFParseError and PRS500_PROFILE are ported, with the imports they use;
# the rest is conversion code the metadata readers do not use.
# SPDX-License-Identifier: GPL-3.0-only

"""
This package contains logic to read and write LRF files.
The LRF file format is documented at U{http://www.sven.de/librie/Librie/LrfFormat}.
"""


class LRFParseError(Exception):
    pass


class PRS500_PROFILE:
    screen_width = 600
    screen_height = 775
    dpi = 166
    # Number of pixels to subtract from screen_height when calculating height of text area
    fudge = 0
    font_size = 10  #: Default (in pt)
    parindent = 10  #: Default (in pt)
    line_space = 1.2  # : Default (in pt)
    header_font_size = 6  #: In pt
    header_height = 30  # : In px
    default_fonts = {'sans': 'Swis721 BT Roman', 'mono': 'Courier10 BT Roman', 'serif': 'Dutch801 Rm BT Roman'}

    name = 'prs500'
