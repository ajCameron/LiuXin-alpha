#!/usr/bin/env python
# License: GPLv3 Copyright: 2008, Kovid Goyal <kovid at kovidgoyal.net>
# Ported from calibre (https://calibre-ebook.com) 9.18: src/calibre/ebooks/mobi/__init__.py
# Changes from upstream: calibre imports point at upstream_calibre and its _compat, and
# 'from __future__ import annotations' is added.
# SPDX-License-Identifier: GPL-3.0-only


from __future__ import annotations

class MobiError(Exception):
    pass


# That might be a bit small on the PW, but Amazon/KG 2.5 still uses these values, even when delivered to a PW
MAX_THUMB_SIZE = 16 * 1024
MAX_THUMB_DIMEN = (180, 240)
