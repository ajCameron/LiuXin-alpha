#!/usr/bin/env python
# License: GPLv3 Copyright: 2013, Kovid Goyal <kovid at kovidgoyal.net>
# Ported from calibre (https://calibre-ebook.com) 9.18: src/calibre/ebooks/docx/__init__.py
# Changes from upstream: calibre imports point at upstream_calibre and its _compat.
# SPDX-License-Identifier: GPL-3.0-only


class InvalidDOCX(ValueError):
    pass
