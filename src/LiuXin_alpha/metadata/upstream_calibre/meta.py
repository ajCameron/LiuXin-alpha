#!/usr/bin/env python
# License: GPLv3 Copyright: 2008, Kovid Goyal <kovid at kovidgoyal.net>
# Ported from calibre (https://calibre-ebook.com) 9.18: src/calibre/ebooks/metadata/meta.py
# Changes from upstream: only opf_metadata is ported, which calibre's OPF 2 tests use;
# calibre imports point at upstream_calibre and its _compat; and
# 'from __future__ import annotations' is added.
# SPDX-License-Identifier: GPL-3.0-only
"""
Read an OPF file's metadata and cover, as calibre's ``meta`` module does.

Example:
    >>> import os, tempfile
    >>> with tempfile.TemporaryDirectory() as folder:
    ...     path = os.path.join(folder, "plain.opf")
    ...     with open(path, "w") as f:
    ...         _ = f.write('<package xmlns="http://www.idpf.org/2007/opf" version="2.0"/>')
    ...     opf_metadata(path) is None
    True
"""

from __future__ import annotations

import os
from typing import IO

from LiuXin_alpha.metadata.upstream_calibre._compat import StreamOrPath, is_existing_subpath
from LiuXin_alpha.metadata.upstream_calibre.book_base import Metadata
from LiuXin_alpha.metadata.upstream_calibre.opf2 import OPF


def opf_metadata(opfpath: str | IO[bytes]) -> Metadata | None:
    try:
        with StreamOrPath(opfpath, 'rb') as f:
            opfpath = getattr(f, 'name', os.getcwd())
            opf = OPF(f, os.path.dirname(opfpath))
            if opf.application_id is not None:
                mi = opf.to_book_metadata()
                if hasattr(opf, 'cover') and opf.cover:
                    base = os.path.dirname(opfpath)
                    cpath = os.path.join(base, opf.cover)
                    # Only read covers inside the OPF's directory, to prevent
                    # a malicious OPF from reading arbitrary files
                    if is_existing_subpath(cpath, base) and os.access(cpath, os.R_OK):
                        fmt = cpath.rpartition('.')[-1]
                        with open(cpath, 'rb') as f:
                            data = f.read()
                        mi.cover_data = (fmt, data)
                return mi
    except Exception:
        import traceback

        traceback.print_exc()
