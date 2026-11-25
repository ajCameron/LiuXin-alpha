#!/usr/bin/env python
# License: GPLv3 Copyright: 2008, Kovid Goyal kovid@kovidgoyal.net
# Ported from calibre (https://calibre-ebook.com) 9.18: src/calibre/libunzip.py
# Changes from upstream: calibre imports point at upstream_calibre and its _compat.
# Only extract, sort_key, extract_member, comic_exts, name_ok and extract_cover_image are ported, with the imports they use;
# the rest is conversion code the metadata readers do not use.
# ICU's numeric collation is _compat.numeric_sort_key, a natural sort on Python's case folding;
# calibre's zipfile is Python's, which reads archives alike.
# SPDX-License-Identifier: GPL-3.0-only

import re
from typing import TYPE_CHECKING

import zipfile
from LiuXin_alpha.metadata.upstream_calibre._compat import numeric_sort_key

if TYPE_CHECKING:
    from typing import Any as ZipStream


def extract(filename: str | ZipStream, dir: str) -> None:
    """
    Extract archive C{filename} into directory C{dir}
    """
    zf = zipfile.ZipFile(filename)
    zf.extractall(dir)


def sort_key(filename: str) -> tuple[bytes, bytes]:
    bn, ext = filename.rpartition('.')[::2]
    if not bn and ext:
        bn, ext = ext, bn
    return (numeric_sort_key(bn), numeric_sort_key(ext))


def extract_member(
    filename: str | ZipStream, match: re.Pattern[str] = re.compile(r'\.(jpg|jpeg|gif|png)\s*$', re.I), sort_alphabetically: bool = False
) -> tuple[str, bytes] | None:
    zf = zipfile.ZipFile(filename)
    names = list(zf.namelist())
    if sort_alphabetically:
        names.sort(key=sort_key)
    for name in names:
        if match.search(name):
            return name, zf.read(name)


comic_exts = {'png', 'jpg', 'jpeg', 'gif', 'webp', 'avif', 'jxl'}


def name_ok(name: str) -> bool:
    return bool(name and not name.startswith('__MACOSX/') and name.rpartition('.')[-1].lower() in comic_exts)


def extract_cover_image(filename: str | ZipStream) -> tuple[str, bytes] | None:
    with zipfile.ZipFile(filename) as zf:
        for name in sorted(zf.namelist(), key=sort_key):
            if name_ok(name):
                return name, zf.read(name)
