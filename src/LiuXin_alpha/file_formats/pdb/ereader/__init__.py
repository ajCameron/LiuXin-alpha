# -*- coding: utf-8 -*-

from __future__ import annotations

import typing as _typing
import os

from LiuXin_alpha.utils.libraries.liuxin_six import six_string_types

__license__ = "GPL v3"
__copyright__ = "2009, John Schember <john@nachtimwald.com>"
__docformat__ = "restructuredtext en"


class EreaderError(Exception):
    pass


def image_name(name: _typing.Any, taken_names: tuple[_typing.Any, ...] = ()) -> _typing.Any:
    if isinstance(name, bytes):
        name = name.decode("ascii", "ignore")
    elif not isinstance(name, six_string_types):
        name = str(name)

    name = name.replace("\x00", "").replace("\\", "/").strip()
    name = os.path.basename(name)
    if name in ("", ".", ".."):
        name = "image.png"

    if len(name) > 32:
        cut = len(name) - 32
        names = name[:10]
        namee = name[10 + cut :]
        name = "%s%s.png" % (names, namee)

    taken = set()
    for item in taken_names:
        if isinstance(item, bytes):
            item = item.decode("ascii", "ignore")
        elif not isinstance(item, six_string_types):
            item = str(item)
        taken.add(item.replace("\x00", ""))

    base = name
    root, ext = os.path.splitext(base)
    suffix = 1
    while name in taken:
        marker = str(suffix)
        max_root = max(1, 32 - len(ext) - len(marker))
        name = "%s%s%s" % (root[:max_root], marker, ext)
        suffix += 1

    name = name.ljust(32, "\x00")[:32]

    return name
