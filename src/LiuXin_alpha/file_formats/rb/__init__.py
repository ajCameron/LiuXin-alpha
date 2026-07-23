# -*- coding: utf-8 -*-

from __future__ import annotations

import typing as _typing
import os
from collections.abc import Collection

__license__ = "GPL 3"
__copyright__ = "2009, John Schember <john@nachtimwald.com>"
__docformat__ = "restructuredtext en"

HEADER = b"\xb0\x0c\xb0\x0c\x02\x00NUVO\x00\x00\x00\x00"


class RocketBookError(Exception):
    pass


def unique_name(name: str, used_names: Collection[str]) -> str:
    name = os.path.basename(name)
    if len(name) < 32 and name not in used_names:
        return name

    ext = os.path.splitext(name)[1].lstrip(".")[:3]
    base_name = os.path.splitext(name)[0][:22]
    for i in range(0, 9999):
        suffix = str(i).rjust(4, "0")[:4]
        if ext:
            candidate = "%s-%s.%s" % (suffix, base_name, ext)
        else:
            candidate = "%s-%s" % (suffix, base_name)
        if candidate not in used_names:
            return candidate

    raise ValueError("Unable to allocate a unique RocketBook filename")
