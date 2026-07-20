#!/usr/bin/env python
# vim:fileencoding=utf-8

from __future__ import unicode_literals, division, absolute_import, print_function
from __future__ import annotations

import typing as _typing

from LiuXin_alpha.utils.image_tools.imghdr import what

from struct import unpack_from, error

try:
    from LiuXin_alpha.utils.wrappers.magick.draw import identify_data
except (ImportError, RuntimeError) as e:
    try:
        from LiuXin_alpha.utils.plugins.fallbacks.magick import Image as _FallbackImage
    except Exception:
        _FallbackImage = None

    def identify_data(data: _typing.Any) -> tuple[_typing.Any, ...]:
        if _FallbackImage is None:
            raise RuntimeError("No image identify backend is available")
        meta = _FallbackImage(data).identify()
        return meta.get("width", 0), meta.get("height", 0), meta.get("format", "unknown")

__license__ = "GPL v3"
__copyright__ = "2014, Kovid Goyal <kovid at kovidgoyal.net>"


def find_imgtype(data: _typing.Any) -> _typing.Any:
    imgtype = what(None, data)
    if imgtype is None:
        try:
            imgtype = identify_data(data)[2]
        except Exception:
            imgtype = "unknown"
    return imgtype


class Container(object):
    def __init__(self: _typing.Self, data: _typing.Any) -> None:
        self.is_image_container = False
        self.resource_index = 0

        if len(data) > 60 and data[48:52] == b"EXTH":
            length, num_items = unpack_from(b">LL", data, 52)
            pos = 60
            while pos < 60 + length - 8:
                try:
                    idx, size = unpack_from(b">LL", data, pos)
                except error:
                    break
                pos += 8
                size -= 8
                if size < 0:
                    break
                if idx == 539:
                    self.is_image_container = data[pos : pos + size] == b"application/image"
                    break
                pos += size

    def load_image(self: _typing.Self, data: _typing.Any) -> tuple[_typing.Any, ...]:
        self.resource_index += 1
        if self.is_image_container:
            data = data[12:]
            imgtype = find_imgtype(data)
            if imgtype != "unknown":
                return data, imgtype
        return None, None
