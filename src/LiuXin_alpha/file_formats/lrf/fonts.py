from __future__ import annotations

import typing as _typing
__license__ = "GPL v3"
__copyright__ = "2008, Kovid Goyal <kovid at kovidgoyal.net>"

try:
    from PIL import ImageFont
except Exception:
    ImageFont = None

from LiuXin_alpha.utils.resources import P

"""
Default fonts used in the PRS500
"""


LIBERATION_FONT_MAP = {
    "Swis721 BT Roman": "LiberationSans-Regular",
    "Dutch801 Rm BT Roman": "LiberationSerif-Regular",
    "Courier10 BT Roman": "LiberationMono-Regular",
}

FONT_FILE_MAP = {}


def get_font(name: _typing.Any, size: _typing.Any, encoding: str = "unic") -> _typing.Any:
    """
    Get an ImageFont object by name.
    @param size: Font height in pixels. To convert from pts:
                 sz in pixels = (dpi/72) * size in pts
    @param encoding: Font encoding to use. E.g. 'unic', 'symbol', 'ADOB', 'ADBE', 'aprm'
    @param manager: A dict that will store the PersistentTemporary
    """
    if ImageFont is None:
        raise RuntimeError("Pillow is required for LRF font rasterization")
    if name in LIBERATION_FONT_MAP:
        return ImageFont.truetype(
            P("fonts/liberation/%s.ttf" % LIBERATION_FONT_MAP[name]),
            size,
            encoding=encoding,
        )
    elif name in FONT_FILE_MAP:
        return ImageFont.truetype(FONT_FILE_MAP[name], size, encoding=encoding)
