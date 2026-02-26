from __future__ import annotations

import os
import time
from typing import Iterator, Tuple

from LiuXin_alpha.constants import (
    __appname__,
    __version__,
    filesystem_encoding,
    force_unicode,
    preferred_encoding,
)
from LiuXin_alpha.utils.which_os import isosx
from LiuXin_alpha.utils.storage.local import CurrentDir
from LiuXin_alpha.utils.storage.local.filenames import sanitize_file_name
from LiuXin_alpha.utils.mine_types import guess_type
from LiuXin_alpha.utils.date import strftime
from LiuXin_alpha.utils.text import as_unicode, entity_to_unicode
from LiuXin_alpha.utils.text.xml_utils import (
    _ent_pat,
    prepare_string_for_xml,
    replace_entities,
    xml_entity_to_unicode,
    xml_replace_entities,
)


def isbytestring(obj) -> bool:
    # Legacy calibre compatibility (Py2-era code often treats text as string-like).
    return isinstance(obj, (str, bytes))


def walk(path: str) -> Iterator[str]:
    for root, _, files in os.walk(path):
        for name in files:
            yield os.path.join(root, name)


def fit_image(owidth: int, oheight: int, max_width: int, max_height: int) -> Tuple[bool, int, int]:
    if owidth <= 0 or oheight <= 0:
        return False, max(0, int(max_width)), max(0, int(max_height))
    max_width = int(max_width)
    max_height = int(max_height)
    if owidth <= max_width and oheight <= max_height:
        return False, int(owidth), int(oheight)
    ratio = min(max_width / float(owidth), max_height / float(oheight))
    nw = max(1, int(round(owidth * ratio)))
    nh = max(1, int(round(oheight * ratio)))
    return True, nw, nh


def setup_cli_handlers(logger=None):
    return None


def filename_to_utf8(name):
    if isinstance(name, bytes):
        return name.decode("utf-8", "replace")
    return str(name)

