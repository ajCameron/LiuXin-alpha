# License: GPLv3 Copyright: 2008, Kovid Goyal <kovid at kovidgoyal.net>
# Ported from calibre (https://calibre-ebook.com) 9.18: ComicMetadataReader.get_metadata
# in src/calibre/customize/builtins.py, by hand, as a function: the plugin's
# site_customization is the series_index argument. calibre imports point at
# upstream_calibre; calibre.utils.unrar is upstream_calibre._unrar.
# SPDX-License-Identifier: GPL-3.0-only
"""
Read a comic's cover and ComicBookInfo metadata, as calibre's comic reader plugin does.

The cover is the first image in natural name order. The rest comes from the
ComicBookInfo JSON a CBZ or CBR may hold in its archive comment. A CBC is a ZIP of
comics listing them in ``comics.txt``; the first is read. The file's own first bytes
decide its kind, whatever its extension says.

Example:
    >>> import io, zipfile
    >>> buffer = io.BytesIO()
    >>> with zipfile.ZipFile(buffer, "w") as zf:
    ...     zf.writestr("page10.png", b"ten")
    ...     zf.writestr("page2.png", b"two")
    >>> get_metadata(buffer, "cbz").cover_data
    ('png', b'two')
"""

import os
from typing import IO

from LiuXin_alpha.metadata.upstream_calibre.archive import get_comic_metadata
from LiuXin_alpha.metadata.upstream_calibre.book_base import Metadata
from LiuXin_alpha.metadata.upstream_calibre.metadata_base import MetaInformation


def get_metadata(
    stream: IO[bytes], ftype: str, series_index: str = "volume"
) -> Metadata:
    """
    Read a CBZ, CBR, CB7 or CBC.

    Example:
        See the module.


    :param stream: The comic.
    :param ftype: Its file type: ``cbz``, ``cbr``, ``cb7`` or ``cbc``.
    :param series_index: Which ComicBookInfo number is the series index: ``volume``
        (calibre's default) or ``issue``.
    :return: The metadata, with the cover as ``cover_data``.
    """
    type = ftype
    if type == "cbc":
        from zipfile import ZipFile

        zf = ZipFile(stream)
        fcn = zf.open("comics.txt").read().decode("utf-8").splitlines()[0]
        oname = getattr(stream, "name", None)
        stream = zf.open(fcn)  # type: ignore[assignment]
        type = fcn.split(".")[-1].lower()
        if oname:
            stream.name = oname
    if hasattr(stream, "seek") and hasattr(stream, "tell"):
        pos = stream.tell()
        id_ = stream.read(3)
        stream.seek(pos)
        if id_ == b"Rar":
            type = "cbr"
        elif id_.startswith(b"PK"):
            type = "cbz"
        elif id_.startswith(b"7z"):
            type = "cb7"
    if type == "cbr":
        from LiuXin_alpha.metadata.upstream_calibre._unrar import (
            extract_cover_image,  # type: ignore[no-redef]
        )
    elif type == "cb7":
        from LiuXin_alpha.metadata.upstream_calibre.seven_zip import (
            extract_cover_image,  # type: ignore[no-redef]
        )
    else:
        from LiuXin_alpha.metadata.upstream_calibre.libunzip import extract_cover_image

    ret = extract_cover_image(stream)
    mi = MetaInformation(None, None)
    stream.seek(0)
    if type in {"cbr", "cbz"}:
        if series_index not in {"volume", "issue"}:
            series_index = "volume"
        try:
            mi.smart_update(get_comic_metadata(stream, type, series_index=series_index))
        except Exception:
            pass
    if ret is not None:
        path, data = ret
        ext = os.path.splitext(path)[1][1:]
        mi.cover_data = (ext.lower(), data)
    return mi
