#!/usr/bin/env  python

"""
Read metadata from RAR archives
"""

import os
from io import BytesIO

from LiuXin.utils.decompression.unrar import extract_member, names

__license__ = "GPL v3"
__copyright__ = "2009, Kovid Goyal kovid@kovidgoyal.net"
__docformat__ = "restructuredtext en"


def get_metadata(stream):
    """
    Read metadata from a rar file.
    Looks for an ebook in the compressed file - if there is one reads the metadata from it.
    Files will be tested in
    :param stream:
    :return:
    """

    from LiuXin.file_formats.compression.compressed_ebooks import is_comic
    from LiuXin.metadata.meta import get_metadata

    file_names = list(names(stream))
    if is_comic(file_names):
        return get_metadata(stream, "cbr")
    for f in file_names:
        stream_type = os.path.splitext(f)[1].lower()
        if stream_type:
            stream_type = stream_type[1:]
            # Todo: This should be pulled from the constants file somewhere
            if stream_type in {
                "lit",
                "opf",
                "prc",
                "mobi",
                "fb2",
                "epub",
                "rb",
                "imp",
                "pdf",
                "lrf",
                "azw",
                "azw1",
                "azw3",
            }:
                name, data = extract_member(stream, match=None, name=f)
                stream = BytesIO(data)
                stream.name = os.path.basename(name)
                mi = get_metadata(stream, stream_type)
                mi.timestamp = None
                return mi
    raise ValueError("No ebook found in RAR archive")
