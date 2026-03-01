from __future__ import with_statement

"""
Read meta information from SNB files
"""

import os
from lxml import etree

from LiuXin.file_formats.snb.snbfile import SNBFile

from LiuXin.metadata.metadata import MetaData as MetaInformation

from LiuXin.utils.localization import trans as _

# Py2/Py3 compatibility layer
from LiuXin.utils.lx_libraries.liuxin_six import six_cStringIO as StringIO

__license__ = "GPL v3"
__copyright__ = "2010, Li Fanxi <lifanxi@freemindworld.com>"


def get_metadata(stream, extract_cover=True):
    """
    Return metadata as a L{MetaInfo} object.
    :param stream:
    :param extract_cover:
    :return:
    """
    mi = MetaInformation(_("Unknown"), [_("Unknown")])
    snbFile = SNBFile()

    try:
        if not hasattr(stream, "write"):
            snbFile.Parse(StringIO(stream), True)
        else:
            stream.seek(0)
            snbFile.Parse(stream, True)

        meta = snbFile.GetFileStream("snbf/book.snbf")

        if meta != None:
            meta = etree.fromstring(meta)
            mi.title = meta.find(".//head/name").text
            mi.authors = [meta.find(".//head/author").text]
            mi.language = meta.find(".//head/language").text.lower().replace("_", "-")
            mi.publisher = meta.find(".//head/publisher").text

            if extract_cover:
                cover = meta.find(".//head/cover")
                if cover != None and cover.text != None:
                    root, ext = os.path.splitext(cover.text)
                    if ext == ".jpeg":
                        ext = ".jpg"
                    mi.cover_data = (
                        ext[-3:],
                        snbFile.GetFileStream("snbc/images/" + cover.text),
                    )

    except Exception:
        import traceback

        traceback.print_exc()

    return mi
