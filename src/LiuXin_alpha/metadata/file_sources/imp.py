import sys

from LiuXin.metadata.metadata import MetaInformation
from LiuXin.metadata.ebook_metadata_tools import string_to_authors

from LiuXin.utils.logger import default_log

# Py2/Py3 compatibility layer
from LiuXin.utils.lx_libraries.liuxin_six import six_unicode

__license__ = "GPL v3"
__copyright__ = "2008, Ashish Kulkarni <kulkarni.ashish@gmail.com>"

"""Read meta information from IMP files"""


MAGIC = ["\x00\x01BOOKDOUG", "\x00\x02BOOKDOUG"]


def get_metadata(stream):
    """Return metadata as a L{MetaInfo} object"""
    title = "Unknown"
    mi = MetaInformation(title, ["Unknown"])
    stream.seek(0)
    try:
        if stream.read(10) not in MAGIC:
            default_log.warning("Couldn't read IMP header from file")
            return mi

        def cString(skip=0):
            result = ""
            while 1:
                data = stream.read(1)
                if data == "\x00":
                    if not skip:
                        return result
                    skip -= 1
                    result, data = "", ""
                result += data

        stream.read(38)  # skip past some uninteresting headers
        cString()
        category, title, author = cString(), cString(1), cString(2)

        if title:
            mi.title = title
        if author:
            mi.authors = string_to_authors(author)
            mi.author = author
        if category:
            mi.category = category
    except Exception as err:
        msg = "Couldn't read metadata from imp: %s with error %s" % (
            mi.title,
            six_unicode(err),
        )
        default_log.warning(msg.encode("utf8"))
    return mi
