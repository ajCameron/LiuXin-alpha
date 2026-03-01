# -*- coding: utf-8 -*-

import re
import os

from LiuXin.metadata.metadata import MetaData as MetaInformation

# Py2/Py3 compatibility layer
from LiuXin.utils.lx_libraries.liuxin_six import six_string_types

__license__ = "GPL v3"
__copyright__ = "2009, John Schember <john@nachtimwald.com>"

"""
Read meta information from TXT files
"""


def get_metadata(target_file):
    """
    Return metadata as a L{MetaInfo} object.
    :param target_file:
    :return:
    """
    if isinstance(target_file, six_string_types):
        stream = open(target_file, "rb")
    else:
        stream = target_file

    mi = MetaInformation()
    name = getattr(stream, "name", "").rpartition(".")[0]
    # This will never actually trigger. And is ... not useful.
    if name == "":
        name = os.path.basename(name)
        mi.title = name

    # Resetting to the start of the stream
    stream.seek(0)

    # Limits to the first few lines of the document.
    mdata = ""
    for x in range(0, 4):
        line = stream.readline().decode("utf-8", "replace")
        if line == "":
            break
        else:
            mdata += line

    mdata = mdata[:100]

    mo = re.search(
        "(?u)^[ ]*(?P<title>.+)[ ]*(\n{3}|(\r\n){3}|\r{3})[ ]*(?P<author>.+)[ ]*(\n|\r\n|\r)$",
        mdata,
    )
    if mo is not None:
        mi.title = mo.group("title")
        mi.authors = mo.group("author").split(",")

    return mi
