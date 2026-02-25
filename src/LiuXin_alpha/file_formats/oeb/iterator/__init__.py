#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai

from __future__ import unicode_literals, division, absolute_import, print_function

import os
import re
import sys


from LiuXin_alpha.utils.localization import trans as _

__license__ = "GPL v3"
__copyright__ = "2012, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"


def is_supported(path):
    """
    Is the file in the list of formats which can be converted to html?
    :param path:
    :return:
    """
    from LiuXin_alpha.customize.ui import available_input_formats

    ext = os.path.splitext(path)[1].replace(".", "").lower()
    ext = re.sub(r"(x{0,1})htm(l{0,1})", "html", ext)
    return ext in available_input_formats() or ext == "kepub"


class UnsupportedFormatError(Exception):
    def __init__(self, fmt):
        Exception.__init__(self, _("%s format books are not supported") % fmt.upper())


def EbookIterator(*args, **kwargs):
    """
    For backwards compatibility
    :param args:
    :param kwargs:
    :return:
    """
    from LiuXin_alpha.file_formats.oeb.iterator.book import EbookIterator

    return EbookIterator(*args, **kwargs)


def get_preprocess_html(path_to_ebook, output=None):
    """
    Return an html version of the book before pre-process has been run.
    :param path_to_ebook:
    :param output:
    :return:
    """
    from LiuXin_alpha.file_formats.conversion.plumber import (
        set_regex_wizard_callback,
        Plumber,
    )
    from LiuXin_alpha.utils.logger import DevNull
    from LiuXin_alpha.utils.ptempfiles import TemporaryDirectory

    raw = {}
    set_regex_wizard_callback(raw.__setitem__)
    with TemporaryDirectory("_regex_wiz") as tdir:
        pl = Plumber(
            path_to_ebook,
            os.path.join(tdir, "a.epub"),
            DevNull(),
            for_regex_wizard=True,
        )
        pl.run()
        items = [raw[item.href] for item in pl.oeb.spine if item.href in raw]

    with (sys.stdout if output is None else open(output, "wb")) as out:
        for html in items:
            out.write(html.encode("utf-8"))
            out.write(b"\n\n" + b"-" * 80 + b"\n\n")
