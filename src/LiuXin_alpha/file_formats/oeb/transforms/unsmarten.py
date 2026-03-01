# -*- coding: utf-8 -*-

from __future__ import unicode_literals, division, absolute_import, print_function

from LiuXin_alpha.file_formats.oeb.base import OEB_DOCS, XPath, barename

try:
    from LiuXin_alpha.utils.libraries.unsmarten import unsmarten_text
except ModuleNotFoundError:
    # Keep transform functional even if legacy helper module is absent.
    _UNSMARTEN_REPLACEMENTS = {
        "&#8211;": "--",
        "&ndash;": "--",
        "–": "--",
        "&#8212;": "---",
        "&mdash;": "---",
        "—": "---",
        "&#8230;": "...",
        "&hellip;": "...",
        "…": "...",
        "&#8220;": '"',
        "&#8221;": '"',
        "&#8222;": '"',
        "&#8243;": '"',
        "&ldquo;": '"',
        "&rdquo;": '"',
        "&bdquo;": '"',
        "&Prime;": '"',
        "“": '"',
        "”": '"',
        "„": '"',
        "″": '"',
        "&#8216;": "'",
        "&#8217;": "'",
        "&#8242;": "'",
        "&lsquo;": "'",
        "&rsquo;": "'",
        "&prime;": "'",
        "‘": "'",
        "’": "'",
        "′": "'",
    }

    def unsmarten_text(text):
        for src, dst in _UNSMARTEN_REPLACEMENTS.items():
            text = text.replace(src, dst)
        return text

__license__ = "GPL 3"
__copyright__ = "2011, John Schember <john@nachtimwald.com>"
__docformat__ = "restructuredtext en"


class UnsmartenPunctuation(object):
    def __init__(self):
        self.html_tags = XPath("descendant::h:*")

    def unsmarten(self, root):
        for x in self.html_tags(root):
            if not barename(x.tag) == "pre":
                if getattr(x, "text", None):
                    x.text = unsmarten_text(x.text)
                if getattr(x, "tail", None) and x.tail:
                    x.tail = unsmarten_text(x.tail)

    def __call__(self, oeb, context):
        bx = XPath("//h:body")
        for x in oeb.manifest.items:
            if x.media_type in OEB_DOCS:
                for body in bx(x.data):
                    self.unsmarten(body)
