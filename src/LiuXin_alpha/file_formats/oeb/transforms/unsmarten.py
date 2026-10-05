# -*- coding: utf-8 -*-

"""
Replace typographic punctuation with conservative text equivalents.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise unsmarten through a consuming regression::

        python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py
"""
from __future__ import unicode_literals, division, absolute_import, print_function
from __future__ import annotations

import typing as _typing

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

    def unsmarten_text(text: _typing.Any) -> _typing.Any:
        """
        Perform the unsmarten text operation under explicit file-format and conversion rules.

        Example:
            Exercise unsmarten text through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param text: Text parsed, normalized or rendered.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        for src, dst in _UNSMARTEN_REPLACEMENTS.items():
            text = text.replace(src, dst)
        return text

__license__ = "GPL 3"
__copyright__ = "2011, John Schember <john@nachtimwald.com>"
__docformat__ = "restructuredtext en"


class UnsmartenPunctuation(object):
    """
    Provide the unsmartenpunctuation contract for validated ebook processing.

    Example:
        Exercise UnsmartenPunctuation through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py
    """
    def __init__(self: _typing.Self) -> None:
        """
        Initialize and validate the unsmartenpunctuation state.

        Example:
            Exercise UnsmartenPunctuation.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :return: None; validated state is stored on the receiving object.
        """
        self.html_tags = XPath("descendant::h:*")

    def unsmarten(self: _typing.Self, root: _typing.Any) -> None:
        """
        Perform the unsmarten operation under explicit file-format and conversion rules.

        Example:
            Exercise UnsmartenPunctuation.unsmarten through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param root: Root directory that bounds path resolution or traversal.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        for x in self.html_tags(root):
            if not barename(x.tag) == "pre":
                if getattr(x, "text", None):
                    x.text = unsmarten_text(x.text)
                if getattr(x, "tail", None) and x.tail:
                    x.tail = unsmarten_text(x.tail)

    def __call__(self: _typing.Self, oeb: _typing.Any, context: _typing.Any) -> None:
        """
        Perform the call operation under explicit file-format and conversion rules.

        Example:
            Exercise UnsmartenPunctuation.  call   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param oeb: Value supplied for oeb under the utility contract.
        :param context: Value supplied for context under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        bx = XPath("//h:body")
        for x in oeb.manifest.items:
            if x.media_type in OEB_DOCS:
                for body in bx(x.data):
                    self.unsmarten(body)
