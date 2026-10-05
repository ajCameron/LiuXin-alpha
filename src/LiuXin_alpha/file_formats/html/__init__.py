#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai
# metadata extraction should now be working

"""
Expose the supported html compatibility surface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise   init   through a consuming regression::

        python -m pytest -q tests/file_formats/html/test_html_modernized.py
"""
from __future__ import with_statement
from __future__ import annotations

import typing as _typing

import re

__license__ = "GPL v3"
__copyright__ = "2009, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"


COMMENT_PAT = re.compile(r"<!--.*?-->", re.DOTALL)


def tostring(root: _typing.Any, strip_comments: bool = False, pretty_print: bool = False) -> _typing.Any:
    """
    Serializes an XHTML structure

    Example:
        Exercise tostring through a consuming regression::

            python -m pytest -q tests/file_formats/html/test_html_modernized.py


    :param root: Root directory that bounds path resolution or traversal.
    :param strip_comments: Value supplied for strip comments under the utility contract.
    :param pretty_print: Value supplied for pretty print under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    from LiuXin_alpha.utils.libraries.liuxin_etree import etree

    root.set("xmlns", "http://www.w3.org/1999/xhtml")
    root.set("{http://www.w3.org/1999/xhtml}xlink", "http://www.w3.org/1999/xlink")
    for x in root.iter():
        if hasattr(x.tag, "rpartition") and x.tag.rpartition("}")[-1].lower() == "svg":
            x.set("xmlns", "http://www.w3.org/2000/svg")

    ans = etree.tostring(root, encoding="utf-8", pretty_print=pretty_print)
    if isinstance(ans, str):
        ans = ans.encode("utf-8")

    if strip_comments:
        ans = COMMENT_PAT.sub("", ans.decode("utf-8", "replace")).encode("utf-8")
    ans = b'<?xml version="1.0" encoding="utf-8" ?>\n' + ans

    return ans
