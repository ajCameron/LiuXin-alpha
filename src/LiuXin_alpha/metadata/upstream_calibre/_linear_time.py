"""
Give the ported calibre modules LiuXin's linear-time stand-ins for calibre's patterns.

``re.sub`` tries a pattern at every position, and several of calibre's
patterns scan a long way from each position before failing: on crafted text
the whole substitution costs its square, or worse. The fixes marked
``# LiuXin:`` in the ported modules call the helpers in
``LiuXin_alpha.utils.linear_time``, which LiuXin's own readers use too; this
module passes them through under the names the port's ``PATCHES`` use.

It adds ``linear_pattern``, which recognises calibre's encoding-declaration
patterns by their source as calibre writes it, and gives the stand-in that
finds their matches by hand. ``tests/file_formats/providers/test_ported_mobi_reader.py``
compares every use with calibre's original on random text.

Example:
    >>> import re
    >>> sub_skipping_failures(re.compile(r"<p[^>]*>"), "P", "<p <p x> <p", re.compile(r"<p"), ">")
    'P <p'
"""

from __future__ import annotations

import re
from typing import Any, AnyStr, cast

from LiuXin_alpha.utils.linear_time import (
    ForwardFinder,
    LinearPattern,
    first_images,
    html4_charsets,
    sub_before_last,
    sub_from_run_starts,
    sub_page_breaks,
    sub_skipping_failures,
    xml_declarations,
    xml_named_declarations,
)

__all__ = [
    "ForwardFinder",
    "first_images",
    "linear_pattern",
    "nested_deeper_than",
    "sub_before_last",
    "sub_from_run_starts",
    "sub_page_breaks",
    "sub_skipping_failures",
]

# calibre's patterns for encoding declarations, as written in its chardet and,
# for declarations named xml, its XMP reader. They match what LiuXin's
# XML_DECLARATION, XML_NAMED_DECLARATION and HTML4_CHARSET match; only the
# quoting of the source differs.
_XML_DECLARATION = r'<\?[^<>]+encoding\s*=\s*[\'"](.*?)[\'"][^<>]*>'
_XML_NAMED_DECLARATION = r'<\?xml[^<>]+encoding\s*=\s*[\'"](.*?)[\'"][^<>]*>'
_HTML4_CHARSET = r'''<meta\s+?[^<>]*?content\s*=\s*['"][^'"]*?charset=([-_a-z0-9]+)[^'"]*?['"][^<>]*>(?:\s*</meta>){0,1}'''


def linear_pattern(pattern: re.Pattern[AnyStr]) -> re.Pattern[AnyStr]:
    """
    Give calibre's encoding-declaration patterns a linear-time stand-in.

    The XML-declaration patterns (any declaration, and those named ``xml``, which
    the XMP reader strips) and the HTML 4 charset pattern are replaced; any other
    pattern is returned as it is. The stand-in offers ``search``, ``sub`` and
    ``finditer`` only, which is all calibre asks of these patterns.

    Example:
        >>> linear_pattern(re.compile("a")).pattern
        'a'
        >>> type(linear_pattern(re.compile(_HTML4_CHARSET, re.IGNORECASE))).__name__
        'LinearPattern'


    :param pattern: The compiled pattern, with ``re.IGNORECASE``.
    :return: The pattern, or its stand-in.
    """
    source = pattern.pattern if isinstance(pattern.pattern, str) else pattern.pattern.decode("ascii")
    if source == _XML_DECLARATION:
        return cast("re.Pattern[AnyStr]", LinearPattern(pattern, xml_declarations))
    if source == _XML_NAMED_DECLARATION:
        return cast("re.Pattern[AnyStr]", LinearPattern(pattern, xml_named_declarations))
    if source == _HTML4_CHARSET:
        return cast("re.Pattern[AnyStr]", LinearPattern(pattern, html4_charsets))
    return pattern


def nested_deeper_than(root: Any, limit: int) -> bool:
    """
    Tell whether an element tree nests more than ``limit`` elements deep, in one pass.

    calibre's XMP reader looks up ``//dc:title`` and the like, then reads every
    match's descendants. A packet that nests matches inside matches makes it
    read the same descendants once per level, so the reader refuses packets
    deeper than real ones go.

    Example:
        >>> from lxml import etree
        >>> nested_deeper_than(etree.fromstring("<a><b><c/></b></a>"), 2), nested_deeper_than(etree.fromstring("<a><b/></a>"), 2)
        (True, False)


    :param root: The tree's root element.
    :param limit: The deepest nesting allowed, counting the root as 1.
    :return: Whether some element is nested deeper.
    """
    from lxml import etree

    depth = 0
    for event, _ in etree.iterwalk(root, events=("start", "end")):
        depth += 1 if event == "start" else -1
        if depth > limit:
            return True
    return False
