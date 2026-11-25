#!/usr/bin/env python
# License: GPLv3 Copyright: 2019, Kovid Goyal <kovid at kovidgoyal.net>
# Ported from calibre (https://calibre-ebook.com) 9.18: src/calibre/ebooks/BeautifulSoup.py
# Changes from upstream: calibre imports point at upstream_calibre and its _compat.
# html5-parser's soup builder is replaced by Beautiful Soup's lxml builder
# (_compat.html5_soup_parse); Beautiful Soup itself stays optional.
# SPDX-License-Identifier: GPL-3.0-only

import bs4
from bs4 import CData, Comment, Declaration, NavigableString, ProcessingInstruction, SoupStrainer, Tag, __version__  # ruff: ignore[unused-import]


def parse_html(markup: str | bytes) -> bs4.BeautifulSoup:
    from LiuXin_alpha.metadata.upstream_calibre._compat import xml_replace_entities
    from LiuXin_alpha.metadata.upstream_calibre.chardet import strip_encoding_declarations
    from LiuXin_alpha.metadata.upstream_calibre.chardet import xml_to_unicode
    from LiuXin_alpha.metadata.upstream_calibre.cleantext import clean_xml_chars

    if isinstance(markup, str):
        markup = strip_encoding_declarations(markup)
        markup = xml_replace_entities(markup)
    else:
        markup = xml_to_unicode(markup, strip_encoding_pats=True, resolve_entities=True)[0]
    markup = clean_xml_chars(markup)
    from LiuXin_alpha.metadata.upstream_calibre._compat import html5_soup_parse as parse

    return parse(markup, return_root=False)


def prettify(soup: Tag) -> str:
    ans = soup.prettify()
    if isinstance(ans, bytes):
        ans = ans.decode('utf-8')
    return ans


def BeautifulSoup(markup: str | bytes = '', *a: object, **kw: object) -> bs4.BeautifulSoup:
    return parse_html(markup)


def BeautifulStoneSoup(markup: str | bytes = '', *a: object, **kw: object) -> bs4.BeautifulSoup:
    return bs4.BeautifulSoup(markup, 'xml')
