#!/usr/bin/env python
# License: GPLv3 Copyright: 2013, Kovid Goyal <kovid at kovidgoyal.net>
# Ported from calibre (https://calibre-ebook.com) 9.18: src/calibre/ebooks/docx/writer/container.py
# Changes from upstream: calibre imports point at upstream_calibre and its _compat, and
# 'from __future__ import annotations' is added.
# Only xml2str and update_doc_props are ported, with the imports they use;
# the rest is conversion code the metadata readers do not use.
# SPDX-License-Identifier: GPL-3.0-only

from __future__ import annotations

from typing import TYPE_CHECKING

from lxml import etree

from LiuXin_alpha.metadata.upstream_calibre.docx_names import DOCXNamespace
from LiuXin_alpha.metadata.upstream_calibre.metadata_base import authors_to_string
from LiuXin_alpha.metadata.upstream_calibre._compat import canonicalize_lang
from LiuXin_alpha.metadata.upstream_calibre._compat import lang_as_iso639_1

if TYPE_CHECKING:
    from LiuXin_alpha.metadata.upstream_calibre.book_base import Metadata


def xml2str(root: etree._Element, pretty_print: bool = False, with_tail: bool = False) -> bytes:
    if hasattr(etree, 'cleanup_namespaces'):
        etree.cleanup_namespaces(root)
    ans = etree.tostring(root, encoding='utf-8', xml_declaration=True, pretty_print=pretty_print, with_tail=with_tail)
    return ans


def update_doc_props(root: etree._Element, mi: Metadata, namespace: DOCXNamespace) -> None:
    def setm(name: str, text: str | None = None, ns: str = 'dc') -> etree._Element:
        ans = root.makeelement(f'{{{namespace.namespaces[ns]}}}{name}')
        for child in tuple(root):
            if child.tag == ans.tag:
                root.remove(child)
        ans.text = text
        root.append(ans)
        return ans

    setm('title', mi.title)
    setm('creator', authors_to_string(mi.authors))
    if mi.tags:
        setm('keywords', ', '.join(mi.tags), ns='cp')
    if mi.comments:
        setm('description', mi.comments)
    if mi.languages:
        l = canonicalize_lang(mi.languages[0])
        setm('language', lang_as_iso639_1(l) or l)
