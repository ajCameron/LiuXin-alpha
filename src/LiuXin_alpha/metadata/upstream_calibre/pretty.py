#!/usr/bin/env python
# License: GPLv3 Copyright: 2013, Kovid Goyal <kovid at kovidgoyal.net>
# Ported from calibre (https://calibre-ebook.com) 9.18: src/calibre/ebooks/oeb/polish/pretty.py
# Changes from upstream: only isspace, pretty_xml_tree and pretty_opf are ported, which
# is what the OPF code uses; calibre imports point at _compat, where OEB_DOCS and
# OEB_STYLES are built from calibre's media-type table.
# SPDX-License-Identifier: GPL-3.0-only

from typing import TYPE_CHECKING

from LiuXin_alpha.metadata.upstream_calibre._compat import OPF_NAMESPACES, barename, oeb_docs, oeb_styles, sort_key
from LiuXin_alpha.metadata.upstream_calibre._compat import polish_guess_type as guess_type

if TYPE_CHECKING:
    from lxml.etree import _Element

OEB_DOCS = oeb_docs()
OEB_STYLES = oeb_styles()


def isspace(x: str) -> bool:
    return not x.strip('\u0009\u000a\u000c\u000d\u0020')


def pretty_xml_tree(elem: _Element, level: int = 0, indent: str = '  ') -> None:
    """XML beautifier, assumes that elements that have children do not have
    textual content.  Also assumes that there is no text immediately after
    closing tags. These are true for opf/ncx and container.xml files. If either
    of the assumptions are violated, there should be no data loss, but pretty
    printing won't produce optimal results."""
    if (not elem.text and len(elem) > 0) or (elem.text and isspace(elem.text)):
        elem.text = '\n' + (indent * (level + 1))
    for i, child in enumerate(elem):
        pretty_xml_tree(child, level=level + 1, indent=indent)
        if not child.tail or isspace(child.tail):
            l = level + 1
            if i == len(elem) - 1:
                l -= 1
            child.tail = '\n' + (indent * l)


def pretty_opf(root: _Element) -> None:
    # Put all dc: tags first starting with title and author. Preserve order for
    # the rest.
    def dckey(x: _Element) -> int:
        tag = x.tag
        assert isinstance(tag, str)  # dc: elements always have str tags
        return {'title': 0, 'creator': 1}.get(barename(tag), 2)

    for metadata in root.xpath('//opf:metadata', namespaces=OPF_NAMESPACES):
        dc_tags = metadata.xpath('./*[namespace-uri()="{}"]'.format(OPF_NAMESPACES['dc']))
        dc_tags.sort(key=dckey)
        for x in reversed(dc_tags):
            metadata.insert(0, x)

    # Group items in the manifest
    spine_ids = root.xpath('//opf:spine/opf:itemref/@idref', namespaces=OPF_NAMESPACES)
    spine_ids = {x: i for i, x in enumerate(spine_ids)}

    def manifest_key(x: _Element) -> tuple[int, int | bytes]:
        mt = x.get('media-type', '')
        href = x.get('href', '')
        ext = href.rpartition('.')[-1].lower()
        cat = 1000
        if mt in OEB_DOCS:
            cat = 0
        elif mt == guess_type('a.ncx'):
            cat = 1
        elif mt in OEB_STYLES:
            cat = 2
        elif mt.startswith('image/'):
            cat = 3
        elif ext in {'otf', 'ttf', 'woff', 'woff2'}:
            cat = 4
        elif mt.startswith('audio/'):
            cat = 5
        elif mt.startswith('video/'):
            cat = 6

        if cat == 0:
            i = spine_ids.get(x.get('id', None), 1000000000)
        else:
            i = sort_key(href)
        return cat, i

    for manifest in root.xpath('//opf:manifest', namespaces=OPF_NAMESPACES):
        try:
            children = sorted(manifest, key=manifest_key)
        except AttributeError:
            continue  # There are comments so don't sort since that would mess up the comments
        for x in reversed(children):
            manifest.insert(0, x)
