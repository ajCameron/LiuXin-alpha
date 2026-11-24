#!/usr/bin/env python
# License: GPLv3 Copyright: 2013, Kovid Goyal <kovid at kovidgoyal.net>
# Ported from calibre (https://calibre-ebook.com) 9.18: src/calibre/ebooks/docx/names.py
# Changes from upstream: calibre imports point at upstream_calibre and its _compat.
# Only TRANSITIONAL_NAMES, STRICT_NAMES, TRANSITIONAL_NAMESPACES, STRICT_NAMESPACES, SVG_BLIP_URI, USE_LOCAL_DPI_URI, AttrGetter, barename, XML and DOCXNamespace are ported, with the imports they use;
# the rest is conversion code the metadata readers do not use.
# SPDX-License-Identifier: GPL-3.0-only

from typing import TYPE_CHECKING, Protocol, overload

from lxml.etree import XPath as X

if TYPE_CHECKING:
    from lxml.etree import _Element


# Names {{{
TRANSITIONAL_NAMES = {
    'DOCUMENT': 'http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument',
    'DOCPROPS': 'http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties',
    'APPPROPS': 'http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties',
    'STYLES': 'http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles',
    'NUMBERING': 'http://schemas.openxmlformats.org/officeDocument/2006/relationships/numbering',
    'FONTS': 'http://schemas.openxmlformats.org/officeDocument/2006/relationships/fontTable',
    'EMBEDDED_FONT': 'http://schemas.openxmlformats.org/officeDocument/2006/relationships/font',
    'IMAGES': 'http://schemas.openxmlformats.org/officeDocument/2006/relationships/image',
    'LINKS': 'http://schemas.openxmlformats.org/officeDocument/2006/relationships/hyperlink',
    'FOOTNOTES': 'http://schemas.openxmlformats.org/officeDocument/2006/relationships/footnotes',
    'ENDNOTES': 'http://schemas.openxmlformats.org/officeDocument/2006/relationships/endnotes',
    'THEMES': 'http://schemas.openxmlformats.org/officeDocument/2006/relationships/theme',
    'SETTINGS': 'http://schemas.openxmlformats.org/officeDocument/2006/relationships/settings',
    'WEB_SETTINGS': 'http://schemas.openxmlformats.org/officeDocument/2006/relationships/webSettings',
}


STRICT_NAMES = {
    k: v.replace('http://schemas.openxmlformats.org/officeDocument/2006', 'http://purl.oclc.org/ooxml/officeDocument') for k, v in TRANSITIONAL_NAMES.items()
}


TRANSITIONAL_NAMESPACES = {
    'mo': 'http://schemas.microsoft.com/office/mac/office/2008/main',
    'o': 'urn:schemas-microsoft-com:office:office',
    've': 'http://schemas.openxmlformats.org/markup-compatibility/2006',
    'mc': 'http://schemas.openxmlformats.org/markup-compatibility/2006',
    # Text Content
    'w': 'http://schemas.openxmlformats.org/wordprocessingml/2006/main',
    'w10': 'urn:schemas-microsoft-com:office:word',
    'wne': 'http://schemas.microsoft.com/office/word/2006/wordml',
    'xml': 'http://www.w3.org/XML/1998/namespace',
    # Drawing
    'a': 'http://schemas.openxmlformats.org/drawingml/2006/main',
    'a14': 'http://schemas.microsoft.com/office/drawing/2010/main',
    'm': 'http://schemas.openxmlformats.org/officeDocument/2006/math',
    'mv': 'urn:schemas-microsoft-com:mac:vml',
    'pic': 'http://schemas.openxmlformats.org/drawingml/2006/picture',
    'v': 'urn:schemas-microsoft-com:vml',
    'wp': 'http://schemas.openxmlformats.org/drawingml/2006/wordprocessingDrawing',
    # Properties (core and extended)
    'cp': 'http://schemas.openxmlformats.org/package/2006/metadata/core-properties',
    'dc': 'http://purl.org/dc/elements/1.1/',
    'ep': 'http://schemas.openxmlformats.org/officeDocument/2006/extended-properties',
    'xsi': 'http://www.w3.org/2001/XMLSchema-instance',
    # Content Types
    'ct': 'http://schemas.openxmlformats.org/package/2006/content-types',
    # Package Relationships
    'r': 'http://schemas.openxmlformats.org/officeDocument/2006/relationships',
    'pr': 'http://schemas.openxmlformats.org/package/2006/relationships',
    # Dublin Core document properties
    'dcmitype': 'http://purl.org/dc/dcmitype/',
    'dcterms': 'http://purl.org/dc/terms/',
    # SVG embeds
    'asvg': 'http://schemas.microsoft.com/office/drawing/2016/SVG/main',
}


STRICT_NAMESPACES = {
    k: v
    .replace('http://schemas.openxmlformats.org/officeDocument/2006', 'http://purl.oclc.org/ooxml/officeDocument')
    .replace('http://schemas.openxmlformats.org/wordprocessingml/2006', 'http://purl.oclc.org/ooxml/wordprocessingml')
    .replace('http://schemas.openxmlformats.org/drawingml/2006', 'http://purl.oclc.org/ooxml/drawingml')
    for k, v in TRANSITIONAL_NAMESPACES.items()
}


SVG_BLIP_URI = '{96DAC541-7B7A-43D3-8B79-37D633B846F1}'


USE_LOCAL_DPI_URI = '{28A0092B-C50C-407E-A947-70E740481C1C}'


class AttrGetter(Protocol):
    """The type of :meth:`DOCXNamespace.get`"""

    @overload
    def __call__(self, x: _Element, attr: str, /) -> str | None: ...
    @overload
    def __call__[T](self, x: _Element, attr: str, default: T, /) -> str | T: ...


def barename(x: str) -> str:
    return x.rpartition('}')[-1]


def XML(x: str) -> str:
    return '{{{}}}{}'.format(TRANSITIONAL_NAMESPACES['xml'], x)


class DOCXNamespace:
    def __init__(self, transitional: bool = True) -> None:
        self.xpath_cache: dict[str, X] = {}
        if transitional:
            self.namespaces = TRANSITIONAL_NAMESPACES.copy()
            self.names = TRANSITIONAL_NAMES.copy()
        else:
            self.namespaces = STRICT_NAMESPACES.copy()
            self.names = STRICT_NAMES.copy()

    def XPath(self, expr: str) -> X:
        ans = self.xpath_cache.get(expr, None)
        if ans is None:
            self.xpath_cache[expr] = ans = X(expr, namespaces=self.namespaces)
        return ans

    def is_tag(self, x: _Element | str, q: str) -> bool:
        tag = getattr(x, 'tag', x)
        ns, name = q.partition(':')[0::2]
        return f'{{{self.namespaces.get(ns, None)}}}{name}' == tag

    def expand(self, name: str, sep: str = ':') -> str:
        ns, tag = name.partition(sep)[::2]
        if ns and tag:
            tag = f'{{{self.namespaces[ns]}}}{tag}'
        return tag or ns

    @overload
    def get(self, x: _Element, attr: str) -> str | None: ...
    @overload
    def get[T](self, x: _Element, attr: str, default: T) -> str | T: ...
    def get[T](self, x: _Element, attr: str, default: T | None = None) -> str | T | None:
        return x.attrib.get(self.expand(attr), default)

    def ancestor(self, elem: _Element, name: str) -> _Element | None:
        try:
            return self.XPath(f'ancestor::{name}[1]')(elem)[0]
        except IndexError:
            return None

    def children(self, elem: _Element, *args: str) -> list[_Element]:
        return self.XPath('|'.join(f'child::{a}' for a in args))(elem)

    def descendants(self, elem: _Element, *args: str) -> list[_Element]:
        return self.XPath('|'.join(f'descendant::{a}' for a in args))(elem)

    def makeelement(self, root: _Element, tag: str, append: bool = True, **attrs: str) -> _Element:
        ans = root.makeelement(self.expand(tag), {self.expand(k, sep='_'): v for k, v in attrs.items()})
        if append:
            root.append(ans)
        return ans
