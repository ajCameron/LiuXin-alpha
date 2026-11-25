#!/usr/bin/env python
# License: GPLv3 Copyright: 2012, Kovid Goyal <kovid at kovidgoyal.net>
# Ported from calibre (https://calibre-ebook.com) 9.18: src/calibre/ebooks/metadata/docx.py
# Changes from upstream: calibre imports point at upstream_calibre and its _compat.
# LiuXin fixes are marked '# LiuXin:'.
# SPDX-License-Identifier: GPL-3.0-only

from io import BytesIO
from typing import IO, TYPE_CHECKING

from LiuXin_alpha.metadata.upstream_calibre.docx_container import DOCX
from LiuXin_alpha.metadata.upstream_calibre.docx_writer_container import update_doc_props
from LiuXin_alpha.metadata.upstream_calibre.docx_writer_container import xml2str
from LiuXin_alpha.metadata.upstream_calibre.imghdr import identify
from LiuXin_alpha.metadata.upstream_calibre._compat import safe_xml_fromstring
from LiuXin_alpha.metadata.upstream_calibre import _linear_time  # LiuXin:

if TYPE_CHECKING:
    from LiuXin_alpha.metadata.upstream_calibre.book_base import Metadata


def get_cover(docx: DOCX) -> tuple[str | None, bytes] | None:
    doc = docx.document
    get = docx.namespace.get
    images = docx.namespace.XPath(
        '//*[name()="w:drawing" or name()="w:pict"]/descendant::*[(name()="a:blip" and @r:embed) or (name()="v:imagedata" and @r:id)][1]'
    )
    # LiuXin: libxml2 evaluates that in quadratic time or worse (64,000 drawings
    # took 8 s); first_images finds the same images in one pass (#199).
    images = lambda doc: _linear_time.first_images(doc, docx.namespace.namespaces['r'])  # noqa: E731
    rid_map = docx.document_relationships[0]
    # LiuXin: a document can name one image in a million drawings, and each was
    # read whole again and turned down again. Read each image once.
    tried = set()
    for image in images(doc):
        rid = get(image, 'r:embed') or get(image, 'r:id')
        if rid in rid_map and rid_map[rid] not in tried:
            tried.add(rid_map[rid])
            try:
                raw = docx.read(rid_map[rid])
                fmt, width, height = identify(raw)
            except Exception:
                continue
            if width < 0 or height < 0:
                continue
            if 0.8 <= height / width <= 1.8 and height * width >= 160000:
                return fmt, raw


def get_metadata(stream: IO[bytes]) -> Metadata:
    c = DOCX(stream, extract=False)
    mi = c.metadata
    try:
        cdata = get_cover(c)
    except Exception:
        cdata = None
        import traceback

        traceback.print_exc()
    c.close()
    stream.seek(0)
    if cdata is not None:
        mi.cover_data = cdata

    return mi


def set_metadata(stream: IO[bytes], mi: Metadata) -> None:
    from LiuXin_alpha.metadata.upstream_calibre._compat import safe_replace

    c = DOCX(stream, extract=False)
    dp_name, ap_name = c.get_document_properties_names()
    if dp_name is None:  # c.read(None) would fail too
        raise KeyError('No document properties found in DOCX file')
    dp_raw = c.read(dp_name)
    try:
        ap_raw = None if ap_name is None else c.read(ap_name)
    except Exception:
        ap_raw = None
    cp = safe_xml_fromstring(dp_raw)
    update_doc_props(cp, mi, c.namespace)
    replacements: dict[str, BytesIO] = {}
    if ap_raw is not None and ap_name is not None:
        ap = safe_xml_fromstring(ap_raw)
        comp = ap.makeelement('{{{}}}Company'.format(c.namespace.namespaces['ep']))
        for child in tuple(ap):
            if child.tag == comp.tag:
                ap.remove(child)
        comp.text = mi.publisher
        ap.append(comp)
        replacements[ap_name] = BytesIO(xml2str(ap))
    stream.seek(0)
    safe_replace(stream, dp_name, BytesIO(xml2str(cp)), extra_replacements=replacements)


if __name__ == '__main__':
    import sys

    with open(sys.argv[-1], 'rb') as stream:
        print(get_metadata(stream))
