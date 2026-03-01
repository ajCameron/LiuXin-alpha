#!/usr/bin/env python
# vim:fileencoding=utf-8

from __future__ import unicode_literals, division, absolute_import, print_function

from lxml import etree

from LiuXin_alpha.file_formats.oeb.polish.container import OPF_NAMESPACES

from LiuXin_alpha.utils.localization import canonicalize_lang

__license__ = "GPL v3"
__copyright__ = "2014, Kovid Goyal <kovid at kovidgoyal.net>"


def get_book_language(container):
    for lang in container.opf_xpath("//dc:language"):
        raw = getattr(lang, "text", None)
        if not raw:
            continue
        try:
            primary = str(raw).split(",")[0].strip()
        except Exception:
            continue
        if not primary:
            continue
        try:
            code = canonicalize_lang(primary)
        except Exception:
            continue
        if code:
            return code


def set_guide_item(container, item_type, title, name, frag=None):
    ref_tag = "{%s}reference" % OPF_NAMESPACES["opf"]
    item_type = "" if item_type is None else str(item_type)
    href = None
    if name:
        try:
            href = container.name_to_href(name, container.opf_name)
        except ValueError:
            href = None
        if href and frag:
            href += "#" + frag

    guides = container.opf_xpath("//opf:guide")
    if not guides and href:
        g = container.opf.makeelement("{%s}guide" % OPF_NAMESPACES["opf"], nsmap={"opf": OPF_NAMESPACES["opf"]})
        container.insert_into_xml(container.opf, g)
        guides = [g]

    for guide in guides:
        matches = []
        for child in guide.iterchildren(etree.Element):
            if child.tag == ref_tag and child.get("type", "").lower() == item_type.lower():
                matches.append(child)
        if not matches and href:
            r = guide.makeelement(ref_tag, type=item_type, nsmap={"opf": OPF_NAMESPACES["opf"]})
            container.insert_into_xml(guide, r)
            matches.append(r)
        for m in matches:
            if href:
                if title is not None:
                    m.set("title", str(title))
                elif "title" in m.attrib:
                    del m.attrib["title"]
                m.set("href", href)
                m.set("type", item_type)
            else:
                container.remove_from_xml(m)
    container.dirty(container.opf_name)
