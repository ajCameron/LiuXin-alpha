from __future__ import with_statement

"""
Add page mapping information to an EPUB book.
"""

import re
from itertools import count

from LiuXin_alpha.file_formats.oeb.base import XHTML_NS, OEBBook
from LiuXin_alpha.utils.libraries.liuxin_etree import etree
from LiuXin_alpha.utils.logging import default_log

__license__ = "GPL v3"
__copyright__ = "2008, Marshall T. Vandegrift <llasram@gmail.com>"
__docformat__ = "restructuredtext en"

NSMAP = {"h": XHTML_NS, "html": XHTML_NS, "xhtml": XHTML_NS}
PAGE_RE = re.compile(r"page", re.IGNORECASE)
ROMAN_RE = re.compile(r"^[ivxlcdm]+$", re.IGNORECASE)


def filter_name(name):
    name = name.strip()
    name = PAGE_RE.sub("", name)
    for word in name.split():
        if word.isdigit() or ROMAN_RE.match(word):
            name = word
            break
    return name


def build_name_for(expr):
    if not expr:
        counter = count(1)
        return lambda elem: str(next(counter))
    selector = etree.XPath(expr, namespaces=NSMAP)

    def name_for(elem):
        results = selector(elem)
        if not results:
            return ""
        if isinstance(results, (str, bytes)):
            name = results.decode("utf-8", "replace") if isinstance(results, bytes) else results
        else:
            text_bits = []
            for part in results:
                text_bits.append(str(part))
            name = " ".join(text_bits)
        return filter_name(name)

    return name_for


def add_page_map(opfpath, opts):
    try:
        from LiuXin_alpha.file_formats.oeb.reader import OEBReader
        from LiuXin_alpha.file_formats.oeb.writer import OEBWriter
    except Exception as e:
        raise RuntimeError("add_page_map currently depends on the OEB reader/writer stack") from e

    if not getattr(opts, "page", None):
        raise ValueError("A page selector is required to add a page map")

    oeb = OEBBook(default_log, lambda x: x, pretty_print=bool(getattr(opts, "pretty_print", False)))
    OEBReader()(oeb, opfpath)
    selector = etree.XPath(opts.page, namespaces=NSMAP)
    name_for = build_name_for(opts.page_names)
    idgen = ("calibre-page-%d" % n for n in count(1))
    for item in oeb.spine:
        data = item.data
        for elem in selector(data):
            name = name_for(elem)
            item_id = elem.get("id", None)
            if item_id is None:
                item_id = next(idgen)
                elem.set("id", item_id)
            href = "#".join((item.href, item_id))
            oeb.pages.add(name, href)
    writer = OEBWriter(version="2.0", page_map=True, pretty_print=bool(getattr(opts, "pretty_print", False)))
    writer(oeb, opfpath)
