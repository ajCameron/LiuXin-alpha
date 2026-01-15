#!/usr/bin/env python
# vim:fileencoding=utf-8

from __future__ import unicode_literals, division, absolute_import, print_function

import os
import sys

from lxml import etree

from LiuXin.utils.calibre import prepare_string_for_xml, CurrentDir

from LiuXin.utils.ptempfiles import TemporaryDirectory

from LiuXin.metadata import authors_to_string

from LiuXin.file_formats.oeb.base import serialize
from LiuXin.file_formats.opf.opf2 import metadata_to_opf
from LiuXin.file_formats.oeb.polish.parsing import parse
from LiuXin.file_formats.oeb.polish.container import (
    OPF_NAMESPACES,
    opf_to_azw3,
    Container,
)
from LiuXin.file_formats.oeb.polish.utils import guess_type
from LiuXin.file_formats.oeb.polish.pretty import pretty_xml_tree, pretty_html_tree
from LiuXin.file_formats.oeb.polish.toc import TOC, create_ncx

from LiuXin.utils.localization import lang_as_iso639_1
from LiuXin.utils.localization import trans as _
from LiuXin.utils.logger import DevNull
from LiuXin.utils.resources import P
from LiuXin.utils.calibre_utils.calibre_zipfile import ZipFile, ZIP_STORED

__license__ = "GPL v3"
__copyright__ = "2013, Kovid Goyal <kovid at kovidgoyal.net>"


def create_toc(mi, opf, html_name, lang):
    uuid = ""
    for u in opf.xpath('//*[@id="uuid_id"]'):
        uuid = u.text
    toc = TOC()
    toc.add(_("Start"), html_name)
    return create_ncx(toc, lambda x: x, mi.title, lang, uuid)


def create_book(
    mi,
    path,
    fmt="epub",
    opf_name="metadata.opf",
    html_name="start.xhtml",
    toc_name="toc.ncx",
):
    """
    Create an empty book in the specified format at the specified location.
    :param mi:
    :param path:
    :param fmt:
    :param opf_name:
    :param html_name:
    :param toc_name:
    :return:
    """
    path = os.path.abspath(path)
    lang = "und"
    opf = metadata_to_opf(mi, as_string=False)
    for l in opf.xpath('//*[local-name()="language"]'):
        if l.text:
            lang = l.text
            break
    lang = lang_as_iso639_1(lang) or lang

    opfns = OPF_NAMESPACES["opf"]
    m = opf.makeelement("{%s}manifest" % opfns)
    opf.insert(1, m)
    i = m.makeelement("{%s}item" % opfns, href=html_name, id="start")
    i.set("media-type", guess_type("a.xhtml"))
    m.append(i)
    i = m.makeelement("{%s}item" % opfns, href=toc_name, id="ncx")
    i.set("media-type", guess_type(toc_name))
    m.append(i)
    s = opf.makeelement("{%s}spine" % opfns, toc="ncx")
    opf.insert(2, s)
    i = s.makeelement("{%s}itemref" % opfns, idref="start")
    s.append(i)
    container = """\
<?xml version="1.0"?>
<container version="1.0" xmlns="urn:oasis:names:tc:opendocument:xmlns:container">
   <rootfiles>
      <rootfile full-path="{0}" media-type="application/oebps-package+xml"/>
   </rootfiles>
</container>
    """.format(
        prepare_string_for_xml(opf_name, True)
    ).encode(
        "utf-8"
    )
    html = (
        P("templates/new_book.html", data=True)
        .decode("utf-8")
        .replace("_LANGUAGE_", prepare_string_for_xml(lang, True))
        .replace("_TITLE_", prepare_string_for_xml(mi.title))
        .replace("_AUTHORS_", prepare_string_for_xml(authors_to_string(mi.authors)))
        .encode("utf-8")
    )

    h = parse(html)
    pretty_html_tree(None, h)
    html = serialize(h, "text/html")
    ncx = etree.tostring(
        create_toc(mi, opf, html_name, lang),
        encoding="utf-8",
        xml_declaration=True,
        pretty_print=True,
    )
    pretty_xml_tree(opf)
    opf = etree.tostring(opf, encoding="utf-8", xml_declaration=True, pretty_print=True)

    if fmt == "azw3":
        with TemporaryDirectory("create-azw3") as tdir, CurrentDir(tdir):
            for name, data in ((opf_name, opf), (html_name, html), (toc_name, ncx)):
                with open(name, "wb") as f:
                    f.write(data)
            c = Container(os.path.dirname(os.path.abspath(opf_name)), opf_name, DevNull())
            opf_to_azw3(opf_name, path, c)
    else:
        with ZipFile(path, "w", compression=ZIP_STORED) as zf:
            zf.writestr("mimetype", b"application/epub+zip", compression=ZIP_STORED)
            zf.writestr("META-INF/", b"", 0o0755)
            zf.writestr("META-INF/container.xml", container)
            zf.writestr(opf_name, opf)
            zf.writestr(html_name, html)
            zf.writestr(toc_name, ncx)


if __name__ == "__main__":
    from LiuXin.metadata.book.base import calibreMetadata as Metadata

    test_mi = Metadata("Test book", authors=("Kovid Goyal",))
    test_path = sys.argv[-1]
    ext = test_path.rpartition(".")[-1].lower()
    if ext not in ("epub", "azw3"):
        print("Unsupported format:", ext)
        raise SystemExit(1)
    create_book(test_mi, test_path, fmt=ext)
