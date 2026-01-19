#!/usr/bin/env python2
# vim:fileencoding=utf-8
from __future__ import unicode_literals, division, absolute_import, print_function

import os
import textwrap

from lxml import etree
from lxml.builder import ElementMaker

from LiuXin.file_formats.docx.names import DOCXNamespace

from LiuXin_alpha.metadata import authors_to_string

from LiuXin.utils.calibre import guess_type
from LiuXin.utils.calibre.constants import numeric_version, __appname__
from LiuXin.utils.date import utcnow
from LiuXin.utils.localization import canonicalize_lang, lang_as_iso639_1
from LiuXin.utils.calibre_utils.calibre_zipfile import ZipFile

# Py2/Py3
from LiuXin.utils.lx_libraries.liuxin_six import dict_iteritems as iteritems

try:
    from LiuXin.file_formats.pdf.render.common import PAPER_SIZES
except (KeyError, ImportError):
    # If the PDF paper sizes cannot be imported for some reason fall back to these
    # Sizes {{{
    inch = 72.0
    cm = inch / 2.54
    mm = cm * 0.1
    pica = 12.0
    didot = 0.375 * mm
    cicero = 12 * didot

    _W, _H = (21 * cm, 29.7 * cm)

    A6 = (_W * 0.5, _H * 0.5)
    A5 = (_H * 0.5, _W)
    A4 = (_W, _H)
    A3 = (_H, _W * 2)
    A2 = (_W * 2, _H * 2)
    A1 = (_H * 2, _W * 4)
    A0 = (_W * 4, _H * 4)

    LETTER = (8.5 * inch, 11 * inch)
    LEGAL = (8.5 * inch, 14 * inch)
    ELEVENSEVENTEEN = (11 * inch, 17 * inch)

    _BW, _BH = (25 * cm, 35.3 * cm)
    B6 = (_BW * 0.5, _BH * 0.5)
    B5 = (_BH * 0.5, _BW)
    B4 = (_BW, _BH)
    B3 = (_BH * 2, _BW)
    B2 = (_BW * 2, _BH * 2)
    B1 = (_BH * 4, _BW * 2)
    B0 = (_BW * 4, _BH * 4)

    PAPER_SIZES = {
        k: globals()[k.upper()] for k in ("a0 a1 a2 a3 a4 a5 a6 b0 b1 b2" " b3 b4 b5 b6 letter legal").split()
    }
    # }}}


_license__ = "GPL v3"
__copyright__ = "2013, Kovid Goyal <kovid at kovidgoyal.net>"


def xml2str(root, pretty_print=False, with_tail=False):
    if hasattr(etree, "cleanup_namespaces"):
        etree.cleanup_namespaces(root)
    ans = etree.tostring(
        root,
        encoding="utf-8",
        xml_declaration=True,
        pretty_print=pretty_print,
        with_tail=with_tail,
    )
    return ans


def page_size(opts):
    width, height = PAPER_SIZES[opts.docx_page_size]
    if opts.docx_custom_page_size is not None:
        width, height = map(float, opts.docx_custom_page_size.partition("x")[0::2])
    return width, height


def create_skeleton(opts, namespaces=None):
    namespaces = namespaces or DOCXNamespace().namespaces

    def w(x):
        return "{%s}%s" % (namespaces["w"], x)

    dn = {k: v for k, v in iteritems(namespaces) if k in {"w", "r", "m", "ve", "o", "wp", "w10", "wne", "a", "pic"}}
    e = ElementMaker(namespace=dn["w"], nsmap=dn)

    doc = e.document()
    body = e.body()
    doc.append(body)
    width, height = page_size(opts)
    width, height = int(20 * width), int(20 * height)

    def margin(which):
        return w(which), str(int(getattr(opts, "margin_" + which) * 20))

    body.append(
        e.sectPr(
            e.pgSz(**{w("w"): str(width), w("h"): str(height)}),
            e.pgMar(**dict(map(margin, "left top right bottom".split()))),
            e.cols(**{w("space"): "720"}),
            e.docGrid(**{w("linePitch"): "360"}),
        )
    )

    dn = {k: v for k, v in iteritems(namespaces) if k in tuple("wra") + ("wp",)}
    e = ElementMaker(namespace=dn["w"], nsmap=dn)
    styles = e.styles(
        e.docDefaults(
            e.rPrDefault(
                e.rPr(
                    e.rFonts(
                        **{
                            w("asciiTheme"): "minorHAnsi",
                            w("eastAsiaTheme"): "minorEastAsia",
                            w("hAnsiTheme"): "minorHAnsi",
                            w("cstheme"): "minorBidi",
                        }
                    ),
                    e.sz(**{w("val"): "22"}),
                    e.szCs(**{w("val"): "22"}),
                    e.lang(
                        **{
                            w("val"): "en-US",
                            w("eastAsia"): "en-US",
                            w("bidi"): "ar-SA",
                        }
                    ),
                )
            ),
            e.pPrDefault(e.pPr(e.spacing(**{w("after"): "0", w("line"): "276", w("lineRule"): "auto"}))),
        )
    )
    return doc, styles, body


def update_doc_props(root, mi, namespace):
    """
    Update a document with the given metadata
    :param root:
    :param mi:
    :type mi: calibreMetadata object
    :param namespace:
    :return:
    """

    def setm(name, text=None, ns="dc"):
        """
        Helper function to set the metadata in the document tree.
        :param name: The name of the metadata element to set
        :param text: The text to set the metadata element to
        :param ns:
        :return:
        """
        ans = root.makeelement("{%s}%s" % (namespace.namespaces[ns], name))
        for child in tuple(root):
            if child.tag == ans.tag:
                root.remove(child)
        ans.text = text
        root.append(ans)
        return ans

    setm("title", mi.title)

    setm("creator", authors_to_string(mi.authors))

    if mi.tags:
        # Tags written out without encoded spaces are split on those space. So encode the spaces
        tags_str = ", ".join(mi.tags)
        tags_str = tags_str.replace(" ", "_-_")
        setm("keywords", tags_str, ns="cp")

    if mi.comments:
        setm("description", mi.comments)

    if mi.languages:
        l = canonicalize_lang(mi.languages[0])
        setm("language", lang_as_iso639_1(l) or l)


class DocumentRelationships(object):
    def __init__(self, namespace):
        self.rmap = {}
        self.namespace = namespace
        for typ, target in iteritems(
            {
                namespace.names["STYLES"]: "styles.xml",
                namespace.names["NUMBERING"]: "numbering.xml",
                namespace.names["WEB_SETTINGS"]: "webSettings.xml",
                namespace.names["FONTS"]: "fontTable.xml",
            }
        ):
            self.add_relationship(target, typ)

    def get_relationship_id(self, target, rtype, target_mode=None):
        return self.rmap.get((target, rtype, target_mode))

    def add_relationship(self, target, rtype, target_mode=None):
        ans = self.get_relationship_id(target, rtype, target_mode)
        if ans is None:
            ans = "rId%d" % (len(self.rmap) + 1)
            self.rmap[(target, rtype, target_mode)] = ans
        return ans

    def add_image(self, target):
        return self.add_relationship(target, self.namespace.names["IMAGES"])

    def serialize(self):
        namespaces = self.namespace.namespaces
        e = ElementMaker(namespace=namespaces["pr"], nsmap={None: namespaces["pr"]})
        relationships = e.Relationships()
        for (target, rtype, target_mode), rid in iteritems(self.rmap):
            r = e.Relationship(Id=rid, Type=rtype, Target=target)
            if target_mode is not None:
                r.set("TargetMode", target_mode)
            relationships.append(r)
        return xml2str(relationships)


class DOCX(object):
    def __init__(self, opts, log):
        self.namespace = DOCXNamespace()
        namespaces = self.namespace.namespaces
        self.opts, self.log = opts, log
        self.document_relationships = DocumentRelationships(self.namespace)
        self.font_table = etree.Element("{%s}fonts" % namespaces["w"], nsmap={k: namespaces[k] for k in "wr"})
        self.numbering = etree.Element("{%s}numbering" % namespaces["w"], nsmap={k: namespaces[k] for k in "wr"})
        e = ElementMaker(namespace=namespaces["pr"], nsmap={None: namespaces["pr"]})
        self.embedded_fonts = e.Relationships()
        self.fonts = {}
        self.images = {}

    # Boilerplate {{{
    @property
    def contenttypes(self):
        e = ElementMaker(
            namespace=self.namespace.namespaces["ct"],
            nsmap={None: self.namespace.namespaces["ct"]},
        )
        types = e.Types()
        for partname, mt in iteritems(
            {
                "/word/footnotes.xml": "application/vnd.openxmlformats-officedocument.wordprocessingml.footnotes+xml",
                "/word/document.xml": "application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml",
                "/word/numbering.xml": "application/vnd.openxmlformats-officedocument.wordprocessingml.numbering+xml",
                "/word/styles.xml": "application/vnd.openxmlformats-officedocument.wordprocessingml.styles+xml",
                "/word/endnotes.xml": "application/vnd.openxmlformats-officedocument.wordprocessingml.endnotes+xml",
                "/word/settings.xml": "application/vnd.openxmlformats-officedocument.wordprocessingml.settings+xml",
                "/word/theme/theme1.xml": "application/vnd.openxmlformats-officedocument.theme+xml",
                "/word/fontTable.xml": "application/vnd.openxmlformats-officedocument.wordprocessingml.fontTable+xml",
                "/word/webSettings.xml": "application/vnd.openxmlformats-officedocument.wordprocessingml.webSettings+xml",
                "/docProps/core.xml": "application/vnd.openxmlformats-package.core-properties+xml",
                "/docProps/app.xml": "application/vnd.openxmlformats-officedocument.extended-properties+xml",
            }
        ):
            types.append(e.Override(PartName=partname, ContentType=mt))

        added = {"png", "gif", "jpeg", "jpg", "svg", "xml"}

        for ext in added:
            types.append(e.Default(Extension=ext, ContentType=guess_type("a." + ext)[0]))

        for ext, mt in iteritems(
            {
                "rels": "application/vnd.openxmlformats-package.relationships+xml",
                "odttf": "application/vnd.openxmlformats-officedocument.obfuscatedFont",
            }
        ):
            added.add(ext)
            types.append(e.Default(Extension=ext, ContentType=mt))

        for fname in self.images:
            ext = fname.rpartition(os.extsep)[-1]
            if ext not in added:
                added.add(ext)
                mt = guess_type("a." + ext)[0]
                if mt:
                    types.append(e.Default(Extension=ext, ContentType=mt))
        return xml2str(types)

    @property
    def appproperties(self):
        e = ElementMaker(
            namespace=self.namespace.namespaces["ep"],
            nsmap={None: self.namespace.namespaces["ep"]},
        )
        props = e.Properties(
            e.Application(__appname__),
            e.AppVersion("%02d.%04d" % numeric_version[:2]),
            e.DocSecurity("0"),
            e.HyperlinksChanged("false"),
            e.LinksUpToDate("true"),
            e.ScaleCrop("false"),
            e.SharedDoc("false"),
        )
        if self.mi.publisher:
            props.append(e.Company(self.mi.publisher))

        return xml2str(props)

    @property
    def containerrels(self):
        return textwrap.dedent(
            b"""\
        <?xml version='1.0' encoding='utf-8'?>
        <Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
            <Relationship Id="rId3" Type="{APPPROPS}" Target="docProps/app.xml"/>
            <Relationship Id="rId2" Type="{DOCPROPS}" Target="docProps/core.xml"/>
            <Relationship Id="rId1" Type="{DOCUMENT}" Target="word/document.xml"/>
        </Relationships>""".format(
                **self.namespace.names
            )
        )

    @property
    def websettings(self):
        e = ElementMaker(
            namespace=self.namespace.namespaces["w"],
            nsmap={"w": self.namespace.namespaces["w"]},
        )
        ws = e.webSettings(e.optimizeForBrowser, e.allowPNG, e.doNotSaveAsSingleFile)
        return xml2str(ws)

    # }}}

    def convert_metadata(self, mi):
        namespaces = self.namespace.namespaces
        e = ElementMaker(
            namespace=namespaces["cp"],
            nsmap={x: namespaces[x] for x in "cp dc dcterms xsi".split()},
        )
        cp = e.coreProperties(e.revision("1"), e.lastModifiedBy("calibre"))
        ts = utcnow().isoformat(str("T")).rpartition(".")[0] + "Z"
        for x in "created modified".split():
            x = cp.makeelement(
                "{%s}%s" % (namespaces["dcterms"], x), **{"{%s}type" % namespaces["xsi"]: "dcterms:W3CDTF"}
            )
            x.text = ts
            cp.append(x)
        self.mi = mi
        update_doc_props(cp, self.mi, self.namespace)
        return xml2str(cp)

    def create_empty_document(self, mi):
        self.document, self.styles = create_skeleton(self.opts)[:2]

    def write(self, path_or_stream, mi, create_empty_document=False):
        if create_empty_document:
            self.create_empty_document(mi)
        with ZipFile(path_or_stream, "w") as zf:
            zf.writestr("[Content_Types].xml", self.contenttypes)
            zf.writestr("_rels/.rels", self.containerrels)
            zf.writestr("docProps/core.xml", self.convert_metadata(mi))
            zf.writestr("docProps/app.xml", self.appproperties)
            zf.writestr("word/webSettings.xml", self.websettings)
            zf.writestr("word/document.xml", xml2str(self.document))
            zf.writestr("word/styles.xml", xml2str(self.styles))
            zf.writestr("word/numbering.xml", xml2str(self.numbering))
            zf.writestr("word/fontTable.xml", xml2str(self.font_table))
            zf.writestr("word/_rels/document.xml.rels", self.document_relationships.serialize())
            zf.writestr("word/_rels/fontTable.xml.rels", xml2str(self.embedded_fonts))
            for fname, data_getter in iteritems(self.images):
                zf.writestr(fname, data_getter())
            for fname, data in iteritems(self.fonts):
                zf.writestr(fname, data)


if __name__ == "__main__":
    d = DOCX(None, None)
    print(d.websettings)
