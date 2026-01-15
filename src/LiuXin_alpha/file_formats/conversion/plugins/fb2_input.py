from __future__ import with_statement

"""
Convert .fb2 files to .lrf
"""

import os
import re

from LiuXin.customize.conversion import InputFormatPlugin, OptionRecommendation

from LiuXin.utils.calibre import guess_type
from LiuXin.utils.localization import trans as _
from LiuXin.utils.logger import default_log
from LiuXin.utils.resources import P

# Py2/Py3
from LiuXin.utils.lx_libraries.liuxin_six import dict_iteritems as iteritems
from LiuXin.utils.lx_libraries.liuxin_six import six_unicode

__license__ = "GPL v3"
__copyright__ = "2008, Anatoly Shipitsin <norguhtar at gmail.com>"

FB2NS = "http://www.gribuser.ru/xml/fictionbook/2.0"
FB21NS = "http://www.gribuser.ru/xml/fictionbook/2.1"


class FB2Input(InputFormatPlugin):

    name = "FB2 Input"
    author = "Anatoly Shipitsin"
    description = "Convert FB2 files to HTML"
    file_types = {"fb2"}

    recommendations = {
        ("level1_toc", "//h:h1", OptionRecommendation.MED),
        ("level2_toc", "//h:h2", OptionRecommendation.MED),
        ("level3_toc", "//h:h3", OptionRecommendation.MED),
    }

    options = {
        OptionRecommendation(
            name="no_inline_fb2_toc",
            recommended_value=False,
            level=OptionRecommendation.LOW,
            option_help=_("Do not insert a Table of Contents at the beginning of the book."),
        ),
    }

    def convert(self, stream, options, file_ext, log, accelerators):

        from lxml import etree

        from LiuXin.metadata.meta import get_metadata
        from LiuXin.file_formats.chardet import xml_to_unicode
        from LiuXin.file_formats.oeb.base import XLINK_NS, XHTML_NS, RECOVER_PARSER
        from LiuXin.file_formats.opf.opf2 import OPFCreator

        self.log = log

        log.debug("Parsing XML...")
        raw = stream.read().replace("\0", "")
        raw = xml_to_unicode(raw, strip_encoding_pats=True, assume_utf8=True, resolve_entities=True)[0]

        try:
            doc = etree.fromstring(raw)
        except etree.XMLSyntaxError as e:
            info_str = "Error while trying to parse XML string"
            default_log.log_exception(info_str, e, "INFO")
            try:
                doc = etree.fromstring(raw, parser=RECOVER_PARSER)
                if doc is None:
                    raise Exception("parse failed")
            except Exception as e:
                info_str = "Another error while trying to parse XML string.\n"
                info_str += "Falling back to defaults.\n"
                default_log.log_exception(info_str, e, "INFO")
                doc = etree.fromstring(raw.replace("& ", "&amp;"), parser=RECOVER_PARSER)

        if doc is None:
            raise ValueError("The FB2 file is not valid XML")

        try:
            fb_ns = doc.nsmap[doc.prefix]
        except Exception as e:
            info_str = "Unable to read nsmap while processing fb2 file"
            default_log.log_exception(info_str, e, "INFO")
            fb_ns = FB2NS

        namespaces = {"f": fb_ns, "l": XLINK_NS}
        stylesheets = doc.xpath('//*[local-name() = "stylesheet" and @type="text/css"]')
        css = ""
        for s in stylesheets:
            css += etree.tostring(s, encoding=six_unicode, method="text", with_tail=False) + "\n\n"
        if css:
            import cssutils
            import logging

            parser = cssutils.CSSParser(fetcher=None, log=logging.getLogger("calibre.css"))

            xhtml_css_namespace = '@namespace "%s";\n' % XHTML_NS
            text = xhtml_css_namespace + css
            log.debug("Parsing stylesheet...")
            stylesheet = parser.parseString(text)
            stylesheet.namespaces["h"] = XHTML_NS
            css = six_unicode(stylesheet.cssText).replace("h|style", "h|span")
            css = re.sub(r"name\s*=\s*", "class=", css)

        self.extract_embedded_content(doc)
        log.debug("Converting XML to HTML...")
        with open(P("templates/fb2.xsl"), "rb") as template_file:
            ss = template_file.read()
        ss = ss.replace("__FB_NS__", fb_ns)
        if options.no_inline_fb2_toc:
            log("Disabling generation of inline FB2 TOC")
            ss = re.compile(r"<!-- BUILD TOC -->.*<!-- END BUILD TOC -->", re.DOTALL).sub("", ss)

        styledoc = etree.fromstring(ss)

        transform = etree.XSLT(styledoc)
        result = transform(doc)

        # Handle links of type note and cite
        notes = {
            a.get("href")[1:]: a for a in result.xpath("//a[@link_note and @href]") if a.get("href").startswith("#")
        }
        cites = {a.get("link_cite"): a for a in result.xpath("//a[@link_cite]") if not a.get("href", "")}
        all_ids = {x for x in result.xpath("//*/@id")}
        for cite, a in iteritems(cites):
            note = notes.get(cite, None)
            if note:
                c = 1
                while "cite%d" % c in all_ids:
                    c += 1
                if not note.get("id", None):
                    note.set("id", "cite%d" % c)
                    all_ids.add(note.get("id"))
                a.set("href", "#%s" % note.get("id"))
        for x in result.xpath("//*[@link_note or @link_cite]"):
            x.attrib.pop("link_note", None)
            x.attrib.pop("link_cite", None)

        for img in result.xpath("//img[@src]"):
            src = img.get("src")
            img.set("src", self.binary_map.get(src, src))
        index = transform.tostring(result)
        with open("index.xhtml", "wb") as bin_index_html:
            bin_index_html.write(index)
        with open("inline-styles.css", "wb") as bin_css_file:
            bin_css_file.write(css)
        stream.seek(0)
        mi = get_metadata(stream, "fb2")
        if not mi.title:
            mi.title = _("Unknown")
        if not mi.authors:
            mi.authors = [_("Unknown")]
        cpath = None
        if mi.cover_data and mi.cover_data[1]:
            with open("fb2_cover_calibre_mi.jpg", "wb") as f:
                f.write(mi.cover_data[1])
            cpath = os.path.abspath("fb2_cover_calibre_mi.jpg")
        else:
            for img in doc.xpath("//f:coverpage/f:image", namespaces=namespaces):
                href = img.get("{%s}href" % XLINK_NS, img.get("href", None))
                if href is not None:
                    if href.startswith("#"):
                        href = href[1:]
                    cpath = os.path.abspath(href)
                    break

        opf = OPFCreator(os.getcwdu(), mi)
        entries = [(f2, guess_type(f2)[0]) for f2 in os.listdir(".")]
        opf.create_manifest(entries)
        opf.create_spine(["index.xhtml"])
        if cpath:
            opf.guide.set_cover(cpath)
        with open("metadata.opf", "wb") as f:
            opf.render(f)
        return os.path.join(os.getcwdu(), "metadata.opf")

    def extract_embedded_content(self, doc):
        """
        Extract and decode content embedded in the document.
        :param doc:
        :return:
        """

        from LiuXin.file_formats.fb2 import base64_decode

        self.binary_map = {}
        for elem in doc.xpath("./*"):
            if elem.text and "binary" in elem.tag and "id" in elem.attrib:
                ct = elem.get("content-type", "")
                fname = elem.attrib["id"]
                ext = ct.rpartition("/")[-1].lower()
                if ext in ("png", "jpeg", "jpg"):
                    if fname.lower().rpartition(".")[-1] not in {"jpg", "jpeg", "png"}:
                        fname += "." + ext
                    self.binary_map[elem.get("id")] = fname
                raw = elem.text.strip()
                try:
                    data = base64_decode(raw)
                except TypeError:
                    self.log.exception("Binary data with id=%s is corrupted, ignoring" % (elem.get("id")))
                else:
                    with open(fname, "wb") as f:
                        f.write(data)
