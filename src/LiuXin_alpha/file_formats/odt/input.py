from __future__ import with_statement

"""
Convert an ODT file into a Open Ebook
"""

import logging
import os
import posixpath

from lxml import etree
try:
    from cssutils import CSSParser, parseString
    from cssutils.css import CSSRule
except ModuleNotFoundError:
    CSSParser = None
    parseString = None

    class CSSRule:  # type: ignore[no-redef]
        STYLE_RULE = 1

from LiuXin_alpha.file_formats.odf.draw import Frame as odFrame, Image as odImage
from LiuXin_alpha.file_formats.odf.namespaces import TEXTNS as odTEXTNS
from LiuXin_alpha.file_formats.odf.odf2xhtml import ODF2XHTML
from LiuXin_alpha.file_formats.odf.opendocument import load as odLoad
from LiuXin_alpha.file_formats.archive_preflight import validate_zip_member_infos
from LiuXin_alpha.file_formats.oeb.base import _css_logger
from LiuXin_alpha.metadata.file_sources.odt import get_metadata as odt_get_metadata

from LiuXin_alpha.utils.calibre import CurrentDir, walk
from LiuXin_alpha.utils.localization import trans as _

# Py2/Py3 compatibility layer
from LiuXin_alpha.utils.libraries.liuxin_six import six_string_types

__license__ = "GPL v3"
__copyright__ = "2008, Kovid Goyal kovid@kovidgoyal.net"
__docformat__ = "restructuredtext en"


class Extract(ODF2XHTML):
    required_members = ("META-INF/manifest.xml", "meta.xml", "content.xml")
    max_archive_members = 4096
    max_member_uncompressed_size = 256 * 1024 * 1024
    max_total_uncompressed_size = 512 * 1024 * 1024
    max_compression_ratio = 1000
    min_compression_ratio_check_size = 1024 * 1024

    def validate_container_members(self, stream):
        from LiuXin_alpha.utils.libraries.calibre_zipfile import ZipFile

        stream.seek(0)
        zf = ZipFile(stream, "r")
        try:
            names = set(
                validate_zip_member_infos(
                    zf.infolist(),
                    container_label="ODT file",
                    member_label="ODT archive",
                    error_type=ValueError,
                    # ODT picture extraction skips unsafe Pictures entries
                    # instead of rejecting the whole document.
                    allow_unsafe_paths=True,
                    max_archive_members=self.max_archive_members,
                    max_member_uncompressed_size=self.max_member_uncompressed_size,
                    max_total_uncompressed_size=self.max_total_uncompressed_size,
                    max_compression_ratio=self.max_compression_ratio,
                    min_compression_ratio_check_size=self.min_compression_ratio_check_size,
                )
            )
            missing = [name for name in self.required_members if name not in names]
            if missing:
                raise ValueError("ODT file is missing required member(s): %s" % ", ".join(missing))
        finally:
            zf.close()
            stream.seek(0)

    def extract_pictures(self, zf):
        if not os.path.exists("Pictures"):
            os.makedirs("Pictures")
        pictures_root = os.path.abspath("Pictures")
        for name in zf.namelist():
            normalized = posixpath.normpath(name.replace("\\", "/"))
            if normalized == "Pictures" or not normalized.startswith("Pictures/"):
                continue
            relative_name = normalized[len("Pictures/") :]
            target = os.path.abspath(os.path.join(pictures_root, *relative_name.split("/")))
            try:
                common = os.path.commonpath([pictures_root, target])
            except ValueError:
                continue
            if common != pictures_root:
                continue
            parent = os.path.dirname(target)
            if parent and not os.path.exists(parent):
                os.makedirs(parent)
            data = zf.read(name)
            with open(target, "wb") as f:
                f.write(data)

    def fix_markup(self, html, log):
        root = etree.fromstring(html)
        self.filter_css(root, log)
        self.extract_css(root, log)
        self.epubify_markup(root, log)
        html = etree.tostring(root, encoding="utf-8", xml_declaration=True)
        if isinstance(html, bytes):
            html = html.decode("utf-8", "replace")
        return html

    def extract_css(self, root, log):
        ans = []
        for s in root.xpath('//*[local-name() = "style" and @type="text/css"]'):
            ans.append(s.text)
            s.getparent().remove(s)

        head = root.xpath('//*[local-name() = "head"]')
        if head:
            head = head[0]
            ns = head.nsmap.get(None, "")
            if ns:
                ns = "{%s}" % ns
            etree.SubElement(
                head,
                ns + "link",
                {"type": "text/css", "rel": "stylesheet", "href": "odfpy.css"},
            )

        css = "\n\n".join(ans)
        self.css = None
        if CSSParser is not None:
            parser = CSSParser(loglevel=logging.WARNING, log=_css_logger)
            self.css = parser.parseString(css, validate=False)
        else:
            log.warning("cssutils is not available; ODT CSS optimization/filtering is disabled.")

        with open("odfpy.css", "wb") as f:
            f.write(css.encode("utf-8"))

    def get_css_for_class(self, cls):
        if not cls:
            return None
        if self.css is None:
            return None
        for rule in self.css.cssRules.rulesOfType(CSSRule.STYLE_RULE):
            for sel in rule.selectorList:
                q = sel.selectorText
                if q == "." + cls:
                    return rule

    def epubify_markup(self, root, log):
        from LiuXin_alpha.file_formats.oeb.base import XPath, XHTML

        # Fix empty title tags
        for t in XPath("//h:title")(root):
            if not t.text:
                t.text = " "
        # Fix <p><div> constructs as the asinine epubchecker complains about them
        pdiv = XPath("//h:p/h:div")
        for div in pdiv(root):
            div.getparent().tag = XHTML("div")

        # Remove the position:relative as it causes problems with some epub
        # renderers. Remove display: block on an image inside a div as it is
        # redundant and prevents text-align:center from working in ADE
        # Also ensure that the img is contained in its containing div
        imgpath = XPath("//h:div/h:img[@style]")
        for img in imgpath(root):
            div = img.getparent()
            if len(div) == 1:
                style = div.attrib.get("style", "")
                if style and not style.endswith(";"):
                    style += ";"
                style += "position:static"  # Ensures position of containing div is static
                # Ensure that the img is always contained in its frame
                div.attrib["style"] = style
                img.attrib["style"] = "max-width: 100%; max-height: 100%"

        # Handle anchored images. The default markup + CSS produced by
        # odf2xhtml works with WebKit but not with ADE. So we convert the
        # common cases of left/right/center aligned block images to work on
        # both webkit and ADE. We detect the case of setting the side margins
        # to auto and map it to an appropriate text-align directive, which
        # works in both WebKit and ADE.
        # https://bugs.launchpad.net/bugs/1063207
        # https://bugs.launchpad.net/calibre/+bug/859343
        imgpath = XPath("descendant::h:div/h:div/h:img")
        for img in imgpath(root):
            div2 = img.getparent()
            div1 = div2.getparent()
            if (len(div1), len(div2)) != (1, 1):
                continue
            cls = div1.get("class", "")
            first_rules = filter(None, [self.get_css_for_class(x) for x in cls.split()])
            has_align = False
            for r in first_rules:
                if r.style.getProperty("text-align") is not None:
                    has_align = True
            ml = mr = None
            if not has_align:

                aval = None
                cls = div2.get("class", "")
                rules = filter(None, [self.get_css_for_class(x) for x in cls.split()])

                for r in rules:
                    ml = r.style.getPropertyCSSValue("margin-left") or ml
                    mr = r.style.getPropertyCSSValue("margin-right") or mr
                    ml = getattr(ml, "value", None)
                    mr = getattr(mr, "value", None)

                if ml == mr == "auto":
                    aval = "center"
                elif ml == "auto" and mr != "auto":
                    aval = "right"
                elif ml != "auto" and mr == "auto":
                    aval = "left"

                if aval is not None:
                    style = div1.attrib.get("style", "").strip()
                    if style and not style.endswith(";"):
                        style += ";"
                    style += "text-align:%s" % aval
                    has_align = True
                    div1.attrib["style"] = style

            if has_align:
                # This is needed for ADE, without it the text-align has no effect
                style = div2.attrib["style"]
                div2.attrib["style"] = "display:inline;" + style

    def filter_css(self, root, log):
        if CSSParser is None:
            return
        style = root.xpath('//*[local-name() = "style" and @type="text/css"]')
        if style:
            style = style[0]
            css = style.text
            if css:
                css, sel_map = self.do_filter_css(css)
                if isinstance(css, bytes):
                    css = css.decode("utf-8", "ignore")
                style.text = css
                for x in root.xpath("//*[@class]"):
                    extra = []
                    orig = x.get("class")
                    for cls in orig.split():
                        extra.extend(sel_map.get(cls, []))
                    if extra:
                        x.set("class", orig + " " + " ".join(extra))

    def do_filter_css(self, css):
        if parseString is None:
            return css, {}

        if isinstance(css, bytes):
            css = css.decode("utf-8", "ignore")

        sheet = parseString(css, validate=False)
        rules = list(sheet.cssRules.rulesOfType(CSSRule.STYLE_RULE))
        sel_map = {}
        count = 0
        for r in rules:
            # Check if we have only class selectors for this rule
            nc = [x for x in r.selectorList if not x.selectorText.startswith(".")]
            if len(r.selectorList) > 1 and not nc:
                # Replace all the class selectors with a single class selector
                # This will be added to the class attribute of all elements
                # that have one of these selectors.
                replace_name = "c_odt%d" % count
                count += 1
                for sel in r.selectorList:
                    s = sel.selectorText[1:]
                    if s not in sel_map:
                        sel_map[s] = []
                    sel_map[s].append(replace_name)
                r.selectorText = "." + replace_name
        css_text = sheet.cssText
        if isinstance(css_text, bytes):
            css_text = css_text.decode("utf-8", "ignore")
        return css_text, sel_map

    def search_page_img(self, mi, log):
        for frm in self.document.topnode.getElementsByType(odFrame):
            try:
                if frm.getAttrNS(odTEXTNS, "anchor-type") == "page":
                    log.warn("Document has Pictures anchored to Page, will all end up before first page!")
                    break
            except ValueError:
                pass

    def filter_cover(self, mi, log):
        # filter the Element tree (remove the detected cover)
        if mi.cover and mi.odf_cover_frame:
            for frm in self.document.topnode.getElementsByType(odFrame):
                # search the right frame
                if frm.getAttribute("name") == mi.odf_cover_frame:
                    img = frm.getElementsByType(odImage)
                    # only one draw:image allowed in the draw:frame
                    if len(img) == 1 and img[0].getAttribute("href") == mi.cover:
                        # ok, this is the right frame with the right image
                        # check if there are more childs
                        if len(frm.childNodes) != 1:
                            break
                        # check if the parent paragraph more childs
                        para = frm.parentNode
                        if para.tagName != "text:p" or len(para.childNodes) != 1:
                            break
                        # now it should be safe to remove the text:p
                        parent = para.parentNode
                        parent.removeChild(para)
                        log("Removed cover image paragraph from document...")
                        break

    def filter_load(self, odffile, mi, log):
        """
        This is an adaption from ODF2XHTML. It adds a step between load and parse of the document where the Element
        tree can be modified.
        :param odffile:
        :param mi:
        :param log:
        :return:
        """
        # first load the odf structure
        self.lines = []
        self._wfunc = self._wlines
        if isinstance(odffile, six_string_types) or hasattr(odffile, "read"):  # Added by Kovid
            self.document = odLoad(odffile)
        else:
            self.document = odffile
        # filter stuff
        self.search_page_img(mi, log)
        try:
            self.filter_cover(mi, log)
        except:
            pass
        # parse the modified tree and generate xhtml
        self._walknode(self.document.topnode)

    def __call__(self, stream, odir, log):
        from LiuXin_alpha.file_formats.opf.opf2 import OPFCreator

        from LiuXin_alpha.utils.libraries.calibre_zipfile import ZipFile

        if not os.path.exists(odir):
            os.makedirs(odir)
        with CurrentDir(odir):
            log("Extracting ODT file...")
            stream.seek(0)
            self.validate_container_members(stream)
            mi = odt_get_metadata(stream)
            if not mi.title:
                mi.title = _("Unknown")
            if not mi.authors:
                mi.authors = [_("Unknown")]
            self.filter_load(stream, mi, log)
            html = self.xhtml()
            # A blanket img specification like this causes problems
            # with EPUB output as the containing element often has
            # an absolute height and width set that is larger than
            # the available screen real estate
            html = html.replace("img { width: 100%; height: 100%; }", "")
            # odf2xhtml creates empty title tag
            html = html.replace("<title></title>", "<title>%s</title>" % (mi.title,))

            try:
                html = self.fix_markup(html, log)
            except Exception as e:
                log.exception(
                    "Failed to filter CSS, conversion may be slow " "- exception message: {}".format(e)
                )

            with open("index.xhtml", "wb") as f:
                if isinstance(html, str):
                    html = html.encode("utf-8")
                f.write(html)
            zf = ZipFile(stream, "r")
            self.extract_pictures(zf)
            cwd = os.getcwd()
            opf = OPFCreator(os.path.abspath(cwd), mi)
            opf.create_manifest([(os.path.abspath(f2), None) for f2 in walk(cwd)])
            opf.create_spine([os.path.abspath("index.xhtml")])
            with open("metadata.opf", "wb") as f:
                opf.render(f)
            return os.path.abspath("metadata.opf")
