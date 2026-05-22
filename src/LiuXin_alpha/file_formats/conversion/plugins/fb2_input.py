from __future__ import with_statement

"""
Convert .fb2 files to .lrf
"""

import base64
import binascii
import os
import re

from LiuXin_alpha.customize.conversion import InputFormatPlugin, OptionRecommendation
from LiuXin_alpha.file_formats.conversion.plugins._workdir import (
    choose_conversion_workdir,
)

from LiuXin_alpha.utils.calibre import CurrentDir
from LiuXin_alpha.utils.calibre import guess_type
from LiuXin_alpha.utils.libraries.liuxin_etree import LXML_AVAILABLE, etree
from LiuXin_alpha.utils.localization import trans as _
from LiuXin_alpha.utils.logging import default_log
from LiuXin_alpha.utils.resources import P

# Py2/Py3
from LiuXin_alpha.utils.libraries.liuxin_six import dict_iteritems as iteritems
from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode

__license__ = "GPL v3"
__copyright__ = "2008, Anatoly Shipitsin <norguhtar at gmail.com>"

FB2NS = "http://www.gribuser.ru/xml/fictionbook/2.0"
FB21NS = "http://www.gribuser.ru/xml/fictionbook/2.1"


def _get_fb2_metadata(stream, file_ext):
    """
    Resolve metadata using the legacy path when available, with a fallback to
    the metadata reader plugin registry.
    """
    try:
        from LiuXin_alpha.metadata.meta import get_metadata as legacy_get_metadata
    except Exception:
        from LiuXin_alpha.customize.ui import get_file_type_metadata

        return get_file_type_metadata(stream, file_ext, calibre=True)
    return legacy_get_metadata(stream, file_ext)


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

    def _warn(self, message):
        log = getattr(self, "log", None)
        warn = getattr(log, "warning", None) or getattr(log, "warn", None)
        if warn is not None:
            warn(message)

    def embedded_binary_filename_is_unsafe(self, name):
        if not name:
            return True
        normalized = str(name).replace("\\", "/")
        parts = normalized.split("/")
        return (
            "\\" in str(name)
            or "/" in normalized
            or normalized.startswith("/")
            or (len(normalized) > 1 and normalized[1] == ":")
            or normalized in {".", ".."}
            or ".." in parts
            or os.path.isabs(str(name))
        )

    def safe_embedded_binary_filename(self, binary_id, content_type, index, used_names):
        original_name = str(binary_id or "").strip()
        candidate = original_name
        content_ext = str(content_type or "").rpartition("/")[-1].lower()
        if content_ext == "jpeg":
            content_ext = "jpg"
        image_exts = {"jpg", "jpeg", "png"}

        if content_ext in image_exts and candidate.lower().rpartition(".")[-1] not in image_exts:
            candidate += "." + content_ext

        if self.embedded_binary_filename_is_unsafe(candidate):
            basename = original_name.replace("\\", "/").rsplit("/", 1)[-1]
            existing_ext = os.path.splitext(basename)[1]
            if content_ext in image_exts:
                suffix = "." + content_ext
            elif existing_ext and len(existing_ext) <= 16:
                suffix = existing_ext
            else:
                suffix = ".bin"
            candidate = "fb2_binary_%04d%s" % (index, suffix)
            self._warn(
                _("FB2 embedded binary id has unsafe filename; using %s for %s")
                % (candidate, original_name)
            )

        root, ext = os.path.splitext(candidate)
        unique_candidate = candidate
        counter = 1
        while unique_candidate.casefold() in used_names:
            unique_candidate = "%s_%d%s" % (root, counter, ext)
            counter += 1
        used_names.add(unique_candidate.casefold())
        return unique_candidate

    def decode_embedded_binary(self, raw, binary_id):
        if isinstance(raw, bytes):
            compact = b"".join(raw.split())
        else:
            try:
                compact = "".join(str(raw).split()).encode("ascii")
            except UnicodeEncodeError:
                self._warn(_("Binary data with id=%s is corrupted, ignoring") % binary_id)
                return None
        try:
            return base64.b64decode(compact, validate=True)
        except (binascii.Error, TypeError, ValueError):
            self._warn(_("Binary data with id=%s is corrupted, ignoring") % binary_id)
            return None

    def convert(self, stream, options, file_ext, log, accelerators):
        from LiuXin_alpha.file_formats.chardet import xml_to_unicode
        from LiuXin_alpha.file_formats.oeb.base import XLINK_NS, XHTML_NS, RECOVER_PARSER
        from LiuXin_alpha.file_formats.opf.opf2 import OPFCreator

        if not LXML_AVAILABLE or getattr(etree, "XSLT", None) is None:
            raise RuntimeError("FB2 input conversion requires lxml with XSLT support")

        self.log = log

        log.debug("Parsing XML...")
        raw = stream.read()
        if isinstance(raw, bytes):
            raw = xml_to_unicode(raw, strip_encoding_pats=True, assume_utf8=True, resolve_entities=True)[0]
        else:
            raw = str(raw)
        raw = raw.replace("\0", "")

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
            import logging
            try:
                import cssutils
            except ModuleNotFoundError:
                cssutils = None

            if cssutils is not None:
                parser = cssutils.CSSParser(fetcher=None, log=logging.getLogger("calibre.css"))

                xhtml_css_namespace = '@namespace "%s";\n' % XHTML_NS
                text = xhtml_css_namespace + css
                log.debug("Parsing stylesheet...")
                stylesheet = parser.parseString(text)
                stylesheet.namespaces["h"] = XHTML_NS
                css = six_unicode(stylesheet.cssText).replace("h|style", "h|span")
            else:
                log.warn("cssutils is unavailable, using embedded CSS without namespace normalization")
            css = re.sub(r"name\s*=\s*", "class=", css)

        work_root = choose_conversion_workdir("_fb2_input")
        with CurrentDir(work_root):
            self.extract_embedded_content(doc)
            log.debug("Converting XML to HTML...")
            with open(P("templates/fb2.xsl"), "rb") as template_file:
                ss = template_file.read()
            ss = ss.replace(b"__FB_NS__", fb_ns.encode("utf-8"))
            if options.no_inline_fb2_toc:
                log("Disabling generation of inline FB2 TOC")
                ss = re.compile(br"<!-- BUILD TOC -->.*<!-- END BUILD TOC -->", re.DOTALL).sub(b"", ss)

            styledoc = etree.fromstring(ss)

            transform = etree.XSLT(styledoc)
            result = transform(doc)

            # Handle links of type note and cite
            notes = {
                a.get("href")[1:]: a
                for a in result.xpath("//a[@link_note and @href]")
                if a.get("href").startswith("#")
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
            if isinstance(index, str):
                index = index.encode("utf-8")
            with open("index.xhtml", "wb") as bin_index_html:
                bin_index_html.write(index)
            with open("inline-styles.css", "wb") as bin_css_file:
                bin_css_file.write(css.encode("utf-8"))
            stream.seek(0)
            mi = _get_fb2_metadata(stream, "fb2")
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
                        href = self.binary_map.get(href, href)
                        if self.embedded_binary_filename_is_unsafe(href):
                            self._warn(_("FB2 cover image reference has unsafe path: %s") % href)
                            continue
                        if not os.path.exists(href):
                            self._warn(_("FB2 cover image was not extracted: %s") % href)
                            continue
                        cpath = os.path.abspath(href)
                        break

            opf = OPFCreator(os.getcwd(), mi)
            entries = [(f2, guess_type(f2)[0]) for f2 in os.listdir(".")]
            opf.create_manifest(entries)
            opf.create_spine(["index.xhtml"])
            if cpath:
                opf.guide.set_cover(cpath)
            with open("metadata.opf", "wb") as f:
                opf.render(f)
            return os.path.join(os.getcwd(), "metadata.opf")

    def extract_embedded_content(self, doc):
        """
        Extract and decode content embedded in the document.
        :param doc:
        :return:
        """

        self.binary_map = {}
        used_names = set()
        binary_index = 0
        for elem in doc.xpath("./*"):
            if elem.text and "binary" in elem.tag and "id" in elem.attrib:
                binary_index += 1
                ct = elem.get("content-type", "")
                binary_id = elem.attrib["id"]
                fname = self.safe_embedded_binary_filename(binary_id, ct, binary_index, used_names)
                self.binary_map[binary_id] = fname
                raw = elem.text.strip()
                data = self.decode_embedded_binary(raw, binary_id)
                if data is None:
                    continue
                with open(fname, "wb") as f:
                    f.write(data)
