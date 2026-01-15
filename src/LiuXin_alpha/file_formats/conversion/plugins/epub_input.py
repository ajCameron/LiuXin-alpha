from __future__ import with_statement

import os
import re
from itertools import cycle

from LiuXin.customize.conversion import InputFormatPlugin, OptionRecommendation

from LiuXin.utils.logger import default_log

__license__ = "GPL 3"
__copyright__ = "2009, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"


ADOBE_OBFUSCATION = "http://ns.adobe.com/pdf/enc#RC"
IDPF_OBFUSCATION = "http://www.idpf.org/2008/embedding"


def decrypt_font_data(key, data, algorithm):
    is_adobe = algorithm == ADOBE_OBFUSCATION
    crypt_len = 1024 if is_adobe else 1040
    crypt = bytearray(data[:crypt_len])
    key = cycle(iter(bytearray(key)))
    decrypt = bytes(bytearray(x ^ key.next() for x in crypt))
    return decrypt + data[crypt_len:]


def decrypt_font(key, path, algorithm):
    with open(path, "r+b") as f:
        data = decrypt_font_data(key, f.read(), algorithm)
        f.seek(0), f.truncate(), f.write(data)


class EPUBInput(InputFormatPlugin):

    name = "EPUB Input"
    author = "Kovid Goyal"
    description = "Convert EPUB files (.epub) to HTML"
    file_types = {"epub"}
    output_encoding = None

    recommendations = {("page_breaks_before", "/", OptionRecommendation.MED)}

    def process_encryption(self, encfile, opf, log):
        from lxml import etree
        import uuid
        import hashlib

        idpf_key = opf.raw_unique_identifier
        if idpf_key:
            idpf_key = re.sub("\u0020\u0009\u000d\u000a", "", idpf_key)
            idpf_key = hashlib.sha1(idpf_key.encode("utf-8")).digest()
        key = None
        for item in opf.identifier_iter():
            scheme = None
            for xkey in item.attrib.keys():
                if xkey.endswith("scheme"):
                    scheme = item.get(xkey)
            if (scheme and scheme.lower() == "uuid") or (item.text and item.text.startswith("urn:uuid:")):
                try:
                    key = bytes(item.text).rpartition(":")[-1]
                    key = uuid.UUID(key).bytes
                except Exception as e:
                    err_message = "Unable to handle epub encryption."
                    default_log.log_exception(message=err_message, exception=e, level="INFO")
                    key = None

        try:
            root = etree.parse(encfile)
            for em in root.xpath('descendant::*[contains(name(), "EncryptionMethod")]'):
                algorithm = em.get("Algorithm", "")
                if algorithm not in {ADOBE_OBFUSCATION, IDPF_OBFUSCATION}:
                    return False
                cr = em.getparent().xpath('descendant::*[contains(name(), "CipherReference")]')[0]
                uri = cr.get("URI")
                path = os.path.abspath(os.path.join(os.path.dirname(encfile), "..", *uri.split("/")))
                tkey = key if algorithm == ADOBE_OBFUSCATION else idpf_key
                if tkey and os.path.exists(path):
                    self._encrypted_font_uris.append(uri)
                    decrypt_font(tkey, path, algorithm)
            return True
        except Exception as e:
            err_message = "Unable to handle epub encryption."
            default_log.log_exception(message=err_message, exception=e, level="INFO")

        return False

    def rationalize_cover(self, opf, log):
        """
        Ensure that the cover information in the guide is correct. That means, at most one entry with type="cover" that
        points to a raster cover and at most one entry with type="titlepage" that points to an HTML titlepage.
        :param opf: OPF to be parsed
        :param log: A log object for notes to be written to
        :return:
        """
        removed = None
        from lxml import etree

        guide_cover, guide_elem = None, None
        for guide_elem in opf.iterguide():
            if guide_elem.get("type", "").lower() == "cover":
                guide_cover = guide_elem.get("href", "").partition("#")[0]
                break
        if not guide_cover:
            return
        spine = list(opf.iterspine())
        if not spine:
            return
        # Check if the cover specified in the guide is also the first element in spine
        idref = spine[0].get("idref", "")
        manifest = list(opf.itermanifest())
        if not manifest:
            return
        elem = [x for x in manifest if x.get("id", "") == idref]
        if not elem or elem[0].get("href", None) != guide_cover:
            return
        log("Found HTML cover", guide_cover)

        # Remove from spine as covers must be treated specially
        if not self.for_viewer:
            if len(spine) == 1:
                log.warn("There is only a single spine item and it is marked as the cover. Removing cover marking.")
                for guide_elem in tuple(opf.iterguide()):
                    if guide_elem.get("type", "").lower() == "cover":
                        guide_elem.getparent().remove(guide_elem)
                return
            else:
                spine[0].getparent().remove(spine[0])
                removed = guide_cover
        else:
            # Ensure the cover is displayed as the first item in the book, some
            # epub files have it set with linear='no' which causes the cover to
            # display in the end
            spine[0].attrib.pop("linear", None)
            opf.spine[0].is_linear = True
        # Ensure that the guide has a cover entry pointing to a raster cover
        # and a titlepage entry pointing to the html titlepage. The titlepage
        # entry will be used by the epub output plugin, the raster cover entry
        # by other output plugins.

        from LiuXin.file_formats.oeb.base import OPF

        # Search for a raster cover identified in the OPF
        raster_cover = opf.raster_cover

        # Set the cover guide entry
        if raster_cover is not None:
            guide_elem.set("href", raster_cover)
        else:
            # Render the titlepage to create a raster cover
            from LiuXin.file_formats import render_html_svg_workaround

            guide_elem.set("href", "calibre_raster_cover.jpg")
            t = etree.SubElement(
                elem[0].getparent(),
                OPF("item"),
                href=guide_elem.get("href"),
                id="calibre_raster_cover",
            )
            t.set("media-type", "image/jpeg")
            if os.path.exists(guide_cover):
                renderer = render_html_svg_workaround(guide_cover, log)
                if renderer is not None:
                    open("calibre_raster_cover.jpg", "wb").write(renderer)

        # Set the titlepage guide entry
        for elem in list(opf.iterguide()):
            if elem.get("type", "").lower() == "titlepage":
                elem.getparent().remove(elem)

        t = etree.SubElement(guide_elem.getparent(), OPF("reference"))
        t.set("type", "titlepage")
        t.set("href", guide_cover)
        t.set("title", "Title Page")
        return removed

    def find_opf(self):
        from lxml import etree

        def attr(n, attr):
            for k, v in n.attrib.items():
                if k.endswith(attr):
                    return v

        try:
            with open("META-INF/container.xml") as f:
                root = etree.fromstring(f.read())
                for r in root.xpath('//*[local-name()="rootfile"]'):
                    if attr(r, "media-type") != "application/oebps-package+xml":
                        continue
                    path = attr(r, "full-path")
                    if not path:
                        continue
                    path = os.path.join(os.getcwdu(), *path.split("/"))
                    if os.path.exists(path):
                        return path
        except Exception as e:
            err_str = "Find opf failed./n"
            default_log.log_exception(message=err_str, exception=e, level="WARN")

    # Todo: Add ConversionError for when generic things go wrong with the conversion
    def convert(self, stream, options, file_ext, log, accelerators):
        """
        Run
        :param stream:
        :param options:
        :param file_ext:
        :param log:
        :param accelerators:
        :return:
        """
        from LiuXin.utils.calibre_utils.calibre_zipfile import ZipFile
        from LiuXin.utils.calibre import walk
        from LiuXin.file_formats import DRMError
        from LiuXin.file_formats.opf.opf2 import OPF

        try:
            zf = ZipFile(stream)
            zf.extractall(os.getcwdu())
        except Exception as e:

            info_str = "EPUB appears to be invalid ZIP file, trying a more forgiving ZIP parser"
            log.exception(info_str)
            err_str = default_log.log_exception(info_str, e, "INFO")
            assert True is False, err_str

            from LiuXin.utils.decompression.localunzip import extractall

            stream.seek(0)
            extractall(stream)

        encfile = os.path.abspath(os.path.join("META-INF", "encryption.xml"))
        opf = self.find_opf()
        if opf is None:
            for f in walk("."):
                if f.lower().endswith(".opf") and "__MACOSX" not in f and not os.path.basename(f).startswith("."):
                    opf = os.path.abspath(f)
                    break
        path = getattr(stream, "name", "stream")

        if opf is None:
            raise ValueError("%s is not a valid EPUB file (could not find opf)" % path)

        opf = os.path.relpath(opf, os.getcwdu())
        parts = os.path.split(opf)
        opf = OPF(opf, os.path.dirname(os.path.abspath(opf)))

        self._encrypted_font_uris = []
        if os.path.exists(encfile):
            if not self.process_encryption(encfile, opf, log):
                raise DRMError(os.path.basename(path))
        self.encrypted_fonts = self._encrypted_font_uris

        if len(parts) > 1 and parts[0]:
            delta = "/".join(parts[:-1]) + "/"
            for elem in opf.itermanifest():
                elem.set("href", delta + elem.get("href"))
            for elem in opf.iterguide():
                elem.set("href", delta + elem.get("href"))

        self.removed_cover = self.rationalize_cover(opf, log)

        self.optimize_opf_parsing = opf
        for x in opf.itermanifest():
            if x.get("media-type", "") == "application/x-dtbook+xml":
                raise ValueError("EPUB files with DTBook markup are not supported")

        not_for_spine = set()
        for y in opf.itermanifest():
            id_ = y.get("id", None)
            if id_ and y.get("media-type", None) in {
                "application/vnd.adobe-page-template+xml",
                "application/vnd.adobe.page-template+xml",
                "application/adobe-page-template+xml",
                "application/adobe.page-template+xml",
                "application/text",
            }:
                not_for_spine.add(id_)

        seen = set()
        for x in list(opf.iterspine()):
            ref = x.get("idref", None)
            if not ref or ref in not_for_spine or ref in seen:
                x.getparent().remove(x)
                continue
            seen.add(ref)

        if len(list(opf.iterspine())) == 0:
            raise ValueError("No valid entries in the spine of this EPUB")

        with open("content.opf", "wb") as nopf:
            nopf.write(opf.render())

        return os.path.abspath("content.opf")

    def postprocess_book(self, oeb, opts, log):
        rc = getattr(self, "removed_cover", None)
        if rc:
            cover_toc_item = None
            for item in oeb.toc.iterdescendants():
                if item.href and item.href.partition("#")[0] == rc:
                    cover_toc_item = item
                    break
            spine = {x.href for x in oeb.spine}
            if cover_toc_item is not None and cover_toc_item not in spine:
                oeb.toc.item_that_refers_to_cover = cover_toc_item
