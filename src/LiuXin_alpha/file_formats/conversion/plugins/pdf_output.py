# -*- coding: utf-8 -*-

"""
Convert OEB ebook format to PDF.
"""

import glob
import os

from LiuXin_alpha.constants import iswindows, islinux
from LiuXin_alpha.customize.conversion import OutputFormatPlugin, OptionRecommendation

from LiuXin_alpha.utils.icu import upper as icu_upper
from LiuXin_alpha.utils.icu import lower as icu_lower
from LiuXin_alpha.utils.localization import trans as _
from LiuXin_alpha.utils.ptempfiles import TemporaryDirectory

# Py2/Py3
from LiuXin_alpha.utils.lx_libraries.liuxin_six import six_unicode

__license__ = "GPL 3"
__copyright__ = "2012, Kovid Goyal <kovid at kovidgoyal.net>"
__docformat__ = "restructuredtext en"

UNITS = [
    "millimeter",
    "centimeter",
    "point",
    "inch",
    "pica",
    "didot",
    "cicero",
    "devicepixel",
]

PAPER_SIZES = [
    "a0",
    "a1",
    "a2",
    "a3",
    "a4",
    "a5",
    "a6",
    "b0",
    "b1",
    "b2",
    "b3",
    "b4",
    "b5",
    "b6",
    "legal",
    "letter",
]


class PDFMetadata(object):  # {{{
    def __init__(self, mi=None):
        from LiuXin_alpha.utils.calibre import force_unicode
        from LiuXin_alpha.metadata.ebook_metadata_tools import authors_to_string

        self.title = _("Unknown")
        self.author = _("Unknown")
        self.tags = ""
        self.mi = mi

        if mi is not None:
            if mi.title:
                self.title = mi.title
            if mi.authors:
                self.author = authors_to_string(mi.authors)
            if mi.tags:
                self.tags = ", ".join(mi.tags)

        self.title = force_unicode(self.title)
        self.author = force_unicode(self.author)


# }}}


class PDFOutput(OutputFormatPlugin):

    name = "PDF Output"
    author = "Kovid Goyal"
    file_type = "pdf"

    options = {
        OptionRecommendation(
            name="override_profile_size",
            recommended_value=False,
            option_help=_(
                "Normally, the PDF page size is set by the output profile chosen under page "
                "options. This option will cause the  page size settings under PDF Output to "
                "override the  size specified by the output profile."
            ),
        ),
        OptionRecommendation(
            name="unit",
            recommended_value="inch",
            level=OptionRecommendation.LOW,
            short_switch="u",
            choices=UNITS,
            option_help=_(
                "The unit of measure for page sizes. Default is inch. Choices are %s Note: "
                "This does not override the unit for margins!"
            )
            % UNITS,
        ),
        OptionRecommendation(
            name="paper_size",
            recommended_value="letter",
            level=OptionRecommendation.LOW,
            choices=PAPER_SIZES,
            option_help=_(
                "The size of the paper. This size will be overridden when a"
                " non default output profile is used. Default is letter."
                " Choices are %s"
            )
            % PAPER_SIZES,
        ),
        OptionRecommendation(
            name="custom_size",
            recommended_value=None,
            option_help=_(
                "Custom size of the document. Use the form widthxheight EG. `123x321` to specify"
                " the width and height. This overrides any specified paper-size."
            ),
        ),
        OptionRecommendation(
            name="preserve_cover_aspect_ratio",
            recommended_value=False,
            option_help=_(
                "Preserve the aspect ratio of the cover, instead of stretching it to fill the "
                "full first page of the generated pdf."
            ),
        ),
        OptionRecommendation(
            name="pdf_serif_family",
            recommended_value="Liberation Serif" if islinux else "Times New Roman",
            option_help=_("The font family used to render serif fonts"),
        ),
        OptionRecommendation(
            name="pdf_sans_family",
            recommended_value="Liberation Sans" if islinux else "Helvetica",
            option_help=_("The font family used to render sans-serif fonts"),
        ),
        OptionRecommendation(
            name="pdf_mono_family",
            recommended_value="Liberation Mono" if islinux else "Courier New",
            option_help=_("The font family used to render monospaced fonts"),
        ),
        OptionRecommendation(
            name="pdf_standard_font",
            choices=["serif", "sans", "mono"],
            recommended_value="serif",
            option_help=_("The font family used to render monospaced fonts"),
        ),
        OptionRecommendation(
            name="pdf_default_font_size",
            recommended_value=20,
            option_help=_("The default font size"),
        ),
        OptionRecommendation(
            name="pdf_mono_font_size",
            recommended_value=16,
            option_help=_("The default font size for monospaced text"),
        ),
        OptionRecommendation(
            name="pdf_mark_links",
            recommended_value=False,
            option_help=_("Surround all links with a red box, useful for debugging."),
        ),
        OptionRecommendation(
            name="old_pdf_engine",
            recommended_value=False,
            option_help=_("Use the old, less capable engine to generate the PDF"),
        ),
        OptionRecommendation(
            name="uncompressed_pdf",
            recommended_value=False,
            option_help=_("Generate an uncompressed PDF, useful for debugging, " "only works with the new PDF engine."),
        ),
        OptionRecommendation(
            name="pdf_page_numbers",
            recommended_value=False,
            option_help=_(
                "Add page numbers to the bottom of every page in the generated PDF file. "
                "If you specify a footer template, it will take precedence over this option."
            ),
        ),
        OptionRecommendation(
            name="pdf_footer_template",
            recommended_value=None,
            option_help=_(
                "An HTML template used to generate %s on every page. The strings _PAGENUM_,"
                " _TITLE_, _AUTHOR_ and _SECTION_ will be replaced by their current values."
            )
            % _("footers"),
        ),
        OptionRecommendation(
            name="pdf_header_template",
            recommended_value=None,
            option_help=_(
                "An HTML template used to generate %s on every page. The strings _PAGENUM_,"
                " _TITLE_, _AUTHOR_ and _SECTION_ will be replaced by their current values."
            )
            % _("headers"),
        ),
        OptionRecommendation(
            name="pdf_add_toc",
            recommended_value=False,
            option_help=_(
                "Add a Table of Contents at the end of the PDF that lists page numbers. "
                "Useful if you want to print out the PDF. If this PDF is intended for "
                "electronic use, use the PDF Outline instead."
            ),
        ),
        OptionRecommendation(
            name="toc_title",
            recommended_value=None,
            option_help=_("Title for generated table of contents."),
        ),
    }

    def convert(self, oeb_book, output_path, input_plugin, opts, log):

        from io import BytesIO

        from lxml import etree

        from LiuXin_alpha.interfaces.gui2 import must_use_qt, load_builtin_fonts
        from LiuXin_alpha.file_formats.oeb.base import OPF, OPF2_NS

        log.info("Converting OEB to PDF...")

        must_use_qt()
        load_builtin_fonts()

        self.oeb = oeb_book
        self.input_plugin, self.opts, self.log = input_plugin, opts, log
        self.output_path = output_path

        package = etree.Element(
            OPF("package"),
            attrib={"version": "2.0", "unique-identifier": "dummy"},
            nsmap={None: OPF2_NS},
        )

        from LiuXin_alpha.file_formats.opf.opf2 import OPF

        self.oeb.metadata.to_opf2(package)
        self.metadata = OPF(BytesIO(etree.tostring(package))).to_book_metadata()
        self.cover_data = None

        if input_plugin.is_image_collection:
            log.debug("Converting input as an image collection...")
            self.convert_images(input_plugin.get_images())
        else:
            log.debug("Converting input as a text based book...")
            self.convert_text(oeb_book)

    def convert_images(self, images):
        """
        Convert images into PDF format.
        :param images:
        :return:
        """
        from LiuXin_alpha.file_formats.pdf.writer import ImagePDFWriter

        self.write(ImagePDFWriter, images, None)

    def get_cover_data(self):
        oeb = self.oeb
        if oeb.metadata.cover and six_unicode(oeb.metadata.cover[0]) in oeb.manifest.ids:
            cover_id = six_unicode(oeb.metadata.cover[0])
            item = oeb.manifest.ids[cover_id]
            self.cover_data = item.data

    def handle_embedded_fonts(self):
        """
        On windows, Qt uses GDI which does not support OpenType (CFF) fonts, so we need to nuke references to OpenType
        fonts.
        Qt's directwrite text backend is not mature.
        Also make sure all fonts are embeddable.
        """
        from LiuXin_alpha.file_formats.oeb.base import urlnormalize
        from LiuXin_alpha.utils.fonts.utils import remove_embed_restriction
        from PyQt5.Qt import QByteArray, QRawFont

        font_warnings = set()
        processed = set()
        is_cff = {}
        for item in list(self.oeb.manifest):
            if not hasattr(item.data, "cssRules"):
                continue
            remove = set()
            for i, rule in enumerate(item.data.cssRules):
                if rule.type == rule.FONT_FACE_RULE:
                    try:
                        s = rule.style
                        src = s.getProperty("src").propertyValue[0].uri
                    except:
                        continue
                    path = item.abshref(src)
                    ff = self.oeb.manifest.hrefs.get(urlnormalize(path), None)
                    if ff is None:
                        continue

                    raw = nraw = ff.data
                    if path not in processed:
                        processed.add(path)
                        try:
                            nraw = remove_embed_restriction(raw)
                        except:
                            continue
                        if nraw != raw:
                            ff.data = nraw
                            self.oeb.container.write(path, nraw)

                    if iswindows:
                        if path not in is_cff:
                            f = QRawFont(QByteArray(nraw), 12)
                            is_cff[path] = f.isValid() and len(f.fontTable("head")) == 0
                        if is_cff[path]:
                            if path not in font_warnings:
                                font_warnings.add(path)
                                self.log.warn("CFF OpenType fonts are not supported on windows, ignoring: %s" % path)
                            remove.add(i)
            for i in sorted(remove, reverse=True):
                item.data.cssRules.pop(i)

    def convert_text(self, oeb_book):
        from LiuXin_alpha.file_formats.opf.opf2 import OPF

        if self.opts.old_pdf_engine:
            from LiuXin_alpha.file_formats.pdf.writer import PDFWriter

            # PDFWriter  # To make pyflakes shut up
        else:
            self.log.warn("New PDFWriter does not currently work - falling back on the old one")
            from LiuXin_alpha.file_formats.pdf.writer import PDFWriter

        self.log.debug("Serializing oeb input to disk for processing...")
        self.get_cover_data()

        self.handle_embedded_fonts()

        with TemporaryDirectory("_pdf_out") as oeb_dir:
            from LiuXin_alpha.customize.ui import plugin_for_output_format

            oeb_output = plugin_for_output_format("oeb")
            oeb_output.convert(oeb_book, oeb_dir, self.input_plugin, self.opts, self.log)

            opfpath = glob.glob(os.path.join(oeb_dir, "*.opf"))[0]
            opf = OPF(opfpath, os.path.dirname(opfpath))

            self.write(PDFWriter, [s.path for s in opf.spine], getattr(opf, "toc", None))

    def write(self, Writer, items, toc):
        writer = Writer(self.opts, self.log, cover_data=self.cover_data, toc=toc)
        writer.report_progress = self.report_progress

        close = False
        if not hasattr(self.output_path, "write"):
            close = True
            if not os.path.exists(os.path.dirname(self.output_path)) and os.path.dirname(self.output_path) != "":
                os.makedirs(os.path.dirname(self.output_path))
            out_stream = open(self.output_path, "wb")
        else:
            out_stream = self.output_path

        out_stream.seek(0)
        out_stream.truncate()
        self.log.debug("Rendering pages to PDF...")
        import time

        st = time.time()
        if False:
            import cProfile

            cProfile.runctx(
                "writer.dump(items, out_stream, PDFMetadata(self.metadata))",
                globals(),
                locals(),
                "/tmp/profile",
            )
        else:
            writer.dump(items, out_stream, PDFMetadata(self.metadata))
        self.log("Rendered PDF in %g seconds:" % (time.time() - st))

        if close:
            out_stream.close()

    def specialize_css_for_output(self, log, opts, item, stylizer):
        """
        Qt WebKit (4.8.x) cannot handle font-variant: small-caps. It tries to fake the small caps,
        which is ok, but the faking continues on to subsequent text that should not be in small-caps.
        So we workaround the problem by faking small caps ourselves. A minimal example that Qt chokes on:
        <html><body>
        <p style="font-variant:small-caps">Some Small-caps Text</p>
        <p style="text-align:justify">Some non small-caps text with enough text for at least one
        full line and justification enabled. Both of these are needed for the example to work.</p>
        </body></html>
        :param log:
        :param opts:
        :param item:
        :param stylizer:
        :return:
        """
        from LiuXin_alpha.file_formats.oeb.base import XHTML
        import itertools
        import string

        if not hasattr(item.data, "xpath"):
            return
        ws = six_unicode(string.whitespace)

        def fake_small_caps(elem):
            spans = []
            for lowercase, textiter in itertools.groupby(elem.text, lambda x: x not in ws and icu_lower(x) == x):
                text = "".join(textiter)
                if lowercase:
                    text = icu_upper(text)
                span = elem.makeelement(XHTML("span"))
                span.text = text
                style = stylizer.style(span)
                if lowercase:
                    style.set("font-size", "0.65em")
                spans.append(span)
            elem.text = None
            elem[0:] = spans

        def process_elem(elem, parent_fv=None):
            children = tuple(elem)
            style = stylizer.style(elem)
            fv = style.drop("font-variant")
            if not fv or fv.lower() == "inherit":
                fv = parent_fv
            if fv and fv.lower() in {"smallcaps", "small-caps"}:
                if elem.text:
                    fake_small_caps(elem)
            for child in children:
                if hasattr(getattr(child, "tag", None), "lower"):
                    process_elem(child, parent_fv=fv)

        for body in item.data.xpath('//*[local-name()="body"]'):
            process_elem(body)
