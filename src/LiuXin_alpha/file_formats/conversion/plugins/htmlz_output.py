# -*- coding: utf-8 -*-

from __future__ import unicode_literals, division, absolute_import, print_function

import os

from LiuXin.customize.conversion import OutputFormatPlugin, OptionRecommendation

from LiuXin.utils.localization import trans as _
from LiuXin.utils.ptempfiles import TemporaryDirectory

# Py2/Py3
from LiuXin.utils.lx_libraries.liuxin_six import six_cStringIO
from LiuXin.utils.lx_libraries.liuxin_six import six_unicode

__license__ = "GPL 3"
__copyright__ = "2011, John Schember <john@nachtimwald.com>"
__docformat__ = "restructuredtext en"


class HTMLZOutput(OutputFormatPlugin):

    name = "HTMLZ Output"
    author = "John Schember"
    file_type = "htmlz"

    options = {
        OptionRecommendation(
            name="htmlz_css_type",
            recommended_value="class",
            level=OptionRecommendation.LOW,
            choices=["class", "inline", "tag"],
            option_help=_(
                "Specify the handling of CSS. Default is class.\n"
                "class: Use CSS classes and have elements reference them.\n"
                "inline: Write the CSS as an inline style attribute.\n"
                "tag: Turn as many CSS styles as possible into HTML tags."
            ),
        ),
        OptionRecommendation(
            name="htmlz_class_style",
            recommended_value="external",
            level=OptionRecommendation.LOW,
            choices=["external", "inline"],
            option_help=_(
                "How to handle the CSS when using css-type = 'class'.\n"
                "Default is external.\n"
                "external: Use an external CSS file that is linked in the document.\n"
                "inline: Place the CSS in the head section of the document."
            ),
        ),
        OptionRecommendation(
            name="htmlz_title_filename",
            recommended_value=False,
            level=OptionRecommendation.LOW,
            option_help=_(
                "If set this option causes the file name of the html file"
                " inside the htmlz archive to be based on the book title."
            ),
        ),
    }

    def convert(self, oeb_book, output_path, input_plugin, opts, log):

        from lxml import etree

        from LiuXin.file_formats.oeb.base import OEB_IMAGES, SVG_MIME
        from LiuXin.file_formats.opf.opf2 import OPF, metadata_to_opf

        from LiuXin.utils.filenames import ascii_filename
        from LiuXin.utils.calibre_utils.calibre_zipfile import ZipFile

        # HTML
        if opts.htmlz_css_type == "inline":
            from LiuXin.file_formats.htmlz.oeb2html import OEB2HTMLInlineCSSizer

            oeb2htmlizer = OEB2HTMLInlineCSSizer

        elif opts.htmlz_css_type == "tag":
            from LiuXin.file_formats.htmlz.oeb2html import OEB2HTMLNoCSSizer

            oeb2htmlizer = OEB2HTMLNoCSSizer

        else:
            from LiuXin.file_formats.htmlz.oeb2html import OEB2HTMLClassCSSizer

            oeb2htmlizer = OEB2HTMLClassCSSizer

        with TemporaryDirectory("_htmlz_output") as tdir:

            htmlizer = oeb2htmlizer(log)
            html = htmlizer.oeb2html(oeb_book, opts)

            fname = "index"

            if opts.htmlz_title_filename:
                from LiuXin.utils.filenames import shorten_components_to

                fname = shorten_components_to(100, (ascii_filename(six_unicode(oeb_book.metadata.title[0])),))[0]

            with open(os.path.join(tdir, fname + ".html"), "wb") as tf:
                tf.write(html)

            # CSS
            if opts.htmlz_css_type == "class" and opts.htmlz_class_style == "external":
                with open(os.path.join(tdir, "style.css"), "wb") as tf:
                    tf.write(htmlizer.get_css(oeb_book))

            # Images
            images = htmlizer.images
            if images:
                if not os.path.exists(os.path.join(tdir, "images")):
                    os.makedirs(os.path.join(tdir, "images"))
                for item in oeb_book.manifest:
                    if item.media_type in OEB_IMAGES and item.href in images:
                        if item.media_type == SVG_MIME:
                            data = six_unicode(etree.tostring(item.data, encoding=six_unicode))
                        else:
                            data = item.data
                        fname = os.path.join(tdir, "images", images[item.href])
                        with open(fname, "wb") as img:
                            img.write(data)

            # Cover
            cover_path = None
            try:
                cover_data = None
                if oeb_book.metadata.cover:
                    term = oeb_book.metadata.cover[0].term
                    cover_data = oeb_book.guide[term].item.data
                if cover_data:
                    from LiuXin.utils.calibre.utils.magick.draw import (
                        save_cover_data_to,
                    )

                    cover_path = os.path.join(tdir, "cover.jpg")
                    with open(cover_path, "w") as cf:
                        cf.write("")
                    save_cover_data_to(cover_data, cover_path)
            except:
                import traceback

                traceback.print_exc()

            # Metadata
            with open(os.path.join(tdir, "metadata.opf"), "wb") as mdataf:
                opf = OPF(six_cStringIO(etree.tostring(oeb_book.metadata.to_opf1())))
                mi = opf.to_book_metadata()
                if cover_path:
                    mi.cover = "cover.jpg"
                mdataf.write(metadata_to_opf(mi))

            htmlz = ZipFile(output_path, "w")
            htmlz.add_dir(tdir)
