# -*- coding: utf-8 -*-

import os
import string

from LiuXin.customize.conversion import OutputFormatPlugin, OptionRecommendation

from LiuXin.utils.calibre.constants import __appname__, __version__
from LiuXin.utils.localization import trans as _
from LiuXin.utils.ptempfiles import TemporaryDirectory

# Py2/Py3 compatibility layer
from LiuXin.utils.lx_libraries.liuxin_six import six_unicode

__license__ = "GPL 3"
__copyright__ = "2010, Li Fanxi <lifanxi@freemindworld.com>"
__docformat__ = "restructuredtext en"


class SNBOutput(OutputFormatPlugin):

    name = "SNB Output"
    author = "Li Fanxi"
    file_type = "snb"

    options = {
        OptionRecommendation(
            name="snb_output_encoding",
            recommended_value="utf-8",
            level=OptionRecommendation.LOW,
            option_help=_("Specify the character encoding of the output document. The default is utf-8."),
        ),
        OptionRecommendation(
            name="snb_max_line_length",
            recommended_value=0,
            level=OptionRecommendation.LOW,
            option_help=_(
                "The maximum number of characters per line. This splits on "
                "the first space before the specified value. If no space is found "
                "the line will be broken at the space after and will exceed the "
                "specified value. Also, there is a minimum of 25 characters. "
                "Use 0 to disable line splitting."
            ),
        ),
        OptionRecommendation(
            name="snb_insert_empty_line",
            recommended_value=False,
            level=OptionRecommendation.LOW,
            option_help=_("Specify whether or not to insert an empty line between two paragraphs."),
        ),
        OptionRecommendation(
            name="snb_dont_indent_first_line",
            recommended_value=False,
            level=OptionRecommendation.LOW,
            option_help=_(
                "Specify whether or not to insert two space characters " "to indent the first line of each paragraph."
            ),
        ),
        OptionRecommendation(
            name="snb_hide_chapter_name",
            recommended_value=False,
            level=OptionRecommendation.LOW,
            option_help=_(
                "Specify whether or not to hide the chapter title for each "
                "chapter. Useful for image-only output (eg. comics)."
            ),
        ),
        OptionRecommendation(
            name="snb_full_screen",
            recommended_value=False,
            level=OptionRecommendation.LOW,
            option_help=_("Resize all the images for full screen view. "),
        ),
    }

    def convert(self, oeb_book, output_path, input_plugin, opts, log):

        from lxml import etree
        from LiuXin.file_formats.snb.snbfile import SNBFile
        from LiuXin.file_formats.snb.snbml import SNBMLizer, ProcessFileName

        self.opts = opts
        # from LiuXin.file_formats.oeb.transforms.rasterize import SVGRasterizer, Unavailable
        from LiuXin.file_formats.oeb.transforms.rasterize import Unavailable
        from LiuXin.file_formats.oeb.transforms.rasterize_safe import (
            SVGRasterizerSafe as SVGRasterizer,
        )

        try:
            rasterizer = SVGRasterizer()
            rasterizer(oeb_book, opts)
        except Unavailable:
            log.warn("SVG rasterizer unavailable, SVG will not be converted")

        # Create temp dir
        with TemporaryDirectory("_snb_output") as tdir:
            # Create stub directories
            snbf_dir = os.path.join(tdir, "snbf")
            snbc_dir = os.path.join(tdir, "snbc")
            snbi_dir = os.path.join(tdir, "snbc/images")
            os.mkdir(snbf_dir)
            os.mkdir(snbc_dir)
            os.mkdir(snbi_dir)

            # Process Meta data
            meta = oeb_book.metadata
            if meta.title:
                title = six_unicode(meta.title[0])
            else:
                title = ""
            authors = [six_unicode(x) for x in meta.creator if x.role == "aut"]
            if meta.publisher:
                publishers = six_unicode(meta.publisher[0])
            else:
                publishers = ""
            if meta.language:
                lang = six_unicode(meta.language[0]).upper()
            else:
                lang = ""
            if meta.description:
                abstract = six_unicode(meta.description[0])
            else:
                abstract = ""

            # Process Cover
            g, m, s = oeb_book.guide, oeb_book.manifest, oeb_book.spine
            href = None
            if "titlepage" not in g:
                if "cover" in g:
                    href = g["cover"].href

            # Output book info file
            book_info_tree = etree.Element("book-snbf", version="1.0")
            head_tree = etree.SubElement(book_info_tree, "head")
            etree.SubElement(head_tree, "name").text = title
            etree.SubElement(head_tree, "author").text = " ".join(authors)
            etree.SubElement(head_tree, "language").text = lang
            etree.SubElement(head_tree, "rights")
            etree.SubElement(head_tree, "publisher").text = publishers
            etree.SubElement(head_tree, "generator").text = __appname__ + " " + __version__
            etree.SubElement(head_tree, "created")
            etree.SubElement(head_tree, "abstract").text = abstract
            if href is not None:
                etree.SubElement(head_tree, "cover").text = ProcessFileName(href)
            else:
                etree.SubElement(head_tree, "cover")
            with open(os.path.join(snbf_dir, "book.snbf"), "wb") as book_info_file:
                book_info_file.write(etree.tostring(book_info_tree, pretty_print=True, encoding="utf-8"))

            # Output TOC
            toc_info_tree = etree.Element("toc-snbf")
            toc_head = etree.SubElement(toc_info_tree, "head")
            toc_body = etree.SubElement(toc_info_tree, "body")
            output_files = {}
            if oeb_book.toc.count() == 0:
                log.warn("This SNB file has no Table of Contents. Creating a default TOC")
                first = iter(oeb_book.spine).next()
                oeb_book.toc.add(_("Start Page"), first.href)
            else:
                first = iter(oeb_book.spine).next()
                if oeb_book.toc[0].href != first.href:
                    # The pages before the fist item in toc will be stored as
                    # "Cover Pages".
                    # oeb_book.toc does not support "insert", so we generate
                    # the tocInfoTree directly instead of modifying the toc
                    ch = etree.SubElement(toc_body, "chapter")
                    ch.set("src", ProcessFileName(first.href) + ".snbc")
                    ch.text = _("Cover Pages")
                    output_files[first.href] = []
                    output_files[first.href].append(("", _("Cover Pages")))

            for tocitem in oeb_book.toc:
                if tocitem.href.find("#") != -1:
                    item = string.split(tocitem.href, "#")
                    if len(item) != 2:
                        log.error("Error in TOC item: %s" % tocitem)
                    else:
                        if item[0] in output_files:
                            output_files[item[0]].append((item[1], tocitem.title))
                        else:
                            output_files[item[0]] = []
                            if "" not in output_files[item[0]]:
                                output_files[item[0]].append(("", tocitem.title + _(" (Preface)")))
                                ch = etree.SubElement(toc_body, "chapter")
                                ch.set("src", ProcessFileName(item[0]) + ".snbc")
                                ch.text = tocitem.title + _(" (Preface)")
                            output_files[item[0]].append((item[1], tocitem.title))
                else:
                    if tocitem.href in output_files:
                        output_files[tocitem.href].append(("", tocitem.title))
                    else:
                        output_files[tocitem.href] = []
                        output_files[tocitem.href].append(("", tocitem.title))
                ch = etree.SubElement(toc_body, "chapter")
                ch.set("src", ProcessFileName(tocitem.href) + ".snbc")
                ch.text = tocitem.title

            etree.SubElement(toc_head, "chapters").text = "%d" % len(toc_body)

            toc_info_file = open(os.path.join(snbf_dir, "toc.snbf"), "wb")
            toc_info_file.write(etree.tostring(toc_info_tree, pretty_print=True, encoding="utf-8"))
            toc_info_file.close()

            # Output Files
            OEB_IMAGES = ()
            old_tree = None
            merge_last = False
            last_name = None
            for item in s:
                from LiuXin.file_formats.oeb.base import OEB_DOCS, OEB_IMAGES

                if m.hrefs[item.href].media_type in OEB_DOCS:
                    if item.href not in output_files:
                        log.debug("File %s is unused in TOC. Continue in last chapter" % item.href)
                        merge_last = True
                    else:
                        if old_tree is not None and merge_last:
                            log.debug("Output the modified chapter again: %s" % last_name)
                            output_file = open(os.path.join(snbc_dir, last_name), "wb")
                            output_file.write(etree.tostring(old_tree, pretty_print=True, encoding="utf-8"))
                            output_file.close()
                            merge_last = False

                    log.debug("Converting %s to snbc..." % item.href)
                    snbwriter = SNBMLizer(log)
                    if not merge_last:
                        snbc_trees = snbwriter.extract_content(oeb_book, item, output_files[item.href], opts)
                        for subName in snbc_trees:
                            postfix = ""
                            if subName != "":
                                postfix = "_" + subName
                            last_name = ProcessFileName(item.href + postfix + ".snbc")
                            old_tree = snbc_trees[subName]
                            output_file = open(os.path.join(snbc_dir, last_name), "wb")
                            output_file.write(etree.tostring(old_tree, pretty_print=True, encoding="utf-8"))
                            output_file.close()
                    else:
                        log.debug("Merge %s with last TOC item..." % item.href)
                        snbwriter.merge_content(old_tree, oeb_book, item, [("", _("Start"))], opts)

            # Output the last one if needed
            log.debug("Output the last modified chapter again: %s" % last_name)
            if old_tree is not None and merge_last:
                output_file = open(os.path.join(snbc_dir, last_name), "wb")
                output_file.write(etree.tostring(old_tree, pretty_print=True, encoding="utf-8"))
                output_file.close()

            for item in m:
                if m.hrefs[item.href].media_type in OEB_IMAGES:
                    log.debug("Converting image: %s ..." % item.href)
                    content = m.hrefs[item.href].data
                    # Convert & Resize image
                    self.HandleImage(content, os.path.join(snbi_dir, ProcessFileName(item.href)))

            # Package as SNB File
            snb_file = SNBFile()
            snb_file.FromDir(tdir)
            snb_file.Output(output_path)

    def HandleImage(self, imageData, imagePath):
        from LiuXin.utils.magick import Image

        img = Image()
        # Todo: Instead of just failing, write an image indicating failure
        try:
            img.load(imageData)
        except ValueError:
            return
        (x, y) = img.size
        if self.opts:
            if self.opts.snb_full_screen:
                screen_x, screen_y = self.opts.output_profile.screen_size
            else:
                screen_x, screen_y = self.opts.output_profile.comic_screen_size
        else:
            screen_x = 540
            screen_y = 700
        # Handle big image only
        if x > screen_x or y > screen_y:
            xScale = float(x) / screen_x
            yScale = float(y) / screen_y
            scale = max(xScale, yScale)
            # TODO : intelligent image rotation
            #     img = img.rotate(90)
            #     x,y = y,x
            img.size = (x / scale, y / scale)
        img.save(imagePath)


if __name__ == "__main__":

    from LiuXin.customize.profiles import HanlinV3Output

    from LiuXin.file_formats.oeb.reader import OEBReader
    from LiuXin.file_formats.oeb.base import OEBBook
    from LiuXin.file_formats.conversion.preprocess import HTMLPreProcessor

    class OptionValues(object):
        pass

    opts = OptionValues()
    opts.output_profile = HanlinV3Output(None)

    html_preprocessor = HTMLPreProcessor(None, None, opts)
    from LiuXin.utils.logger import default_log

    oeb = OEBBook(default_log, html_preprocessor)
    reader = OEBReader
    reader()(oeb, "/tmp/bbb/processed/")
    SNBOutput(None).convert(oeb, "/tmp/test.snb", None, None, default_log)
