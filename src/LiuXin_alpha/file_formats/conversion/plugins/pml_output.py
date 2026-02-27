# -*- coding: utf-8 -*-

import os

from LiuXin_alpha.customize.conversion import OutputFormatPlugin, OptionRecommendation

from LiuXin_alpha.utils.localization import trans as _
from LiuXin_alpha.utils.ptempfiles import TemporaryDirectory

# Py2/Py3
from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode
from LiuXin_alpha.utils.libraries.liuxin_six import six_cStringIO

__license__ = "GPL 3"
__copyright__ = "2009, John Schember <john@nachtimwald.com>"
__docformat__ = "restructuredtext en"


class PMLOutput(OutputFormatPlugin):

    name = "PML Output"
    author = "John Schember"
    file_type = "pmlz"

    options = {
        OptionRecommendation(
            name="pml_output_encoding",
            recommended_value="cp1252",
            level=OptionRecommendation.LOW,
            option_help=_("Specify the character encoding of the output document. " "The default is cp1252."),
        ),
        OptionRecommendation(
            name="inline_toc",
            recommended_value=False,
            level=OptionRecommendation.LOW,
            option_help=_("Add Table of Contents to beginning of the book."),
        ),
        OptionRecommendation(
            name="full_image_depth",
            recommended_value=False,
            level=OptionRecommendation.LOW,
            option_help=_(
                "Do not reduce the size or bit depth of images. Images "
                "have their size and depth reduced by default to accommodate "
                "applications that can not convert images on their "
                "own such as Dropbook."
            ),
        ),
    }

    def convert(self, oeb_book, output_path, input_plugin, opts, log):
        from LiuXin_alpha.file_formats.pml.pmlml import PMLMLizer
        from LiuXin_alpha.utils.libraries.calibre_zipfile import ZipFile

        with TemporaryDirectory("_pmlz_output") as tdir:
            pmlmlizer = PMLMLizer(log)
            pml = six_unicode(pmlmlizer.extract_content(oeb_book, opts))
            with open(os.path.join(tdir, "index.pml"), "wb") as out:
                out.write(pml.encode(opts.pml_output_encoding, "replace"))

            img_path = os.path.join(tdir, "index_img")
            if not os.path.exists(img_path):
                os.makedirs(img_path)
            self.write_images(oeb_book.manifest, pmlmlizer.image_hrefs, img_path, opts)

            log.debug("Compressing output...")
            pmlz = ZipFile(output_path, "w")
            pmlz.add_dir(tdir)

    def write_images(self, manifest, image_hrefs, out_dir, opts):
        """
        Write images out to the file.
        :param manifest:
        :param image_hrefs:
        :param out_dir:
        :param opts:
        :return:
        """
        try:
            from PIL import Image

            # Image  # To make pyflakes shut up
        except ImportError:
            import Image

        from LiuXin_alpha.file_formats.oeb.base import OEB_RASTER_IMAGES

        for item in manifest:
            if item.media_type in OEB_RASTER_IMAGES and item.href in image_hrefs.keys():
                # Todo: This is a hack. lit_1 seems to not be being rendered properly - and I can't trace why right now
                # The problem seems to be in how it was created - it was built with calibre -  the data for the images
                # recorded in the file is not being properl6y processes - as a result the image is coming back as an
                # empty string, which causes problems when trying to read in here
                try:
                    if opts.full_image_depth:
                        im = Image.open(six_cStringIO(item.data))
                    else:
                        im = Image.open(six_cStringIO(item.data)).convert("P")
                        im.thumbnail((300, 300), Image.ANTIALIAS)
                except IOError:
                    continue

                data = six_cStringIO()
                im.save(data, "PNG")
                data = data.getvalue()

                path = os.path.join(out_dir, image_hrefs[item.href])

                with open(path, "wb") as out:
                    out.write(data)
