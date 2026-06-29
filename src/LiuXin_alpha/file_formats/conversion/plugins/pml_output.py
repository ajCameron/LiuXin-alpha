# -*- coding: utf-8 -*-

import os
import io

from LiuXin_alpha.customize.conversion import OutputFormatPlugin, OptionRecommendation

from LiuXin_alpha.file_formats.conversion.report import ensure_conversion_report
from LiuXin_alpha.utils.localization import trans as _
from LiuXin_alpha.utils.ptempfiles import TemporaryDirectory

# Py2/Py3
from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode

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

        self.log = log
        edge = getattr(opts, "conversion_edge", None)
        input_format = getattr(input_plugin, "file_type", None) or "oeb"
        source_format = getattr(edge, "source_format", None) or input_format
        target_format = getattr(edge, "target_format", None) or self.file_type
        edge_name = getattr(edge, "name", None) or "%s-to-%s" % (source_format, target_format)
        self.conversion_report = ensure_conversion_report(
            opts,
            source_format=source_format,
            target_format=target_format,
            edge_name=edge_name,
        )
        with TemporaryDirectory("_pmlz_output") as tdir:
            pmlmlizer = PMLMLizer(log, conversion_report=self.conversion_report)
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
            from PIL import Image as PILImage
        except Exception:
            PILImage = None

        from LiuXin_alpha.file_formats.oeb.base import OEB_RASTER_IMAGES
        if PILImage is None:
            warn = getattr(self.log, "warning", None) or getattr(self.log, "warn", None) or getattr(self.log, "info", None)
            if warn is not None:
                warn("Pillow not available; skipping image export in PML output.")
            return

        resampling = getattr(PILImage, "Resampling", None)
        lanczos = resampling.LANCZOS if resampling is not None and hasattr(resampling, "LANCZOS") else getattr(PILImage, "ANTIALIAS", None)

        for item in manifest:
            if item.media_type in OEB_RASTER_IMAGES and item.href in image_hrefs.keys():
                # Todo: This is a hack. lit_1 seems to not be being rendered properly - and I can't trace why right now
                # The problem seems to be in how it was created - it was built with calibre -  the data for the images
                # recorded in the file is not being properl6y processes - as a result the image is coming back as an
                # empty string, which causes problems when trying to read in here
                try:
                    if opts.full_image_depth:
                        im = PILImage.open(io.BytesIO(item.data))
                    else:
                        im = PILImage.open(io.BytesIO(item.data)).convert("P")
                        if lanczos is None:
                            im.thumbnail((300, 300))
                        else:
                            im.thumbnail((300, 300), lanczos)
                except IOError:
                    continue

                data = io.BytesIO()
                im.save(data, "PNG")
                data = data.getvalue()

                path = os.path.join(out_dir, image_hrefs[item.href])

                with open(path, "wb") as out:
                    out.write(data)
