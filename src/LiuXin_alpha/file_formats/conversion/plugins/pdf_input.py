# -*- coding: utf-8 -*-

"""
Convert PDF content into the normalized ebook conversion pipeline.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise pdf input through a consuming regression::

        python -m pytest -q tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py
"""
from __future__ import annotations

import typing as _typing
import os

from LiuXin_alpha.customize.conversion import InputFormatPlugin, OptionRecommendation
from LiuXin_alpha.file_formats.conversion.plugins._workdir import (
    choose_conversion_workdir,
)

from LiuXin_alpha.utils.calibre import CurrentDir
from LiuXin_alpha.utils.localization import trans as _

__license__ = "GPL 3"
__copyright__ = "2009, John Schember <john@nachtimwald.com>"
__docformat__ = "restructuredtext en"


class PDFInput(InputFormatPlugin):

    """
    Convert pdfinput sources into the normalized OEB pipeline model.

    Example:
        Exercise PDFInput through a consuming regression::

            python -m pytest -q tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py
    """
    name = "PDF Input"
    author = "Kovid Goyal and John Schember"
    description = "Convert PDF files to HTML"
    file_types = {"pdf"}

    options = {
        OptionRecommendation(
            name="no_images",
            recommended_value=False,
            option_help=_("Do not extract images from the document"),
        ),
        OptionRecommendation(
            name="unwrap_factor",
            recommended_value=0.45,
            option_help=_(
                "Scale used to determine the length at which a line should "
                "be unwrapped. Valid values are a decimal between 0 and 1. The "
                "default is 0.45, just below the median line length."
            ),
        ),
        OptionRecommendation(
            name="new_pdf_engine",
            recommended_value=False,
            option_help=_("Use the new PDF conversion engine."),
        ),
    }

    def convert_new(self: _typing.Self, stream: _typing.Any, accelerators: _typing.Any) -> _typing.Any:
        """
        Convert new under the format's safety and compatibility rules.

        Example:
            Exercise PDFInput.convert new through a consuming regression::

                python -m pytest -q tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py


        :param stream: Input or output stream wrapped by the terminal or compatibility
            layer.
        :param accelerators: Value supplied for accelerators under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        from LiuXin_alpha.file_formats.pdf.pdftohtml import pdftohtml
        from LiuXin_alpha.utils.libraries.cleantext import clean_ascii_chars
        from LiuXin_alpha.file_formats.pdf.reflow import PDFDocument

        pdftohtml(os.getcwd(), stream.name, self.opts.no_images, as_xml=True)
        with open("index.xml", "rb") as f:
            xml = clean_ascii_chars(f.read())
        PDFDocument(xml, self.opts, self.log)
        return os.path.join(os.getcwd(), "metadata.opf")

    def convert(self: _typing.Self, stream: _typing.Any, options: _typing.Any, file_ext: _typing.Any, log: _typing.Any, accelerators: _typing.Any) -> _typing.Any:
        """
        Should always be run as part of a fork job - as pdftohtml (which is used here) has to change the cwd.

        Example:
            Exercise PDFInput.convert through a consuming regression::

                python -m pytest -q tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py


        :param stream: Input or output stream wrapped by the terminal or compatibility
            layer.
        :param options: Value supplied for options under the utility contract.
        :param file_ext: Value supplied for file ext under the utility contract.
        :param log: Value supplied for log under the utility contract.
        :param accelerators: Value supplied for accelerators under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        from LiuXin_alpha.file_formats.opf.opf2 import OPFCreator
        from LiuXin_alpha.file_formats.pdf.pdftohtml import pdftohtml

        work_root = choose_conversion_workdir("_pdf_input")
        with CurrentDir(work_root):
            log.debug("Converting file to html...")
            # The main html file will be named index.html
            self.opts, self.log = options, log
            if options.new_pdf_engine:
                return self.convert_new(stream, accelerators)
            pdftohtml(os.getcwd(), stream.name, options.no_images)

            from LiuXin_alpha.customize.ui import get_file_type_metadata

            log.debug("Retrieving document metadata...")
            mi = get_file_type_metadata(stream, "pdf")
            opf = OPFCreator(os.getcwd(), mi)

            manifest = [("index.html", None)]

            images = os.listdir(os.getcwd())
            images.remove("index.html")
            for i in images:
                manifest.append((i, None))
            log.debug("Generating manifest...")
            opf.create_manifest(manifest)

            opf.create_spine(["index.html"])
            log.debug("Rendering manifest...")
            with open("metadata.opf", "wb") as opffile:
                opf.render(opffile)

            return os.path.join(os.getcwd(), "metadata.opf")
