# -*- coding: utf-8 -*-

"""
Convert PDF_OUTPUT_QT content from the normalized ebook conversion pipeline.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise pdf output qt through a consuming regression::

        python -m pytest -q tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py
"""
from __future__ import annotations

import typing as _typing

from LiuXin_alpha.file_formats.conversion.plugins.pdf_output import PDFOutput

__license__ = "GPL 3"
__copyright__ = "2026, LiuXin contributors"
__docformat__ = "restructuredtext en"


class PDFQtOutput(PDFOutput):
    """
    Provide the pdfqtoutput contract for validated ebook processing.

    Example:
        Exercise PDFQtOutput through a consuming regression::

            python -m pytest -q tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py
    """
    name = "PDF Output (Qt)"
    file_type = "pdfqt"

    def convert(self: _typing.Self, oeb_book: _typing.Any, output_path: _typing.Any, input_plugin: _typing.Any, opts: _typing.Any, log: _typing.Any) -> _typing.Any:
        """
        Convert the supplied source into the stage's normalized output representation.

        Example:
            Exercise PDFQtOutput.convert through a consuming regression::

                python -m pytest -q tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py


        :param oeb_book: Value supplied for oeb book under the utility contract.
        :param output_path: Value supplied for output path under the utility contract.
        :param input_plugin: Value supplied for input plugin under the utility contract.
        :param opts: Value supplied for opts under the utility contract.
        :param log: Value supplied for log under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        setattr(opts, "pdf_engine_mode", "qt")
        return super().convert(oeb_book, output_path, input_plugin, opts, log)
