# -*- coding: utf-8 -*-

"""
Read the package format into normalized text, metadata and resources.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise reader through a consuming regression::

        python -m pytest -q tests/file_formats/pdb/test_pdb_modernized.py
"""
from __future__ import annotations

import typing as _typing

from LiuXin_alpha.file_formats.pdb.formatreader import FormatReader

from LiuXin_alpha.utils.libraries.liuxin_six import memory_range
from LiuXin_alpha.utils.ptempfiles import PersistentTemporaryFile

__license__ = "GPL v3"
__copyright__ = "2010, John Schember <john@nachtimwald.com>"
__docformat__ = "restructuredtext en"


class Reader(FormatReader):
    """
    Parse reader data into normalized ebook structures.

    Example:
        Exercise Reader through a consuming regression::

            python -m pytest -q tests/file_formats/pdb/test_pdb_modernized.py
    """
    def __init__(self: _typing.Self, header: _typing.Any, stream: _typing.Any, log: _typing.Any, options: _typing.Any) -> None:
        """
        Initialize and validate the reader state.

        Example:
            Exercise Reader.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/pdb/test_pdb_modernized.py


        :param header: Value supplied for header under the utility contract.
        :param stream: Input or output stream wrapped by the terminal or compatibility
            layer.
        :param log: Value supplied for log under the utility contract.
        :param options: Value supplied for options under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.header = header
        self.stream = stream
        self.log = log
        self.options = options

    def extract_content(self: _typing.Self, output_dir: _typing.Any) -> _typing.Any:
        """
        Extract content under the format's safety and compatibility rules.

        Example:
            Exercise Reader.extract content through a consuming regression::

                python -m pytest -q tests/file_formats/pdb/test_pdb_modernized.py


        :param output_dir: Value supplied for output dir under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        self.log.info("Extracting PDF...")

        pdf = PersistentTemporaryFile(".pdf")
        pdf.close()
        pdf = open(pdf, "wb")
        for x in memory_range(self.header.section_count()):
            pdf.write(self.header.section_data(x))
        pdf.close()

        from LiuXin_alpha.customize.ui import plugin_for_input_format

        pdf_plugin = plugin_for_input_format("pdf")
        for opt in pdf_plugin.options:
            if not hasattr(self.options, opt.option.name):
                setattr(self.options, opt.option.name, opt.recommended_value)

        return pdf_plugin.convert(open(pdf, "rb"), self.options, "pdf", self.log, {})
