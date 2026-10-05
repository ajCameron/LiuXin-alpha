# -*- coding: utf-8 -*-

"""
Convert RTF content from the normalized ebook conversion pipeline.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise rtf output through a consuming regression::

        python -m pytest -q tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py
"""
from __future__ import annotations

import typing as _typing
import os

from LiuXin_alpha.customize.conversion import OutputFormatPlugin

__license__ = "GPL 3"
__copyright__ = "2009, John Schember <john@nachtimwald.com>"
__docformat__ = "restructuredtext en"


class RTFOutput(OutputFormatPlugin):

    """
    Provide the rtfoutput contract for validated ebook processing.

    Example:
        Exercise RTFOutput through a consuming regression::

            python -m pytest -q tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py
    """
    name = "RTF Output"
    author = "John Schember"
    file_type = "rtf"

    def convert(self: _typing.Self, oeb_book: _typing.Any, output_path: _typing.Any, input_plugin: _typing.Any, opts: _typing.Any, log: _typing.Any) -> None:
        """
        Make a rtf file out of an OEB one.

        Example:
            Exercise RTFOutput.convert through a consuming regression::

                python -m pytest -q tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py


        :param oeb_book: Value supplied for oeb book under the utility contract.
        :param output_path: Value supplied for output path under the utility contract.
        :param input_plugin: Value supplied for input plugin under the utility contract.
        :param opts: Value supplied for opts under the utility contract.
        :param log: Value supplied for log under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        from LiuXin_alpha.file_formats.rtf.rtfml import RTFMLizer

        log.info("Converting to rtf...")

        rtfmlitzer = RTFMLizer(log)
        content = rtfmlitzer.extract_content(oeb_book, opts)

        close = False
        if not hasattr(output_path, "write"):
            close = True
            if not os.path.exists(os.path.dirname(output_path)) and os.path.dirname(output_path) != "":
                os.makedirs(os.path.dirname(output_path))
            out_stream = open(output_path, "wb")
        else:
            out_stream = output_path

        out_stream.seek(0)
        out_stream.truncate()
        out_stream.write(content.encode("ascii", "replace"))

        if close:
            out_stream.close()
