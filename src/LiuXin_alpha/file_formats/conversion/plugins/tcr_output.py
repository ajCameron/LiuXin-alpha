# -*- coding: utf-8 -*-

"""
Convert TCR content from the normalized ebook conversion pipeline.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise tcr output through a consuming regression::

        python -m pytest -q tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py
"""
from __future__ import annotations

import typing as _typing
import os

from LiuXin_alpha.customize.conversion import OutputFormatPlugin, OptionRecommendation

from LiuXin_alpha.utils.localization import trans as _

__license__ = "GPL 3"
__copyright__ = "2009, John Schember <john@nachtimwald.com>"
__docformat__ = "restructuredtext en"


class TCROutput(OutputFormatPlugin):

    """
    Provide the tcroutput contract for validated ebook processing.

    Example:
        Exercise TCROutput through a consuming regression::

            python -m pytest -q tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py
    """
    name = "TCR Output"
    author = "John Schember"
    file_type = "tcr"

    options = {
        OptionRecommendation(
            name="tcr_output_encoding",
            recommended_value="utf-8",
            level=OptionRecommendation.LOW,
            option_help=_("Specify the character encoding of the output document. The default is utf-8."),
        ),
    }

    def convert(self: _typing.Self, oeb_book: _typing.Any, output_path: _typing.Any, input_plugin: _typing.Any, opts: _typing.Any, log: _typing.Any) -> None:
        """
        Convert an oeb book to a tcr book.

        Example:
            Exercise TCROutput.convert through a consuming regression::

                python -m pytest -q tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py


        :param oeb_book: Value supplied for oeb book under the utility contract.
        :param output_path: Value supplied for output path under the utility contract.
        :param input_plugin: Value supplied for input plugin under the utility contract.
        :param opts: Value supplied for opts under the utility contract.
        :param log: Value supplied for log under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        log.info("Writing TCR file...")
        from LiuXin_alpha.file_formats.txt.txtml import TXTMLizer
        from LiuXin_alpha.file_formats.compression.tcr import compress

        close = False
        if not hasattr(output_path, "write"):
            close = True
            output_dir = os.path.dirname(output_path)
            if output_dir:
                os.makedirs(output_dir, exist_ok=True)
            out_stream = open(output_path, "wb")
        else:
            out_stream = output_path

        try:
            setattr(opts, "flush_paras", False)
            setattr(opts, "max_line_length", 0)
            setattr(opts, "force_max_line_length", False)
            setattr(opts, "indent_paras", False)

            writer = TXTMLizer(log)
            raw_txt = writer.extract_content(oeb_book, opts)
            output_encoding = getattr(opts, "tcr_output_encoding", "utf-8") or "utf-8"
            if isinstance(raw_txt, bytes):
                txt = raw_txt
            else:
                txt = str(raw_txt).encode(output_encoding, "replace")

            log.info("Compressing text...")
            txt = compress(txt)

            if hasattr(out_stream, "seek"):
                out_stream.seek(0)
            if hasattr(out_stream, "truncate"):
                out_stream.truncate()
            out_stream.write(txt)
        finally:
            if close:
                out_stream.close()
