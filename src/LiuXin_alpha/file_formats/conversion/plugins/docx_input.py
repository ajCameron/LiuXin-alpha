#!/usr/bin/env python
# vim:fileencoding=utf-8

"""
Convert DOCX packages into normalized conversion input.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise docx input through a consuming regression::

        python -m pytest -q tests/file_formats/docx/test_docx_modernized.py
"""
from __future__ import unicode_literals, division, absolute_import, print_function
from __future__ import annotations

import typing as _typing

from LiuXin_alpha.customize.conversion import InputFormatPlugin, OptionRecommendation

from LiuXin_alpha.utils.localization import trans as _

__license__ = "GPL v3"
__copyright__ = "2013, Kovid Goyal <kovid at kovidgoyal.net>"


class DOCXInput(InputFormatPlugin):

    """
    Convert docxinput sources into the normalized OEB pipeline model.

    Example:
        Exercise DOCXInput through a consuming regression::

            python -m pytest -q tests/file_formats/docx/test_docx_modernized.py
    """
    name = "DOCX Input"
    author = "Kovid Goyal"
    description = _("Convert DOCX files (.docx and .docm) to HTML")
    file_types = {"docx", "docm"}

    options = {
        OptionRecommendation(
            name="docx_no_cover",
            recommended_value=False,
            option_help=_(
                "Normally, if a large image is present at the start of the document that "
                "looks like a cover, "
                "it will be removed from the document and used as the cover for created ebook. "
                "This option turns off that behavior."
            ),
        ),
    }

    recommendations = {("page_breaks_before", "/", OptionRecommendation.MED)}

    def convert(self: _typing.Self, stream: _typing.Any, options: _typing.Any, file_ext: _typing.Any, log: _typing.Any, accelerators: _typing.Any) -> _typing.Any:
        """
        Convert the supplied source into the stage's normalized output representation.

        Example:
            Exercise DOCXInput.convert through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :param stream: Input or output stream wrapped by the terminal or compatibility
            layer.
        :param options: Value supplied for options under the utility contract.
        :param file_ext: Value supplied for file ext under the utility contract.
        :param log: Value supplied for log under the utility contract.
        :param accelerators: Value supplied for accelerators under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        from LiuXin_alpha.file_formats.docx.to_html import Convert

        return Convert(stream, detect_cover=not options.docx_no_cover, log=log)()
