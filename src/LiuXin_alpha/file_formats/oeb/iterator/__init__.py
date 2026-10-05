#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai

"""
Expose the supported iterator compatibility surface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise   init   through a consuming regression::

        python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py
"""
from __future__ import unicode_literals, division, absolute_import, print_function
from __future__ import annotations

import typing as _typing

import os
import re
import sys


from LiuXin_alpha.utils.localization import trans as _

__license__ = "GPL v3"
__copyright__ = "2012, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"


def is_supported(path: _typing.Any) -> bool:
    """
    Is the file in the list of formats which can be converted to html?

    Example:
        Exercise is supported through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


    :param path: Filesystem path read, written, normalized or validated by the
        operation.
    :return: True when the documented condition holds; otherwise False.
    """
    from LiuXin_alpha.customize.ui import available_input_formats

    ext = os.path.splitext(path)[1].replace(".", "").lower()
    ext = re.sub(r"(x{0,1})htm(l{0,1})", "html", ext)
    return ext in available_input_formats() or ext == "kepub"


class UnsupportedFormatError(Exception):
    """
    Report a unsupportedformaterror encountered while processing an ebook format.

    Example:
        Exercise UnsupportedFormatError through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py
    """
    def __init__(self: _typing.Self, fmt: _typing.Any) -> None:
        """
        Initialize and validate the unsupportedformaterror state.

        Example:
            Exercise UnsupportedFormatError.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param fmt: Date, number or template format specification.
        :return: None; validated state is stored on the receiving object.
        """
        Exception.__init__(self, _("%s format books are not supported") % fmt.upper())


def EbookIterator(*args: _typing.Any, **kwargs: _typing.Any) -> _typing.Any:
    """
    For backwards compatibility

    Example:
        Exercise EbookIterator through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


    :param args: Positional values forwarded to the compatibility implementation.
    :param kwargs: Keyword values forwarded to the compatibility implementation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    from LiuXin_alpha.file_formats.oeb.iterator.book import EbookIterator

    return EbookIterator(*args, **kwargs)


def get_preprocess_html(path_to_ebook: _typing.Any, output: _typing.Any = None) -> None:
    """
    Return an html version of the book before pre-process has been run.

    Example:
        Exercise get preprocess html through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


    :param path_to_ebook: Value supplied for path to ebook under the utility contract.
    :param output: Value supplied for output under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats.conversion.plumber import (
        set_regex_wizard_callback,
        Plumber,
    )
    from LiuXin_alpha.utils.logger import DevNull
    from LiuXin_alpha.utils.ptempfiles import TemporaryDirectory

    raw = {}
    set_regex_wizard_callback(raw.__setitem__)
    with TemporaryDirectory("_regex_wiz") as tdir:
        pl = Plumber(
            path_to_ebook,
            os.path.join(tdir, "a.epub"),
            DevNull(),
            for_regex_wizard=True,
        )
        pl.run()
        items = [raw[item.href] for item in pl.oeb.spine if item.href in raw]

    with (sys.stdout if output is None else open(output, "wb")) as out:
        for html in items:
            out.write(html.encode("utf-8"))
            out.write(b"\n\n" + b"-" * 80 + b"\n\n")
