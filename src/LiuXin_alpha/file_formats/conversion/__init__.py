# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai

"""
Expose the supported conversion compatibility surface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise   init   through a consuming regression::

        python -m pytest -q tests/file_formats/conversion/test_conversion_top_level_smoke.py
"""
from __future__ import unicode_literals, division, absolute_import, print_function
from __future__ import annotations

import typing as _typing

__license__ = "GPL v3"
__copyright__ = "2011, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"


class ConversionUserFeedBack(Exception):
    """
    Provide the conversionuserfeedback contract for validated ebook processing.

    Example:
        Exercise ConversionUserFeedBack through a consuming regression::

            python -m pytest -q tests/file_formats/conversion/test_conversion_top_level_smoke.py
    """
    def __init__(self: _typing.Self, title: _typing.Any, msg: _typing.Any, level: str = "info", det_msg: str = "") -> None:
        """
        Show a simple message to the user

        Example:
            Exercise ConversionUserFeedBack.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/conversion/test_conversion_top_level_smoke.py


        :param title: Value supplied for title under the utility contract.
        :param msg: Value supplied for msg under the utility contract.
        :param level: Value supplied for level under the utility contract.
        :param det_msg: Value supplied for det msg under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        import json

        Exception.__init__(
            self,
            json.dumps({"msg": msg, "level": level, "det_msg": det_msg, "title": title}),
        )
        self.title, self.msg, self.det_msg = title, msg, det_msg
        self.level = level
