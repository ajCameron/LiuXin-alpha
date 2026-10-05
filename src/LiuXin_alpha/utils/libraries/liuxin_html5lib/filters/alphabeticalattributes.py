"""
Provide alphabeticalattributes utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise alphabeticalattributes through a consuming regression::

        python -m pytest -q tests/file_formats/html/test_html_modernized.py
"""
from __future__ import absolute_import, division, unicode_literals

from LiuXin_alpha.utils.libraries.liuxin_html5lib.filters import _base

try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict


class Filter(_base.Filter):
    """
    Provide the Filter utility contract with explicit state and cleanup behavior.

    Example:
        Exercise Filter through a consuming regression::

            python -m pytest -q tests/file_formats/html/test_html_modernized.py
    """
    def __iter__(self):
        """
        Expose iter behavior for the compatibility container.

        Example:
            Exercise Filter.  iter   through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :return: An iterator yielding the normalized values described above.
        """
        for token in _base.Filter.__iter__(self):
            if token["type"] in ("StartTag", "EmptyTag"):
                attrs = OrderedDict()
                for name, value in sorted(token["data"].items(), key=lambda x: x[0]):
                    attrs[name] = value
                token["data"] = attrs
            yield token
