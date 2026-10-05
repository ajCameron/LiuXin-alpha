"""
Filter unsafe HTML tokens, attributes, URI schemes and CSS constructs.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise sanitizer through a consuming regression::

        python -m pytest -q tests/file_formats/html/test_html_modernized.py
"""
from __future__ import absolute_import, division, unicode_literals

from LiuXin_alpha.utils.libraries.liuxin_html5lib.filters import _base
from LiuXin_alpha.utils.libraries.liuxin_html5lib.sanitizer import HTMLSanitizerMixin


class Filter(_base.Filter, HTMLSanitizerMixin):
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
            token = self.sanitize_token(token)
            if token:
                yield token
