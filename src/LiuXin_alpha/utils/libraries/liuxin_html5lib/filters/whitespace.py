"""
Provide whitespace utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise whitespace through a consuming regression::

        python -m pytest -q tests/file_formats/html/test_html_modernized.py
"""
from __future__ import absolute_import, division, unicode_literals

import re

from LiuXin_alpha.utils.libraries.liuxin_html5lib.filters import _base
from LiuXin_alpha.utils.libraries.liuxin_html5lib.constants import rcdataElements, spaceCharacters

spaceCharacters = "".join(spaceCharacters)

SPACES_REGEX = re.compile("[%s]+" % spaceCharacters)


class Filter(_base.Filter):

    """
    Provide the Filter utility contract with explicit state and cleanup behavior.

    Example:
        Exercise Filter through a consuming regression::

            python -m pytest -q tests/file_formats/html/test_html_modernized.py
    """
    spacePreserveElements = frozenset(["pre", "textarea"] + list(rcdataElements))

    def __iter__(self):
        """
        Expose iter behavior for the compatibility container.

        Example:
            Exercise Filter.  iter   through a consuming regression::

                python -m pytest -q tests/file_formats/html/test_html_modernized.py


        :return: An iterator yielding the normalized values described above.
        """
        preserve = 0
        for token in _base.Filter.__iter__(self):
            type = token["type"]
            if type == "StartTag" and (preserve or token["name"] in self.spacePreserveElements):
                preserve += 1

            elif type == "EndTag" and preserve:
                preserve -= 1

            elif not preserve and type == "SpaceCharacters" and token["data"]:
                # Test on token["data"] above to not introduce spaces where there were not
                token["data"] = " "

            elif not preserve and type == "Characters":
                token["data"] = collapse_spaces(token["data"])

            yield token


def collapse_spaces(text):
    """
    Perform the collapse spaces utility operation under explicit compatibility rules.

    Example:
        Exercise collapse spaces through a consuming regression::

            python -m pytest -q tests/file_formats/html/test_html_modernized.py


    :param text: Text parsed, normalized or rendered.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return SPACES_REGEX.sub(" ", text)
