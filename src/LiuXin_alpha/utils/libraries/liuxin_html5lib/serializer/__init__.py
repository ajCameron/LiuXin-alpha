"""
Expose the supported serializer compatibility surface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise   init   through a consuming regression::

        python -m pytest -q tests/file_formats/html/test_html_modernized.py
"""
from __future__ import absolute_import, division, unicode_literals

from LiuXin_alpha.utils.libraries.liuxin_html5lib import treewalkers

from LiuXin_alpha.utils.libraries.liuxin_html5lib.serializer.htmlserializer import HTMLSerializer


def serialize(input, tree="etree", format="html", encoding=None, **serializer_opts):
    # XXX: Should we cache this?
    """
    Perform the serialize utility operation under explicit compatibility rules.

    Example:
        Exercise serialize through a consuming regression::

            python -m pytest -q tests/file_formats/html/test_html_modernized.py


    :param input: Value supplied for input under the utility contract.
    :param tree: Value supplied for tree under the utility contract.
    :param format: Value supplied for format under the utility contract.
    :param encoding: Value supplied for encoding under the utility contract.
    :param serializer_opts: Value supplied for serializer opts under the utility
        contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    walker = treewalkers.getTreeWalker(tree)
    if format == "html":
        s = HTMLSerializer(**serializer_opts)
    else:
        raise ValueError("type must be html")
    return s.render(walker(input), encoding)
