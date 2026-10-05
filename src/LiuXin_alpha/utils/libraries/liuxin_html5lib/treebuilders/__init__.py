"""
Expose the supported treebuilders compatibility surface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise   init   through a consuming regression::

        python -m pytest -q tests/file_formats/html/test_html_modernized.py
"""

from __future__ import absolute_import, division, unicode_literals

from LiuXin_alpha.utils.libraries.liuxin_html5lib.utils import default_etree

treeBuilderCache = {}


def getTreeBuilder(treeType, implementation=None, **kwargs):
    """
    Get a TreeBuilder class for various types of tree with built-in support

    Example:
        Exercise getTreeBuilder through a consuming regression::

            python -m pytest -q tests/file_formats/html/test_html_modernized.py


    :param treeType: Value supplied for treeType under the utility contract.
    :param implementation: Value supplied for implementation under the utility contract.
    :param kwargs: Keyword values forwarded to the compatibility implementation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """

    treeType = treeType.lower()
    if treeType not in treeBuilderCache:
        if treeType == "dom":
            from . import dom

            # Come up with a sane default (pref. from the stdlib)
            if implementation is None:
                from xml.dom import minidom

                implementation = minidom
            # NEVER cache here, caching is done in the dom submodule
            return dom.getDomModule(implementation, **kwargs).TreeBuilder
        elif treeType == "lxml":
            from . import etree_lxml

            treeBuilderCache[treeType] = etree_lxml.TreeBuilder
        elif treeType == "etree":
            from . import etree

            if implementation is None:
                implementation = default_etree
            # NEVER cache here, caching is done in the etree submodule
            return etree.getETreeModule(implementation, **kwargs).TreeBuilder
        else:
            raise ValueError("""Unrecognised treebuilder "%s" """ % treeType)
    return treeBuilderCache.get(treeType)
