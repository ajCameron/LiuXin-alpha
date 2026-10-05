"""
Adapt HTML5 tree-walker events into SAX content-handler calls.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise sax through a consuming regression::

        python -m pytest -q tests/file_formats/html/test_html_modernized.py
"""
from __future__ import absolute_import, division, unicode_literals

from xml.sax.xmlreader import AttributesNSImpl

from LiuXin_alpha.utils.libraries.liuxin_html5lib.constants import (
    adjustForeignAttributes,
    unadjustForeignAttributes,
)

prefix_mapping = {}
for prefix, localName, namespace in adjustForeignAttributes.values():
    if prefix is not None:
        prefix_mapping[prefix] = namespace


def to_sax(walker, handler):
    """
    Call SAX-like content handler based on treewalker walker

    Example:
        Exercise to sax through a consuming regression::

            python -m pytest -q tests/file_formats/html/test_html_modernized.py


    :param walker: Value supplied for walker under the utility contract.
    :param handler: Value supplied for handler under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    handler.startDocument()
    for prefix, namespace in prefix_mapping.items():
        handler.startPrefixMapping(prefix, namespace)

    for token in walker:
        type = token["type"]
        if type == "Doctype":
            continue
        elif type in ("StartTag", "EmptyTag"):
            attrs = AttributesNSImpl(token["data"], unadjustForeignAttributes)
            handler.startElementNS((token["namespace"], token["name"]), token["name"], attrs)
            if type == "EmptyTag":
                handler.endElementNS((token["namespace"], token["name"]), token["name"])
        elif type == "EndTag":
            handler.endElementNS((token["namespace"], token["name"]), token["name"])
        elif type in ("Characters", "SpaceCharacters"):
            handler.characters(token["data"])
        elif type == "Comment":
            pass
        else:
            assert False, "Unknown token type"

    for prefix, namespace in prefix_mapping.items():
        handler.endPrefixMapping(prefix)
    handler.endDocument()
