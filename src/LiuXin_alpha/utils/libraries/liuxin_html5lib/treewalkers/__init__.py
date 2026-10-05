"""
Expose the supported treewalkers compatibility surface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise   init   through a consuming regression::

        python -m pytest -q tests/file_formats/html/test_html_modernized.py
"""

from __future__ import absolute_import, division, unicode_literals

__all__ = [
    "getTreeWalker",
    "pprint",
    "dom",
    "etree",
    "genshistream",
    "lxmletree",
    "pulldom",
]

import sys

from LiuXin_alpha.utils.libraries.liuxin_html5lib import constants
from LiuXin_alpha.utils.libraries.liuxin_html5lib.utils import default_etree

treeWalkerCache = {}


def getTreeWalker(treeType, implementation=None, **kwargs):
    """
    Get a TreeWalker class for various types of tree with built-in support

    Example:
        Exercise getTreeWalker through a consuming regression::

            python -m pytest -q tests/file_formats/html/test_html_modernized.py


    :param treeType: Value supplied for treeType under the utility contract.
    :param implementation: Value supplied for implementation under the utility contract.
    :param kwargs: Keyword values forwarded to the compatibility implementation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """

    treeType = treeType.lower()
    if treeType not in treeWalkerCache:
        if treeType in ("dom", "pulldom"):
            name = "%s.%s" % (__name__, treeType)
            __import__(name)
            mod = sys.modules[name]
            treeWalkerCache[treeType] = mod.TreeWalker
        elif treeType == "genshi":
            from . import genshistream

            treeWalkerCache[treeType] = genshistream.TreeWalker
        elif treeType == "lxml":
            from . import lxmletree

            treeWalkerCache[treeType] = lxmletree.TreeWalker
        elif treeType == "etree":
            from . import etree

            if implementation is None:
                implementation = default_etree
            # XXX: NEVER cache here, caching is done in the etree submodule
            return etree.getETreeModule(implementation, **kwargs).TreeWalker
    return treeWalkerCache.get(treeType)


def concatenateCharacterTokens(tokens):
    """
    Perform the concatenateCharacterTokens utility operation under explicit compatibility rules.

    Example:
        Exercise concatenateCharacterTokens through a consuming regression::

            python -m pytest -q tests/file_formats/html/test_html_modernized.py


    :param tokens: Value supplied for tokens under the utility contract.
    :return: An iterator yielding the normalized values described above.
    """
    pendingCharacters = []
    for token in tokens:
        type = token["type"]
        if type in ("Characters", "SpaceCharacters"):
            pendingCharacters.append(token["data"])
        else:
            if pendingCharacters:
                yield {"type": "Characters", "data": "".join(pendingCharacters)}
                pendingCharacters = []
            yield token
    if pendingCharacters:
        yield {"type": "Characters", "data": "".join(pendingCharacters)}


def pprint(walker):
    """
    Pretty printer for tree walkers

    Example:
        Exercise pprint through a consuming regression::

            python -m pytest -q tests/file_formats/html/test_html_modernized.py


    :param walker: Value supplied for walker under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    output = []
    indent = 0
    for token in concatenateCharacterTokens(walker):
        type = token["type"]
        if type in ("StartTag", "EmptyTag"):
            # tag name
            if token["namespace"] and token["namespace"] != constants.namespaces["html"]:
                if token["namespace"] in constants.prefixes:
                    ns = constants.prefixes[token["namespace"]]
                else:
                    ns = token["namespace"]
                name = "%s %s" % (ns, token["name"])
            else:
                name = token["name"]
            output.append("%s<%s>" % (" " * indent, name))
            indent += 2
            # attributes (sorted for consistent ordering)
            attrs = token["data"]
            for (namespace, localname), value in sorted(attrs.items()):
                if namespace:
                    if namespace in constants.prefixes:
                        ns = constants.prefixes[namespace]
                    else:
                        ns = namespace
                    name = "%s %s" % (ns, localname)
                else:
                    name = localname
                output.append('%s%s="%s"' % (" " * indent, name, value))
            # self-closing
            if type == "EmptyTag":
                indent -= 2

        elif type == "EndTag":
            indent -= 2

        elif type == "Comment":
            output.append("%s<!-- %s -->" % (" " * indent, token["data"]))

        elif type == "Doctype":
            if token["name"]:
                if token["publicId"]:
                    output.append(
                        """%s<!DOCTYPE %s "%s" "%s">"""
                        % (
                            " " * indent,
                            token["name"],
                            token["publicId"],
                            token["systemId"] if token["systemId"] else "",
                        )
                    )
                elif token["systemId"]:
                    output.append("""%s<!DOCTYPE %s "" "%s">""" % (" " * indent, token["name"], token["systemId"]))
                else:
                    output.append("%s<!DOCTYPE %s>" % (" " * indent, token["name"]))
            else:
                output.append("%s<!DOCTYPE >" % (" " * indent,))

        elif type == "Characters":
            output.append('%s"%s"' % (" " * indent, token["data"]))

        elif type == "SpaceCharacters":
            assert False, "concatenateCharacterTokens should have got rid of all Space tokens"

        else:
            raise ValueError("Unknown token type, %s" % type)

    return "\n".join(output)
