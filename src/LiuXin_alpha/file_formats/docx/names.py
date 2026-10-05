#!/usr/bin/env python2
# vim:fileencoding=utf-8

"""
Define namespace-qualified DOCX element and attribute names.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise names through a consuming regression::

        python -m pytest -q tests/file_formats/docx/test_docx_modernized.py
"""
from __future__ import unicode_literals, division, absolute_import, print_function
from __future__ import annotations

import typing as _typing

import re

from lxml.etree import XPath as Xp

from LiuXin_alpha.utils.storage.local.filenames import ascii_text

# Py2/Py3 compatibility layer
from LiuXin_alpha.utils.libraries.liuxin_six import dict_iteritems as iteritems

__license__ = "GPL v3"
__copyright__ = "2013, Kovid Goyal <kovid at kovidgoyal.net>"

# Names {{{
TRANSITIONAL_NAMES = {
    "DOCUMENT": "http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument",
    "DOCPROPS": "http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties",
    "APPPROPS": "http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties",
    "STYLES": "http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles",
    "NUMBERING": "http://schemas.openxmlformats.org/officeDocument/2006/relationships/numbering",
    "FONTS": "http://schemas.openxmlformats.org/officeDocument/2006/relationships/fontTable",
    "EMBEDDED_FONT": "http://schemas.openxmlformats.org/officeDocument/2006/relationships/font",
    "IMAGES": "http://schemas.openxmlformats.org/officeDocument/2006/relationships/image",
    "LINKS": "http://schemas.openxmlformats.org/officeDocument/2006/relationships/hyperlink",
    "FOOTNOTES": "http://schemas.openxmlformats.org/officeDocument/2006/relationships/footnotes",
    "ENDNOTES": "http://schemas.openxmlformats.org/officeDocument/2006/relationships/endnotes",
    "THEMES": "http://schemas.openxmlformats.org/officeDocument/2006/relationships/theme",
    "SETTINGS": "http://schemas.openxmlformats.org/officeDocument/2006/relationships/settings",
    "WEB_SETTINGS": "http://schemas.openxmlformats.org/officeDocument/2006/relationships/webSettings",
}

STRICT_NAMES = {
    k: v.replace(
        "http://schemas.openxmlformats.org/officeDocument/2006",
        "http://purl.oclc.org/ooxml/officeDocument",
    )
    for k, v in iteritems(TRANSITIONAL_NAMES)
}

TRANSITIONAL_NAMESPACES = {
    "mo": "http://schemas.microsoft.com/office/mac/office/2008/main",
    "o": "urn:schemas-microsoft-com:office:office",
    "ve": "http://schemas.openxmlformats.org/markup-compatibility/2006",
    # Text Content
    "w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main",
    "w10": "urn:schemas-microsoft-com:office:word",
    "wne": "http://schemas.microsoft.com/office/word/2006/wordml",
    "xml": "http://www.w3.org/XML/1998/namespace",
    # Drawing
    "a": "http://schemas.openxmlformats.org/drawingml/2006/main",
    "m": "http://schemas.openxmlformats.org/officeDocument/2006/math",
    "mv": "urn:schemas-microsoft-com:mac:vml",
    "pic": "http://schemas.openxmlformats.org/drawingml/2006/picture",
    "v": "urn:schemas-microsoft-com:vml",
    "wp": "http://schemas.openxmlformats.org/drawingml/2006/wordprocessingDrawing",
    # Properties (core and extended)
    "cp": "http://schemas.openxmlformats.org/package/2006/metadata/core-properties",
    "dc": "http://purl.org/dc/elements/1.1/",
    "ep": "http://schemas.openxmlformats.org/officeDocument/2006/extended-properties",
    "xsi": "http://www.w3.org/2001/XMLSchema-instance",
    # Content Types
    "ct": "http://schemas.openxmlformats.org/package/2006/content-types",
    # Package Relationships
    "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
    "pr": "http://schemas.openxmlformats.org/package/2006/relationships",
    # Dublin Core document properties
    "dcmitype": "http://purl.org/dc/dcmitype/",
    "dcterms": "http://purl.org/dc/terms/",
}

STRICT_NAMESPACES = {
    k: v.replace(
        "http://schemas.openxmlformats.org/officeDocument/2006",
        "http://purl.oclc.org/ooxml/officeDocument",
    )
    .replace(
        "http://schemas.openxmlformats.org/wordprocessingml/2006",
        "http://purl.oclc.org/ooxml/wordprocessingml",
    )
    .replace(
        "http://schemas.openxmlformats.org/drawingml/2006",
        "http://purl.oclc.org/ooxml/drawingml",
    )
    for k, v in iteritems(TRANSITIONAL_NAMESPACES)
}
# }}}


def barename(x: _typing.Any) -> _typing.Any:
    """
    Perform the barename operation under explicit file-format and conversion rules.

    Example:
        Exercise barename through a consuming regression::

            python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


    :param x: Value supplied for x under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return x.rpartition("}")[-1]


def XML(x: _typing.Any) -> _typing.Any:
    """
    Perform the XML operation under explicit file-format and conversion rules.

    Example:
        Exercise XML through a consuming regression::

            python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


    :param x: Value supplied for x under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return "{%s}%s" % (TRANSITIONAL_NAMESPACES["xml"], x)


def generate_anchor(name: _typing.Any, existing: _typing.Any) -> _typing.Any:
    """
    Perform the generate anchor operation under explicit file-format and conversion rules.

    Example:
        Exercise generate anchor through a consuming regression::

            python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


    :param name: Field, file, function or resource name addressed by the operation.
    :param existing: Value supplied for existing under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    x = y = "id_" + re.sub(r"[^0-9a-zA-Z_]", "", ascii_text(name)).lstrip("_")
    c = 1
    while y in existing:
        y = "%s_%d" % (x, c)
        c += 1
    return y


class DOCXNamespace(object):
    """
    Provide the docxnamespace contract for validated ebook processing.

    Example:
        Exercise DOCXNamespace through a consuming regression::

            python -m pytest -q tests/file_formats/docx/test_docx_modernized.py
    """
    def __init__(self: _typing.Self, transitional: bool = True) -> None:
        """
        Initialize and validate the docxnamespace state.

        Example:
            Exercise DOCXNamespace.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :param transitional: Value supplied for transitional under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.xpath_cache = {}
        if transitional:
            self.namespaces = TRANSITIONAL_NAMESPACES.copy()
            self.names = TRANSITIONAL_NAMES.copy()
        else:
            self.namespaces = STRICT_NAMESPACES.copy()
            self.names = STRICT_NAMES.copy()

    def XPath(self: _typing.Self, expr: _typing.Any) -> _typing.Any:
        """
        Perform the XPath operation under explicit file-format and conversion rules.

        Example:
            Exercise DOCXNamespace.XPath through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :param expr: Value supplied for expr under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        ans = self.xpath_cache.get(expr, None)
        if ans is None:
            self.xpath_cache[expr] = ans = Xp(expr, namespaces=self.namespaces)
        return ans

    def is_tag(self: _typing.Self, x: _typing.Any, q: _typing.Any) -> bool:
        """
        Return whether is tag holds for the supplied ebook data.

        Example:
            Exercise DOCXNamespace.is tag through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :param x: Value supplied for x under the utility contract.
        :param q: Value supplied for q under the utility contract.
        :return: True when the documented condition holds; otherwise False.
        """
        tag = getattr(x, "tag", x)
        ns, name = q.partition(":")[0::2]
        return "{%s}%s" % (self.namespaces.get(ns, None), name) == tag

    def expand(self: _typing.Self, name: _typing.Any, sep: str = ":") -> bool:
        """
        Perform the expand operation under explicit file-format and conversion rules.

        Example:
            Exercise DOCXNamespace.expand through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param sep: Delimiter used to split or join list values.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        ns, tag = name.partition(sep)[::2]
        if ns and tag:
            tag = "{%s}%s" % (self.namespaces[ns], tag)
        return tag or ns

    def get(self: _typing.Self, x: _typing.Any, attr: _typing.Any, default: _typing.Any = None) -> _typing.Any:
        """
        Perform the get operation under explicit file-format and conversion rules.

        Example:
            Exercise DOCXNamespace.get through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :param x: Value supplied for x under the utility contract.
        :param attr: Value supplied for attr under the utility contract.
        :param default: Value supplied for default under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return x.attrib.get(self.expand(attr), default)

    def ancestor(self: _typing.Self, elem: _typing.Any, name: _typing.Any) -> _typing.Any:
        """
        Perform the ancestor operation under explicit file-format and conversion rules.

        Example:
            Exercise DOCXNamespace.ancestor through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :param elem: Value supplied for elem under the utility contract.
        :param name: Field, file, function or resource name addressed by the operation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        try:
            return self.XPath("ancestor::%s[1]" % name)(elem)[0]
        except IndexError:
            return None

    def children(self: _typing.Self, elem: _typing.Any, *args: _typing.Any) -> _typing.Any:
        """
        Perform the children operation under explicit file-format and conversion rules.

        Example:
            Exercise DOCXNamespace.children through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :param elem: Value supplied for elem under the utility contract.
        :param args: Positional values forwarded to the compatibility implementation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.XPath("|".join("child::%s" % a for a in args))(elem)

    def descendants(self: _typing.Self, elem: _typing.Any, *args: _typing.Any) -> _typing.Any:
        """
        Perform the descendants operation under explicit file-format and conversion rules.

        Example:
            Exercise DOCXNamespace.descendants through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :param elem: Value supplied for elem under the utility contract.
        :param args: Positional values forwarded to the compatibility implementation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.XPath("|".join("descendant::%s" % a for a in args))(elem)

    def makeelement(self: _typing.Self, root: _typing.Any, tag: _typing.Any, append: bool = True, **attrs: _typing.Any) -> _typing.Any:
        """
        Perform the makeelement operation under explicit file-format and conversion rules.

        Example:
            Exercise DOCXNamespace.makeelement through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :param root: Root directory that bounds path resolution or traversal.
        :param tag: Value supplied for tag under the utility contract.
        :param append: Value supplied for append under the utility contract.
        :param attrs: Value supplied for attrs under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        ans = root.makeelement(self.expand(tag), **{self.expand(k, sep="_"): v for k, v in iteritems(attrs)})
        if append:
            root.append(ans)
        return ans
