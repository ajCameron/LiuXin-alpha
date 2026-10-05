"""
Apply Markdown attribute lists to generated elements.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise attr list through a consuming regression::

        python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py
"""
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import annotations

import typing as _typing

"""
Attribute List Extension for Python-Markdown
============================================

Adds attribute list syntax. Inspired by 
[maruku](http://maruku.rubyforge.org/proposal.html#attribute_lists)'s
feature of the same name.

Copyright 2011 [Waylan Limberg](http://achinghead.com/).

Contact: markdown@freewisdom.org

License: BSD (see ../LICENSE.md for details) 

Dependencies:
* [Python 2.4+](http://python.org)
* [Markdown 2.1+](http://packages.python.org/Markdown/)

"""

import re

from . import Extension
from ..treeprocessors import Treeprocessor
from ..util import isBlockLevel


try:
    Scanner = re.Scanner
except AttributeError:
    # must be on Python 2.4
    from sre import Scanner


def _handle_double_quote(s: _typing.Any, t: _typing.Any) -> tuple[_typing.Any, ...]:
    """
    Perform the handle double quote operation under explicit file-format and conversion rules.

    Example:
        Exercise  handle double quote through a consuming regression::

            python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


    :param s: Value supplied for s under the utility contract.
    :param t: Value supplied for t under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    k, v = t.split("=")
    return k, v.strip('"')


def _handle_single_quote(s: _typing.Any, t: _typing.Any) -> tuple[_typing.Any, ...]:
    """
    Perform the handle single quote operation under explicit file-format and conversion rules.

    Example:
        Exercise  handle single quote through a consuming regression::

            python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


    :param s: Value supplied for s under the utility contract.
    :param t: Value supplied for t under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    k, v = t.split("=")
    return k, v.strip("'")


def _handle_key_value(s: _typing.Any, t: _typing.Any) -> _typing.Any:
    """
    Perform the handle key value operation under explicit file-format and conversion rules.

    Example:
        Exercise  handle key value through a consuming regression::

            python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


    :param s: Value supplied for s under the utility contract.
    :param t: Value supplied for t under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return t.split("=")


def _handle_word(s: _typing.Any, t: _typing.Any) -> tuple[_typing.Any, ...]:
    """
    Perform the handle word operation under explicit file-format and conversion rules.

    Example:
        Exercise  handle word through a consuming regression::

            python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


    :param s: Value supplied for s under the utility contract.
    :param t: Value supplied for t under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if t.startswith("."):
        return ".", t[1:]
    if t.startswith("#"):
        return "id", t[1:]
    return t, t


_scanner = Scanner(
    [
        (r'[^ ]+=".*?"', _handle_double_quote),
        (r"[^ ]+='.*?'", _handle_single_quote),
        (r"[^ ]+=[^ ]*", _handle_key_value),
        (r"[^ ]+", _handle_word),
        (r" ", None),
    ]
)


def get_attrs(str: _typing.Any) -> _typing.Any:
    """
    Parse attribute list and return a list of attribute tuples.

    Example:
        Exercise get attrs through a consuming regression::

            python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


    :param str: Value supplied for str under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return _scanner.scan(str)[0]


def isheader(elem: _typing.Any) -> bool:
    """
    Perform the isheader operation under explicit file-format and conversion rules.

    Example:
        Exercise isheader through a consuming regression::

            python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


    :param elem: Value supplied for elem under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return elem.tag in ["h1", "h2", "h3", "h4", "h5", "h6"]


class AttrListTreeprocessor(Treeprocessor):

    """
    Provide the attrlisttreeprocessor contract for validated ebook processing.

    Example:
        Exercise AttrListTreeprocessor through a consuming regression::

            python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py
    """
    BASE_RE = r"\{\:?([^\}]*)\}"
    HEADER_RE = re.compile(r"[ ]*%s[ ]*$" % BASE_RE)
    BLOCK_RE = re.compile(r"\n[ ]*%s[ ]*$" % BASE_RE)
    INLINE_RE = re.compile(r"^%s" % BASE_RE)
    NAME_RE = re.compile(
        r"[^A-Z_a-z\u00c0-\u00d6\u00d8-\u00f6\u00f8-\u02ff\u0370-\u037d"
        r"\u037f-\u1fff\u200c-\u200d\u2070-\u218f\u2c00-\u2fef"
        r"\u3001-\ud7ff\uf900-\ufdcf\ufdf0-\ufffd"
        r"\:\-\.0-9\u00b7\u0300-\u036f\u203f-\u2040]+"
    )

    def run(self: _typing.Self, doc: _typing.Any) -> None:
        """
        Execute the configured conversion stage and return its primary result.

        Example:
            Exercise AttrListTreeprocessor.run through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param doc: Value supplied for doc under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        for elem in doc.iter():
            if isBlockLevel(elem.tag):
                # Block level: check for attrs on last line of text
                RE = self.BLOCK_RE
                if isheader(elem):
                    # header: check for attrs at end of line
                    RE = self.HEADER_RE
                if len(elem) and elem[-1].tail:
                    # has children. Get from tail of last child
                    m = RE.search(elem[-1].tail)
                    if m:
                        self.assign_attrs(elem, m.group(1))
                        elem[-1].tail = elem[-1].tail[: m.start()]
                        if isheader(elem):
                            # clean up trailing #s
                            elem[-1].tail = elem[-1].tail.rstrip("#").rstrip()
                elif elem.text:
                    # no children. Get from text.
                    m = RE.search(elem.text)
                    if m:
                        self.assign_attrs(elem, m.group(1))
                        elem.text = elem.text[: m.start()]
                        if isheader(elem):
                            # clean up trailing #s
                            elem.text = elem.text.rstrip("#").rstrip()
            else:
                # inline: check for attrs at start of tail
                if elem.tail:
                    m = self.INLINE_RE.match(elem.tail)
                    if m:
                        self.assign_attrs(elem, m.group(1))
                        elem.tail = elem.tail[m.end() :]

    def assign_attrs(self: _typing.Self, elem: _typing.Any, attrs: _typing.Any) -> None:
        """
        Assign attrs to element.

        Example:
            Exercise AttrListTreeprocessor.assign attrs through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param elem: Value supplied for elem under the utility contract.
        :param attrs: Value supplied for attrs under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        for k, v in get_attrs(attrs):
            if k == ".":
                # add to class
                cls = elem.get("class")
                if cls:
                    elem.set("class", "%s %s" % (cls, v))
                else:
                    elem.set("class", v)
            else:
                # assign attr k with v
                elem.set(self.sanitize_name(k), v)

    def sanitize_name(self: _typing.Self, name: _typing.Any) -> _typing.Any:
        """
        Sanitize name as 'an XML Name, minus the ":"'. See http://www.w3.org/TR/REC-xml-names/#NT-NCName

        Example:
            Exercise AttrListTreeprocessor.sanitize name through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param name: Field, file, function or resource name addressed by the operation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.NAME_RE.sub("_", name)


class AttrListExtension(Extension):
    """
    Provide the attrlistextension contract for validated ebook processing.

    Example:
        Exercise AttrListExtension through a consuming regression::

            python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py
    """
    def extendMarkdown(self: _typing.Self, md: _typing.Any, md_globals: _typing.Any) -> None:
        """
        Perform the extendMarkdown operation under explicit file-format and conversion rules.

        Example:
            Exercise AttrListExtension.extendMarkdown through a consuming regression::

                python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


        :param md: Value supplied for md under the utility contract.
        :param md_globals: Value supplied for md globals under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        md.treeprocessors.add("attr_list", AttrListTreeprocessor(md), ">prettify")


def makeExtension(configs: _typing.Any = None) -> _typing.Any:
    """
    Perform the makeExtension operation under explicit file-format and conversion rules.

    Example:
        Exercise makeExtension through a consuming regression::

            python -m pytest -q tests/file_formats/markdown/test_markdown_modernized.py


    :param configs: Value supplied for configs under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return AttrListExtension(configs=configs)
