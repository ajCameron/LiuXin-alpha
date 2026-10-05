"""
Discover and normalize EPUB page-list navigation targets.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise pages through a consuming regression::

        python -m pytest -q tests/file_formats/epub/test_epub_modernized.py
"""
from __future__ import with_statement
from __future__ import annotations

import typing as _typing

"""
Add page mapping information to an EPUB book.
"""

import re
from itertools import count

from LiuXin_alpha.file_formats.oeb.base import XHTML_NS, OEBBook
from LiuXin_alpha.utils.libraries.liuxin_etree import etree
from LiuXin_alpha.utils.logging import default_log

__license__ = "GPL v3"
__copyright__ = "2008, Marshall T. Vandegrift <llasram@gmail.com>"
__docformat__ = "restructuredtext en"

NSMAP = {"h": XHTML_NS, "html": XHTML_NS, "xhtml": XHTML_NS}
PAGE_RE = re.compile(r"page", re.IGNORECASE)
ROMAN_RE = re.compile(r"^[ivxlcdm]+$", re.IGNORECASE)


def filter_name(name: _typing.Any) -> _typing.Any:
    """
    Perform the filter name operation under explicit file-format and conversion rules.

    Example:
        Exercise filter name through a consuming regression::

            python -m pytest -q tests/file_formats/epub/test_epub_modernized.py


    :param name: Field, file, function or resource name addressed by the operation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    name = name.strip()
    name = PAGE_RE.sub("", name)
    for word in name.split():
        if word.isdigit() or ROMAN_RE.match(word):
            name = word
            break
    return name


def build_name_for(expr: _typing.Any) -> _typing.Any:
    """
    Perform the build name for operation under explicit file-format and conversion rules.

    Example:
        Exercise build name for through a consuming regression::

            python -m pytest -q tests/file_formats/epub/test_epub_modernized.py


    :param expr: Value supplied for expr under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if not expr:
        counter = count(1)
        return lambda elem: str(next(counter))
    selector = etree.XPath(expr, namespaces=NSMAP)

    def name_for(elem: _typing.Any) -> _typing.Any:
        """
        Perform the name for operation under explicit file-format and conversion rules.

        Example:
            Exercise build name for.name for through a consuming regression::

                python -m pytest -q tests/file_formats/epub/test_epub_modernized.py


        :param elem: Value supplied for elem under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        results = selector(elem)
        if not results:
            return ""
        if isinstance(results, (str, bytes)):
            name = results.decode("utf-8", "replace") if isinstance(results, bytes) else results
        else:
            text_bits = []
            for part in results:
                text_bits.append(str(part))
            name = " ".join(text_bits)
        return filter_name(name)

    return name_for


def add_page_map(opfpath: _typing.Any, opts: _typing.Any) -> None:
    """
    Perform the add page map operation under explicit file-format and conversion rules.

    Example:
        Exercise add page map through a consuming regression::

            python -m pytest -q tests/file_formats/epub/test_epub_modernized.py


    :param opfpath: Value supplied for opfpath under the utility contract.
    :param opts: Value supplied for opts under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    try:
        from LiuXin_alpha.file_formats.oeb.reader import OEBReader
        from LiuXin_alpha.file_formats.oeb.writer import OEBWriter
    except Exception as e:
        raise RuntimeError("add_page_map currently depends on the OEB reader/writer stack") from e

    if not getattr(opts, "page", None):
        raise ValueError("A page selector is required to add a page map")

    oeb = OEBBook(default_log, lambda x: x, pretty_print=bool(getattr(opts, "pretty_print", False)))
    OEBReader()(oeb, opfpath)
    selector = etree.XPath(opts.page, namespaces=NSMAP)
    name_for = build_name_for(opts.page_names)
    idgen = ("calibre-page-%d" % n for n in count(1))
    for item in oeb.spine:
        data = item.data
        for elem in selector(data):
            name = name_for(elem)
            item_id = elem.get("id", None)
            if item_id is None:
                item_id = next(idgen)
                elem.set("id", item_id)
            href = "#".join((item.href, item_id))
            oeb.pages.add(name, href)
    writer = OEBWriter(version="2.0", page_map=True, pretty_print=bool(getattr(opts, "pretty_print", False)))
    writer(oeb, opfpath)
