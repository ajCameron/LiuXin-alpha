# Derived from calibre (https://calibre-ebook.com) 9.18:
# src/calibre/ebooks/metadata/__init__.py, src/calibre/utils/xml_parse.py and
# src/calibre/__init__.py. License: GPL v3. Copyright: Kovid Goyal.
# SPDX-License-Identifier: GPL-3.0-only
"""
The calibre helpers the ported metadata readers need, ported or mapped.

``string_to_authors``, ``safe_xml_fromstring``, ``MetaInformation`` and
``prints`` are ported from calibre 9.18 so the readers behave as upstream does.
``string_to_authors`` uses calibre's default ``authors_split_regex`` tweak,
because LiuXin has no tweaks file. ``_`` returns its text unchanged, as
calibre's does when no translation is installed. ``Metadata`` is calibre's
metadata class from the project's own copy, ``metadata.book.base``.

Example:
    >>> string_to_authors("Ann Smith and Bob Jones & Cy")
    ['Ann Smith', 'Bob Jones', 'Cy']
"""

from __future__ import annotations

import re
import sys
from collections.abc import Collection
from typing import Any

from lxml import etree

from LiuXin_alpha.metadata.book.base import calibreMetadata as Metadata

__all__ = [
    "MetaInformation",
    "Metadata",
    "_",
    "prints",
    "safe_xml_fromstring",
    "string_to_authors",
]

_author_pat = re.compile(r"(?i),?\s+(and|with)\s+")


def _(text: str) -> str:
    """
    Return text unchanged, as calibre's ``_`` does without a translation.

    Example:
        >>> _("Unknown")
        'Unknown'


    :param text: The text to translate.
    :return: The same text.
    """
    return text


def string_to_authors(raw: str | None) -> list[str]:
    """
    Split an author string into names, as calibre 9.18 does.

    Example:
        >>> string_to_authors("Tom && Jerry with Spike")
        ['Tom & Jerry', 'Spike']


    :param raw: Names separated by ``&``, ``and`` or ``with``; ``&&`` is a
        literal ampersand.
    :return: The names, without empty ones.
    """
    if not raw:
        return []
    raw = raw.replace("&&", "￿")
    raw = _author_pat.sub("&", raw)
    authors = [a.strip().replace("￿", "&") for a in raw.split("&")]
    return [a for a in authors if a]


def MetaInformation(  # noqa: N802 - calibre's name
    title: Any, authors: Collection[str] | None = (_("Unknown"),)
) -> Any:
    """
    Make calibre ``Metadata``, as calibre 9.18's compatibility function does.

    Example:
        >>> mi = MetaInformation("A Book", ["Ann"])
        >>> mi.title, mi.authors
        ('A Book', ['Ann'])


    :param title: A title, or an object with ``title`` and ``authors``.
    :param authors: The authors' names.
    :return: calibre ``Metadata``.
    """
    mi = None
    if hasattr(title, "title") and hasattr(title, "authors"):
        mi = title
        title = mi.title
        authors = mi.authors
    return Metadata(title, authors, other=mi)


def prints(*args: Any, **kwargs: Any) -> None:
    """
    Print to a stream, as calibre's ``prints`` does for text.

    Example:
        >>> prints("a", "b", sep="-")
        a-b


    :param args: Things to print.
    :param kwargs: ``file``, ``sep`` and ``end`` as for ``print``.
    :return: None.
    """
    stream = kwargs.get("file", sys.stdout)
    print(
        *args,
        sep=kwargs.get("sep", " "),
        end=kwargs.get("end", "\n"),
        file=stream,
    )


class _Resolver(etree.Resolver):
    """
    Resolve every external entity to nothing, as calibre's parser does.

    Example:
        >>> isinstance(_Resolver(), etree.Resolver)
        True
    """

    def resolve(self, url: Any, id: Any, context: Any) -> Any:  # noqa: A002
        """
        Answer an entity reference with an empty document.

        Example:
            Used by lxml while parsing; not called directly.


        :param url: The entity's system identifier.
        :param id: The entity's public identifier.
        :param context: lxml's resolver context.
        :return: An empty document.
        """
        return self.resolve_string("", context)


def safe_xml_fromstring(string_or_bytes: str | bytes, recover: bool = True) -> Any:
    """
    Parse XML without network access or external entities, as calibre does.

    Example:
        >>> safe_xml_fromstring("<a><b>x</b></a>").find("b").text
        'x'


    :param string_or_bytes: The document.
    :param recover: Whether to recover from errors.
    :return: The root element.
    """
    encoding = None
    raw = string_or_bytes
    if isinstance(raw, str):
        raw = raw.encode("utf-8")
        encoding = "utf-8"
    parser = etree.XMLParser(recover=recover, no_network=True, encoding=encoding)
    parser.resolvers.add(_Resolver())
    return etree.fromstring(raw, parser=parser)
