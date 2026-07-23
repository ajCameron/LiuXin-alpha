"""Readability HTML cleanup helpers."""

from __future__ import annotations

import typing as _typing

import re

# lxml 5 split html.clean into a separate package.
try:  # pragma: no cover - depends on install extras
    from lxml.html.clean import Cleaner as _LxmlCleaner
except Exception:  # pragma: no cover - depends on runtime env
    try:
        from lxml_html_clean import Cleaner as _LxmlCleaner  # type: ignore
    except Exception:
        _LxmlCleaner = None


_BAD_ATTRS = ["width", "height", "style", "[-a-z]*color", "background[-a-z]*", "on[a-z]+"]
_SINGLE_QUOTED = "'[^']+'"
_DOUBLE_QUOTED = '"[^"]+"'
_NON_SPACE = '[^ "\'>]+'
_HTMLSTRIP_RE = re.compile(
    "<"  # open
    "([^>]+) "  # prefix
    "(?:%s) *" % ("|".join(_BAD_ATTRS),)
    + "= *(?:%s|%s|%s)" % (_NON_SPACE, _SINGLE_QUOTED, _DOUBLE_QUOTED)  # undesirable attributes
    + "([^>]*)"  # postfix
    ">",
    re.I,
)


def clean_attributes(html: str) -> str:
    while _HTMLSTRIP_RE.search(html):
        html = _HTMLSTRIP_RE.sub("<\\1\\2>", html)
    return html


def normalize_spaces(text: str | None) -> str:
    if not text:
        return ""
    return " ".join(text.split())


class _FallbackCleaner:
    """
    Minimal cleaner used when `lxml.html.clean` extras are unavailable.

    This intentionally performs only the parts readability depends on:
    stripping script/style/link tags and comments/processing instructions.
    """

    def __init__(
        self: _typing.Self,
        scripts: bool = True,
        style: bool = True,
        links: bool = True,
        comments: bool = True,
        processing_instructions: bool = True,
        **_ignored: _typing.Any,
    ) -> None:
        self.scripts = scripts
        self.style = style
        self.links = links
        self.comments = comments
        self.processing_instructions = processing_instructions

    def clean_html(self: _typing.Self, doc: _typing.Any) -> _typing.Any:
        if self.scripts:
            for elem in list(doc.xpath(".//script")):
                parent = elem.getparent()
                if parent is not None:
                    parent.remove(elem)
        if self.style:
            for elem in list(doc.xpath(".//style")):
                parent = elem.getparent()
                if parent is not None:
                    parent.remove(elem)
        if self.links:
            for elem in list(doc.xpath(".//link")):
                parent = elem.getparent()
                if parent is not None:
                    parent.remove(elem)
        if self.comments:
            for comment in list(doc.xpath("//comment()")):
                parent = comment.getparent()
                if parent is not None:
                    parent.remove(comment)
        if self.processing_instructions:
            for pi in list(doc.xpath("//processing-instruction()")):
                parent = pi.getparent()
                if parent is not None:
                    parent.remove(pi)
        return doc


_CleanerImpl = _LxmlCleaner or _FallbackCleaner

html_cleaner = _CleanerImpl(
    scripts=True,
    javascript=True,
    comments=True,
    style=True,
    links=True,
    meta=False,
    add_nofollow=False,
    page_structure=False,
    processing_instructions=True,
    embedded=False,
    frames=False,
    forms=False,
    annoying_tags=False,
    remove_tags=None,
    remove_unknown_tags=False,
    safe_attrs_only=False,
)


__all__ = [
    "clean_attributes",
    "normalize_spaces",
    "html_cleaner",
    "_FallbackCleaner",
]
