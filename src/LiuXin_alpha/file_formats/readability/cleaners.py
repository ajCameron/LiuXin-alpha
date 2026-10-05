"""
Clean noisy HTML before readability content extraction.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise cleaners through a consuming regression::

        python -m pytest -q tests/file_formats/readability/test_readability_modernized.py
"""

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
    """
    Perform the clean attributes operation under explicit file-format and conversion rules.

    Example:
        Exercise clean attributes through a consuming regression::

            python -m pytest -q tests/file_formats/readability/test_readability_modernized.py


    :param html: Value supplied for html under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    while _HTMLSTRIP_RE.search(html):
        html = _HTMLSTRIP_RE.sub("<\\1\\2>", html)
    return html


def normalize_spaces(text: str | None) -> str:
    """
    Normalize spaces under the format's safety and compatibility rules.

    Example:
        Exercise normalize spaces through a consuming regression::

            python -m pytest -q tests/file_formats/readability/test_readability_modernized.py


    :param text: Text parsed, normalized or rendered.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if not text:
        return ""
    return " ".join(text.split())


class _FallbackCleaner:
    """
    Minimal cleaner used when `lxml.html.clean` extras are unavailable.

    Example:
        Exercise  FallbackCleaner through a consuming regression::

            python -m pytest -q tests/file_formats/readability/test_readability_modernized.py
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
        """
        Initialize and validate the fallbackcleaner state.

        Example:
            Exercise  FallbackCleaner.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/readability/test_readability_modernized.py


        :param scripts: Value supplied for scripts under the utility contract.
        :param style: Value supplied for style under the utility contract.
        :param links: Value supplied for links under the utility contract.
        :param comments: Value supplied for comments under the utility contract.
        :param processing_instructions: Value supplied for processing instructions under the
            utility contract.
        :param _ignored: Value supplied for ignored under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.scripts = scripts
        self.style = style
        self.links = links
        self.comments = comments
        self.processing_instructions = processing_instructions

    def clean_html(self: _typing.Self, doc: _typing.Any) -> _typing.Any:
        """
        Perform the clean html operation under explicit file-format and conversion rules.

        Example:
            Exercise  FallbackCleaner.clean html through a consuming regression::

                python -m pytest -q tests/file_formats/readability/test_readability_modernized.py


        :param doc: Value supplied for doc under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
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
