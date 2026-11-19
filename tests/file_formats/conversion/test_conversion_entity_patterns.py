"""
Check that the conversion paths resolve entities in linear time, with the same results.

The HTML preprocessor, the heuristic processor's ``<pre>`` pass, the LRF
converter's markup massage and LRF table-of-contents labels each compiled
``&(\\S+?);``, which scans from every ``&`` in a run with no ``;`` to the end of
the run: 80 KB of ``&`` took 33 s. The ``<pre>`` pass also retried its block
pattern from every position after the last ``</pre>``: 40 KB took 8.9 s. Each
now goes through ``LiuXin_alpha.utils.linear_time``.
"""

from __future__ import annotations

import time
from collections.abc import Callable

import pytest

from LiuXin_alpha.file_formats.conversion.preprocess import HTMLPreProcessor
from LiuXin_alpha.file_formats.conversion.utils import HeuristicProcessor
from LiuXin_alpha.file_formats.lrf.html.convert_from import HTMLConverter
from LiuXin_alpha.file_formats.lrf.pylrs.pylrs import TocLabel
from LiuXin_alpha.file_formats.mobi.reader.mobi6 import MobiReader
from LiuXin_alpha.utils.linear_time import ENTITY_PATTERN
from LiuXin_alpha.utils.logging import default_log


def massage(markup: str) -> str:
    """
    Apply the LRF converter's markup massage as BeautifulSoup does.

    Example:
        >>> massage("<p>&eacute;</p>")
        '<p>é</p>'


    :param markup: The HTML.
    :return: The massaged HTML.
    """
    for pattern, replacement in HTMLConverter.MARKUP_MASSAGE:
        markup = pattern.sub(replacement, markup)
    return markup


def preprocess(html: str) -> str:
    """
    Run the HTML preprocessor with its default options.

    Example:
        >>> "é" in preprocess("<html><body><p>&eacute;</p></body></html>")
        True


    :param html: The HTML.
    :return: The preprocessed HTML.
    """
    return str(HTMLPreProcessor(default_log)(html))


def markup_pre(html: str) -> str:
    """
    Run the heuristic processor's ``<pre>`` pass.

    Example:
        >>> "é" in markup_pre("<pre>&eacute;</pre>")
        True


    :param html: The HTML.
    :return: The marked-up HTML.
    """
    return str(HeuristicProcessor(log=default_log).markup_pre(html))


def toc_label(label: str) -> str:
    """
    Make an LRF table-of-contents label.

    Example:
        >>> toc_label("&eacute;")
        'é'


    :param label: The label as the book gives it.
    :return: The stored label.
    """
    return str(TocLabel(label, None).label)


PATHS: dict[str, Callable[[str], str]] = {
    "html preprocessor": preprocess,
    "heuristic <pre> pass": markup_pre,
    "lrf markup massage": massage,
    "lrf toc label": toc_label,
}


def test_the_entity_rules_are_the_linear_stand_in() -> None:
    """
    Keep the shared stand-in in the rule tables and the MOBI 6 reader.

    Example:
        >>> test_the_entity_rules_are_the_linear_stand_in()


    :return: None; the assertions are the check.
    """
    assert any(pattern is ENTITY_PATTERN for pattern, _ in HTMLPreProcessor.PREPROCESS)
    assert any(pattern is ENTITY_PATTERN for pattern, _ in HTMLConverter.MARKUP_MASSAGE)
    assert MobiReader.ENTITY_PAT is ENTITY_PATTERN
    rules = [pattern.pattern for pattern, _ in HTMLPreProcessor.PREPROCESS + HTMLConverter.MARKUP_MASSAGE]
    assert rules.count(r"&(\S+?);") == 2


def test_entities_are_still_resolved() -> None:
    """
    Resolve entities on each path as before, leaving runs of ``&`` alone.

    Example:
        >>> test_entities_are_still_resolved()


    :return: None; the assertions are the check.
    """
    assert "<p>&amp;Aé&&x;</p>" in preprocess("<html><body><p>&amp;&#65;&eacute;&&x;</p></body></html>")
    assert massage("<p>&amp;&#65;&eacute;&&x;</p>") == "<p>&amp;Aé&&x;</p>"
    assert toc_label("a &eacute; &amp; b&&") == "a é &amp; b&amp;&amp;"
    marked = markup_pre("<html><body><pre>caf&eacute; &amp; more</pre><p>&eacute;&&</p></body></html>")
    assert "café" in marked and "&amp;" in marked
    assert marked.endswith("<p>é&&</p></body></html>")


def test_unknown_entities_are_kept() -> None:
    """
    Keep an entity the tables do not know, which used to fail the conversion.

    The resolver looked the name up in a module path that no longer exists.

    Example:
        >>> test_unknown_entities_are_kept()


    :return: None; the assertions are the check.
    """
    assert "<p>&foo; é</p>" in preprocess("<html><body><p>&foo; &eacute;</p></body></html>")
    assert massage("<p>&foo;</p>") == "<p>&foo;</p>"


@pytest.mark.parametrize("name", sorted(PATHS))
def test_crafted_text_converts_quickly(name: str) -> None:
    """
    Read a megabyte of ``&`` after a ``<pre>`` block in well under the time the patterns took.

    Example:
        >>> test_crafted_text_converts_quickly("lrf toc label")  # doctest: +SKIP


    :param name: Which path.
    :return: None; the assertions are the check.
    """
    text = "<html><body><pre>x</pre><p>" + "&" * 1_000_000 + "</p></body></html>"
    started = time.monotonic()
    PATHS[name](text)
    assert time.monotonic() - started < 5
