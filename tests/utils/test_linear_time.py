"""
Check that the linear-time stand-ins find exactly what the backtracking patterns find, quickly.

Each stand-in is compared with its original pattern on random text built from
the pattern's pieces, for ``finditer``, ``search`` and ``sub``. Then crafted
input that took the original patterns minutes or more is decoded through
``calibre_chardet`` and ``xml_utils`` in well under a second.
"""

from __future__ import annotations

import random
import re
import time
from collections.abc import Callable, Iterator
from typing import Any

import pytest

from LiuXin_alpha.utils.libraries import calibre_chardet
from LiuXin_alpha.utils import linear_time
from LiuXin_alpha.utils.linear_time import (
    ENTITY_PATTERN,
    HTML4_CHARSET,
    XML_DECLARATION,
    ForwardFinder,
    LinearPattern,
    entities,
    html4_charsets,
    sub_until_failure,
    xml_declarations,
)
from LiuXin_alpha.utils.text import xml_utils

ENTITY = r"&(\S+?);"
XML_PIECES = ["<?", "<", ">", "?>", "encoding", "ENCODING", "encodıng", "=", " = ", "'", '"', "x", "utf-8", "\n", " "]
META_PIECES = [
    "<meta", "<META", " ", "\n", "content", "CONTENT", "=", '"', "'", "charset=", "chaRSET=", "ſ", "utf-8", "K", "x", "<", ">",
    "</meta>", " </META>",
]
ENTITY_PIECES = ["&", ";", "amp", "#", "x41", " ", "\n", "\t", "a", "&;"]
CASES: dict[str, tuple[str, int, Callable[[Any], Iterator[re.Match[Any]]], list[str]]] = {
    "xml declaration": (XML_DECLARATION, re.IGNORECASE, xml_declarations, XML_PIECES),
    "html 4 charset": (HTML4_CHARSET, re.IGNORECASE, html4_charsets, META_PIECES),
    "entity": (ENTITY, 0, entities, ENTITY_PIECES),
}


def calibre_chardet_source() -> str:
    """
    Read the patterns as ``calibre_chardet`` writes them.

    Example:
        >>> "ENCODING_PATS" in calibre_chardet_source()
        True


    :return: The module's source.
    """
    with open(calibre_chardet.__file__, encoding="utf-8") as handle:
        return handle.read()


def test_the_stand_ins_wrap_the_patterns_as_written() -> None:
    """
    Keep the original patterns, so the stand-ins are checked against what was there.

    Example:
        >>> test_the_stand_ins_wrap_the_patterns_as_written()


    :return: None; the assertions are the check.
    """
    assert XML_DECLARATION == r"<\?[^<>]+encoding\s*=\s*['\"](.*?)['\"][^<>]*>"
    assert HTML4_CHARSET == (
        r"""<meta\s+?[^<>]*?content\s*=\s*['\"][^'\"]*?charset=([-_a-z0-9]+)[^'\"]*?['\"][^<>]*>(?:\s*</meta>){0,1}"""
    )
    xml, html5, html4 = calibre_chardet.ENCODING_PATS
    assert isinstance(xml, LinearPattern) and xml.pattern == XML_DECLARATION
    assert isinstance(html5, re.Pattern)
    assert isinstance(html4, LinearPattern) and html4.pattern == HTML4_CHARSET
    assert linear_time.ENTITY == ENTITY
    assert isinstance(ENTITY_PATTERN, LinearPattern) and ENTITY_PATTERN.pattern == ENTITY
    assert calibre_chardet.ENTITY_PATTERN is ENTITY_PATTERN
    assert xml_utils._ent_pat is ENTITY_PATTERN


@pytest.mark.parametrize("binary", [False, True])
@pytest.mark.parametrize("name", sorted(CASES))
def test_stand_ins_find_what_the_patterns_find(name: str, binary: bool) -> None:
    """
    Give the same matches, spans and groups as the pattern, and the same replacements.

    Example:
        >>> test_stand_ins_find_what_the_patterns_find("entity", False)  # doctest: +SKIP


    :param name: Which pattern.
    :param binary: Whether the text is bytes.
    :return: None; the assertions are the check.
    """
    source, flags, matches, pieces = CASES[name]
    if binary:
        pattern = re.compile(source.encode(), flags)
        alphabet: list[Any] = [piece.encode() for piece in pieces if piece.isascii()]
    else:
        pattern = re.compile(source, flags)
        alphabet = pieces
    stand_in = LinearPattern(pattern, matches)

    def tagged(match: re.Match[Any]) -> Any:
        """
        Replace a match with its group and where it starts, as callers' functions use them.

        Example:
            See the test.


        :param match: The match.
        :return: The replacement.
        """
        text = b"[%s|%d]" if binary else "[%s|%d]"
        return text % (match.group(1), match.start(1) - match.start())

    chooser = random.Random(name + str(binary))
    for _ in range(3000):
        text = alphabet[0][:0].join(chooser.choice(alphabet) for _ in range(chooser.randint(0, 30)))
        assert [m.regs for m in stand_in.finditer(text)] == [m.regs for m in pattern.finditer(text)], text
        assert stand_in.sub(text[:0], text) == pattern.sub(text[:0], text), text
        assert stand_in.sub(tagged, text) == pattern.sub(tagged, text), text
        assert stand_in.sub(tagged, text, count=1) == pattern.sub(tagged, text, count=1), text
        found, expected = stand_in.search(text), pattern.search(text)
        assert (found and found.regs) == (expected and expected.regs), text


CRAFTED = {
    "unclosed xml declarations": b"<?" + b'encoding="' * 100_000,
    "meta and spaces": b"<meta" + b" " * 1_000_000,
    "meta charsets": b'<meta content="' + b"charset=a " * 100_000,
    "meta contents": b"<meta " + b'content="charset=a" ' * 50_000 + b"<",
    "ampersands": b"&" * 1_000_000,
}


@pytest.mark.parametrize("name", sorted(CRAFTED))
def test_crafted_documents_decode_quickly(name: str) -> None:
    """
    Decode 1 MB of crafted text, resolving entities, in well under the time the patterns took.

    Example:
        >>> test_crafted_documents_decode_quickly("ampersands")  # doctest: +SKIP


    :param name: Which crafted text.
    :return: None; the assertions are the check.
    """
    raw = CRAFTED[name]
    started = time.monotonic()
    text, _ = calibre_chardet.xml_to_unicode(raw, strip_encoding_pats=True, resolve_entities=True)
    calibre_chardet.replace_encoding_declarations(raw)
    calibre_chardet.find_declared_encoding(raw)
    xml_utils.xml_replace_entities(text)
    assert time.monotonic() - started < 5


def test_declarations_are_still_found_and_replaced() -> None:
    """
    Read, strip and replace ordinary declarations as before.

    Example:
        >>> test_declarations_are_still_found_and_replaced()


    :return: None; the assertions are the check.
    """
    xml = b'<?xml version="1.0" encoding="ISO-8859-1"?><r>&#65;&amp;</r>'
    html4 = b'<html><head><meta http-equiv="Content-Type" content="text/html; charset=windows-1252"></head></html>'
    assert calibre_chardet.detect_xml_encoding(xml)[1] == "ISO-8859-1"
    assert calibre_chardet.detect_xml_encoding(html4)[1] == "windows-1252"
    assert calibre_chardet.xml_to_unicode(xml, strip_encoding_pats=True, resolve_entities=True)[0] == "<r>A&amp;</r>"
    replaced, changed = calibre_chardet.replace_encoding_declarations(html4.decode())
    assert changed and isinstance(replaced, str) and "charset=utf-8" in replaced
    assert calibre_chardet.find_declared_encoding(xml) == "ISO-8859-1"


def test_forward_finder_answers_as_find_and_rfind() -> None:
    """
    Answer like ``bytes.find`` and ``bytes.rfind`` for growing positions.

    Example:
        >>> test_forward_finder_answers_as_find_and_rfind()


    :return: None; the assertions are the check.
    """
    chooser = random.Random(9)
    for _ in range(3000):
        text = bytes(chooser.choice(b"<>ab") for _ in range(chooser.randint(0, 40)))
        finder = ForwardFinder(text)
        pos = 0
        for _ in range(chooser.randint(1, 20)):
            pos += chooser.randint(0, 5)
            assert finder.find(b"<", pos) == text.find(b"<", pos)
            assert finder.find(b">", pos) == text.find(b">", pos)
            assert finder.rfind(b"<", pos + 1) == text.rfind(b"<", 0, pos + 1)


PRE_BLOCK = r".*?(?<=<pre>)(?P<text>.*?)</pre>"
PRE_PIECES = ["<pre>", "<PRE>", "</pre>", "</Pre>", "<pr", "e>", "</", "x", "\n", "<", ">"]


@pytest.mark.parametrize("binary", [False, True])
def test_sub_until_failure_substitutes_as_sub_does(binary: bool) -> None:
    """
    Give what ``pattern.sub`` gives for the pre-block pattern of the heuristic processor.

    Example:
        >>> test_sub_until_failure_substitutes_as_sub_does(False)  # doctest: +SKIP


    :param binary: Whether the text is bytes.
    :return: None; the assertions are the check.
    """
    flags = re.IGNORECASE | re.DOTALL
    pattern: re.Pattern[Any] = re.compile(PRE_BLOCK.encode() if binary else PRE_BLOCK, flags)
    alphabet: list[Any] = [piece.encode() for piece in PRE_PIECES] if binary else PRE_PIECES
    template = rb"[\g<text>]" if binary else r"[\g<text>]"

    def tagged(match: re.Match[Any]) -> Any:
        """
        Replace a match with its text and where it starts, as the processor's function uses them.

        Example:
            See the test.


        :param match: The match.
        :return: The replacement.
        """
        text = b"[%s|%d]" if binary else "[%s|%d]"
        return text % (match.group("text"), match.start())

    chooser = random.Random(str(binary))
    for _ in range(3000):
        text = alphabet[0][:0].join(chooser.choice(alphabet) for _ in range(chooser.randint(0, 30)))
        assert sub_until_failure(pattern, tagged, text) == pattern.sub(tagged, text), text
        assert sub_until_failure(pattern, template, text) == pattern.sub(template, text), text


def test_sub_until_failure_is_quick_after_the_last_block() -> None:
    """
    Replace the blocks of a megabyte of text whose tail has none, where ``sub`` scanned the tail from every position.

    Example:
        >>> test_sub_until_failure_is_quick_after_the_last_block()  # doctest: +SKIP


    :return: None; the assertions are the check.
    """
    pattern = re.compile(PRE_BLOCK, re.IGNORECASE | re.DOTALL)
    text = "<pre>a</pre>" * 1_000 + "b" * 1_000_000
    started = time.monotonic()
    assert sub_until_failure(pattern, r"\g<text>", text) == "a" * 1_000 + "b" * 1_000_000
    assert time.monotonic() - started < 5


def test_sub_until_failure_refuses_empty_matches() -> None:
    """
    Refuse a pattern that matches nothing, for which the emulation would not hold.

    Example:
        >>> test_sub_until_failure_refuses_empty_matches()


    :return: None; the assertions are the check.
    """
    with pytest.raises(ValueError, match="not empty"):
        sub_until_failure(re.compile(r"x*"), "", "abc")
