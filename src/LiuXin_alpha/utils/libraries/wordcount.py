#!/usr/bin/env python

"""Word/character counting helpers used by conversion heuristics."""

from __future__ import annotations

IDEOGRAPHIC_SPACE = 0x3000


def is_asian(char: str) -> bool:
    """Return ``True`` when ``char`` is an ideographic/asian character."""

    return ord(char) > IDEOGRAPHIC_SPACE


def filter_jchars(char: str) -> str:
    """Map asian characters to spaces so non-asian words can be counted."""

    return " " if is_asian(char) else char


def nonj_len(text: str) -> int:
    """Count non-asian words in ``text``."""

    return len("".join(filter_jchars(c) for c in text).split())


def get_wordcount(text: str) -> dict[str, int]:
    """Return aggregate word/character counts for ``text``."""

    characters = len(text)
    chars_no_spaces = sum(1 for c in text if not c.isspace())
    asian_chars = sum(1 for c in text if is_asian(c))
    non_asian_words = nonj_len(text)
    words = non_asian_words + asian_chars
    return {
        "characters": characters,
        "chars_no_spaces": chars_no_spaces,
        "asian_chars": asian_chars,
        "non_asian_words": non_asian_words,
        "words": words,
    }


class _WordCount:
    def __init__(self, counts: dict[str, int]):
        self.__dict__.update(counts)


def get_wordcount_obj(text: str) -> _WordCount:
    """Return counts as attribute-access object used by legacy callers."""

    return _WordCount(get_wordcount(text))
