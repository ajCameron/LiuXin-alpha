"""
Check how keywords are split into tags: by default against the library's known tags, else as before, or as calibre does.

Example:
    Run the checks::

        python -m pytest -q tests/metadata/file_sources/test_keyword_splitting.py
"""

from __future__ import annotations

import io
import random
import re
from collections import ChainMap
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import pytest

from LiuXin_alpha.configuration import current_preferences, use_preferences
from LiuXin_alpha.file_formats.plugin_runtime import get_file_type_metadata
from LiuXin_alpha.metadata.file_sources.keywords import (
    KEYWORD_SPLITTING_PREFERENCE,
    KnownTags,
    known_tags,
    split_keywords,
    using_known_tags,
)
from tests.metadata.file_sources.test_metadata_writer_interchange import _docx, _values

KNOWN = ["space opera", "military sci-fi", "history", "political", "Science Fiction", "detective", "sci-fi"]


@contextmanager
def splitting(how: str) -> Iterator[None]:
    """
    Set the ``keyword_splitting`` preference for a block, keeping every other preference.

    Example:
        >>> with splitting("calibre"):
        ...     split_keywords("a b,c")
        ['a', 'b', 'c']


    :param how: ``smart``, ``separators`` or ``calibre``.
    :return: An iterator that yields once, inside the block.
    """
    with use_preferences(ChainMap({KEYWORD_SPLITTING_PREFERENCE: how}, current_preferences())):
        yield


def separators_rule(text: str) -> list[str]:
    """
    LiuXin's earlier rule, as the DOCX reader had it: split at commas and semicolons if any, else on spaces.

    Example:
        >>> separators_rule("a b"), separators_rule("a b, c")
        (['a', 'b'], ['a b', 'c'])


    :param text: The keywords.
    :return: The tags.
    """
    parts = re.split(r"[,;]", text) if re.search(r"[,;]", text) else text.split()
    return [part.strip() for part in parts if part.strip()]


def calibre_rule(text: str) -> list[str]:
    """
    calibre's ``read_doc_props``: split on whitespace, then on commas.

    Example:
        >>> calibre_rule("a b,c")
        ['a', 'b', 'c']


    :param text: The keywords.
    :return: The tags.
    """
    tags: list[str] = []
    for word in text.split():
        tags.extend(part.strip() for part in word.split(",") if part.strip())
    return tags


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("spaceopera,military scifi history political", ["space opera", "military sci-fi", "history", "political"]),
        ("science fiction space opera", ["Science Fiction", "space opera"]),
        ("SPACE-OPERA; sci fi", ["space opera", "sci-fi"]),
        ("hard boiled detective, history", ["hard boiled detective", "history"]),
        ("history dragons, space opera wizards", ["history dragons", "space opera", "wizards"]),
        ("dragons wizards", ["dragons", "wizards"]),
        ("Sci-Fi, scifi, SCI FI", ["sci-fi"]),
        ("science fiction, Science Fiction", ["Science Fiction"]),
        ("", []),
    ],
    ids=[
        "the-example",
        "no-separators",
        "spelling-from-the-library",
        "mostly-unknown-piece-kept",
        "half-known-piece-kept",
        "unknown-words-split-on-spaces",
        "repeats-dropped",
        "same-tag-twice",
        "empty",
    ],
)
def test_smart_splitting_uses_the_known_tags(text: str, expected: list[str]) -> None:
    """
    Cut keywords into the library's known tags, in their spelling, keeping unknown pieces as written.

    Example:
        >>> test_smart_splitting_uses_the_known_tags("history", ["history"])


    :param text: The keywords.
    :param expected: The tags.
    :return: None; the assertions are the check.
    """
    with using_known_tags(KNOWN):
        assert split_keywords(text, "smart") == expected


def test_without_known_tags_smart_is_the_separators_rule() -> None:
    """
    Split as LiuXin did before when no known tags are given, on random keyword strings.

    Example:
        >>> test_without_known_tags_smart_is_the_separators_rule()


    :return: None; the assertions are the check.
    """
    rng = random.Random(21)
    for _ in range(500):
        text = "".join(rng.choice("ab c,;  -_") for _ in range(rng.randint(0, 30)))
        assert split_keywords(text, "smart") == separators_rule(text), text
        assert split_keywords(text, "separators") == separators_rule(text), text
        with using_known_tags(KNOWN):
            assert split_keywords(text, "separators") == separators_rule(text), text


def test_the_calibre_splitting_is_calibre_s_whatever_the_known_tags() -> None:
    """
    Split on spaces and commas, as calibre does, even where known tags would match.

    Example:
        >>> test_the_calibre_splitting_is_calibre_s_whatever_the_known_tags()


    :return: None; the assertions are the check.
    """
    rng = random.Random(1904)
    with using_known_tags(KNOWN):
        for _ in range(500):
            text = "".join(rng.choice("ab c,;  -_") for _ in range(rng.randint(0, 30)))
            assert split_keywords(text, "calibre") == calibre_rule(text), text
        assert split_keywords("science fiction, space opera", "calibre") == ["science", "fiction", "space", "opera"]


def test_the_preference_chooses_the_splitting() -> None:
    """
    Split as the ``keyword_splitting`` preference says when the caller names no splitting.

    Example:
        >>> test_the_preference_chooses_the_splitting()


    :return: None; the assertions are the check.
    """
    text = "space opera, history"
    with using_known_tags(KNOWN):
        assert split_keywords(text) == ["space opera", "history"]
        with splitting("calibre"):
            assert split_keywords(text) == ["space", "opera", "history"]
        with splitting("separators"):
            assert split_keywords("space opera") == ["space", "opera"]


def test_known_tags_are_asked_for_once_and_only_when_needed() -> None:
    """
    Call a known-tags source at most once in a scope, and not at all for files without keywords.

    Example:
        >>> test_known_tags_are_asked_for_once_and_only_when_needed()


    :return: None; the assertions are the check.
    """
    calls: list[int] = []

    def library_tags() -> list[str]:
        """
        Stand in for a query of the library's tags, counting calls.

        Example:
            See the test.


        :return: The tags.
        """
        calls.append(1)
        return KNOWN

    with using_known_tags(library_tags):
        assert calls == []
        split_keywords("space opera history")
        split_keywords("dragons")
        assert calls == [1]
    assert not known_tags()


def test_scopes_nest_and_are_restored_after_an_error() -> None:
    """
    Use the innermost scope's tags, and restore the outer ones afterwards, even after an error.

    Example:
        >>> test_scopes_nest_and_are_restored_after_an_error()


    :return: None; the assertions are the check.
    """
    with using_known_tags(["history"]):
        with pytest.raises(RuntimeError), using_known_tags(["dragons"]):
            assert known_tags().spelling("Dragons") == "dragons"
            assert known_tags().spelling("history") is None
            raise RuntimeError("stop")
        assert known_tags().spelling("HISTORY") == "history"
    assert not known_tags()


def test_the_first_spelling_of_a_tag_wins() -> None:
    """
    Keep the first of several tags that match the same words, so a library can list its most used first.

    Example:
        >>> test_the_first_spelling_of_a_tag_wins()


    :return: None; the assertions are the check.
    """
    assert split_keywords("scifi", "smart", known=KnownTags(["SF", "Sci-Fi", "sci fi"])) == ["Sci-Fi"]


@pytest.mark.parametrize(
    ("how", "keywords", "tags"),
    [
        ("smart", "spaceopera,military scifi history political", ["space opera", "military sci-fi", "history", "political"]),
        ("smart", "science_-_fiction", ["Science Fiction"]),
        ("separators", "spaceopera,military scifi history political", ["spaceopera", "military scifi history political"]),
        ("separators", "science_-_fiction", ["science fiction"]),
        ("calibre", "spaceopera,military scifi history political", ["spaceopera", "military", "scifi", "history", "political"]),
        ("calibre", "science_-_fiction", ["science_-_fiction"]),
    ],
)
def test_docx_keywords_are_split_as_the_preference_says(tmp_path: Path, how: str, keywords: str, tags: list[str]) -> None:
    """
    Read a DOCX's keywords through LiuXin's reader, split as the preference says, against the known tags.

    ``_-_`` is how LiuXin once wrote a space inside a tag; calibre's splitting keeps it.

    Example:
        >>> test_docx_keywords_are_split_as_the_preference_says(Path("/tmp"), "smart", "a", ["a"])  # doctest: +SKIP


    :param tmp_path: A folder to build in.
    :param how: The preference's value.
    :param keywords: The ``cp:keywords`` text.
    :param tags: The tags LiuXin's reader should find.
    :return: None; the assertions are the check.
    """
    book = _docx(tmp_path, keywords)
    with splitting(how), using_known_tags(KNOWN):
        md = get_file_type_metadata(io.BytesIO(book), "docx")
    assert _values(md.tags) == tags
