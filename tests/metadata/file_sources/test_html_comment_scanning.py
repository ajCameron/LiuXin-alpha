"""
Read HTML metadata comments in linear time, finding what the pattern finds.

The comment pattern, ``(?P<name>\\S+)\\s*=\\s*"..."``, backtracks over the
rest of a run of characters without spaces from every character it is tried
at, so ``finditer`` over a long run is quadratic.
"""

from __future__ import annotations

import io
import random
import time

import pytest

from LiuXin_alpha.metadata.file_sources import html


@pytest.mark.parametrize("comment", ["a" * 200_000, "a=" * 100_000], ids=["run", "equals"])
def test_long_comments_are_read_quickly(comment: str) -> None:
    """
    Read a page whose comment runs on without spaces, and still find its fields.

    Before the fix, 20,000 such characters took seconds; these took hours.

    Example:
        >>> test_long_comments_are_read_quickly("a" * 10)  # doctest: +SKIP


    :param comment: The run of characters.
    :return: None; the assertions are the check.
    """
    page = f'<html><head><!-- {comment} TITLE="Found" --></head><body></body></html>'
    pairs_started = time.monotonic()
    pairs = html._extract_comment_pairs(f'{comment} TITLE="Found"')
    assert time.monotonic() - pairs_started < 2
    assert pairs["title"] == ["Found"]

    started = time.monotonic()
    html.get_metadata(io.BytesIO(page.encode("utf-8")))
    assert time.monotonic() - started < 5


def test_comment_matches_are_those_finditer_finds() -> None:
    """
    Find exactly the matches ``finditer`` finds, on random comments.

    Example:
        >>> test_comment_matches_are_those_finditer_finds()  # doctest: +SKIP


    :return: None; the assertions are the check.
    """
    chooser = random.Random(180)
    alphabet = ["a", "T", "=", "'", '"', " ", "\t", "\n", "\xa0", "　", "&"]
    for _ in range(20_000):
        text = "".join(chooser.choice(alphabet) for _ in range(chooser.randint(0, 40)))
        expected = [(m.span(), m.groupdict()) for m in html._COMMENT_PAIR_RE.finditer(text)]
        found = [(m.span(), m.groupdict()) for m in html._comment_pair_matches(text)]
        assert found == expected, text
