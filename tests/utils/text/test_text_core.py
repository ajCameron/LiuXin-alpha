from __future__ import annotations

import pytest


def test_isbytestring() -> None:
    from LiuXin_alpha.utils.text import isbytestring

    assert isbytestring("x")
    assert isbytestring(b"x")
    assert not isbytestring(123)


def test_url_slash_cleaner_keeps_scheme_and_cleans_extras() -> None:
    from LiuXin_alpha.utils.text import url_slash_cleaner

    assert url_slash_cleaner("http://example.com//a///b") == "http://example.com/a/b"
    assert url_slash_cleaner("https://x///") == "https://x/"


@pytest.mark.parametrize(
    "size, expected",
    [
        (0, "0 B"),
        (1, "1 B"),
        (1024, "1 KB"),
        (1536, "1.5 KB"),
        (1024 * 1024, "1 MB"),
    ],
)
def test_human_readable(size: int, expected: str) -> None:
    from LiuXin_alpha.utils.text import human_readable

    assert human_readable(size) == expected


def test_remove_bracketed_text_nested_and_custom_pairs() -> None:
    from LiuXin_alpha.utils.text import remove_bracketed_text

    s = "a (b [c] d) e"
    assert remove_bracketed_text(s) == "a  e"

    s2 = "keep <drop> keep"
    assert remove_bracketed_text(s2, brackets={"<": ">"}) == "keep  keep"