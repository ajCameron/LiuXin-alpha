"""
Provide test fallback icu utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test fallback icu through a consuming regression::

        python -m pytest -q tests/utils/plugins/fallbacks/test_fallback_icu.py
"""
from __future__ import annotations

import pytest


def test_icu_change_case_requires_locale() -> None:
    """
    Perform the test icu change case requires locale utility operation under explicit compatibility rules.

    Example:
        Exercise test icu change case requires locale through a consuming regression::

            python -m pytest -q tests/utils/plugins/fallbacks/test_fallback_icu.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.utils.plugins.fallbacks import icu

    with pytest.raises(NotImplementedError):
        icu.change_case("abc", icu.UPPER_CASE)

    assert icu.change_case("abc", icu.UPPER_CASE, locale="en_US") == "ABC"
    assert icu.change_case("ABC", icu.LOWER_CASE, locale="en_US") == "abc"


def test_icu_normalize_and_character_naming() -> None:
    """
    Perform the test icu normalize and character naming utility operation under explicit compatibility rules.

    Example:
        Exercise test icu normalize and character naming through a consuming regression::

            python -m pytest -q tests/utils/plugins/fallbacks/test_fallback_icu.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.utils.plugins.fallbacks import icu

    assert icu.normalize("e\u0301", "NFC") == "é"
    assert icu.character_name("A")
    assert icu.character_name_from_code(ord("A"))


def test_icu_utf16_length() -> None:
    """
    Perform the test icu utf16 length utility operation under explicit compatibility rules.

    Example:
        Exercise test icu utf16 length through a consuming regression::

            python -m pytest -q tests/utils/plugins/fallbacks/test_fallback_icu.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.utils.plugins.fallbacks import icu

    # U+1F600 is a surrogate pair in UTF-16
    assert icu.utf16_length("\U0001F600") == 2


def test_icu_collator_sort_key_and_comparisons() -> None:
    """
    Perform the test icu collator sort key and comparisons utility operation under explicit compatibility rules.

    Example:
        Exercise test icu collator sort key and comparisons through a consuming regression::

            python -m pytest -q tests/utils/plugins/fallbacks/test_fallback_icu.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.utils.plugins.fallbacks import icu

    c = icu.Collator("C")
    assert isinstance(c.sort_key("abc"), (bytes, bytearray))
    assert c.strcmp("a", "b") < 0
    assert c.contains("hello", "ell") is True
    assert c.find("hello", "ell") == 1

    c2 = c.clone()
    assert isinstance(c2, icu.Collator)


def test_icu_break_iterator_split2_and_index() -> None:
    """
    Perform the test icu break iterator split2 and index utility operation under explicit compatibility rules.

    Example:
        Exercise test icu break iterator split2 and index through a consuming regression::

            python -m pytest -q tests/utils/plugins/fallbacks/test_fallback_icu.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.utils.plugins.fallbacks import icu

    bi = icu.BreakIterator(0, "C")
    bi.set_text("hello world foo-bar")
    spans = bi.split2()
    assert spans

    # Spans are (start, length)
    for st, ln in spans:
        assert st >= 0
        assert ln > 0

    assert bi.index("world") == 6
    assert bi.index("") == -1
