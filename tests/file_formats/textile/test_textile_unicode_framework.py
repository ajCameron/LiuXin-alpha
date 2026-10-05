"""
Provide test textile unicode framework utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test textile unicode framework through a consuming regression::

        python -m pytest -q tests/file_formats/textile/test_textile_unicode_framework.py
"""
from __future__ import annotations

from tests.support.file_format_unicode import (
    COMMON_TEXT_FRAGMENTS,
    MULTISCRIPT_TEXT,
    assert_fragments_present,
    assert_no_replacement_chars,
    assert_output_deterministic,
    deterministic_unicode_fuzz,
)


def test_textile_preserves_shared_multiscript_corpus() -> None:
    """
    Perform the test textile preserves shared multiscript corpus operation under explicit file-format and conversion rules.

    Example:
        Exercise test textile preserves shared multiscript corpus through a consuming regression::

            python -m pytest -q tests/file_formats/textile/test_textile_unicode_framework.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats.textile.functions import textile

    source = "h1. Shared Corpus\n\n" + MULTISCRIPT_TEXT

    rendered = assert_output_deterministic(
        textile,
        source,
        context="textile",
    )

    assert "<h1>Shared Corpus</h1>" in rendered
    assert_fragments_present(rendered, COMMON_TEXT_FRAGMENTS, context="textile")
    assert_no_replacement_chars(rendered, context="textile")


def test_textile_restricted_preserves_shared_corpus_while_escaping_html() -> None:
    """
    Perform the test textile restricted preserves shared corpus while escaping html operation under explicit file-format and conversion rules.

    Example:
        Exercise test textile restricted preserves shared corpus while escaping html through a consuming regression::

            python -m pytest -q tests/file_formats/textile/test_textile_unicode_framework.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats.textile.functions import textile_restricted

    source = '<b>Unsafe</b>\n\n"参照":https://example.com/路径?鍵=值\n\n' + MULTISCRIPT_TEXT

    rendered = textile_restricted(source)

    assert "&#60;b&#62;Unsafe&#60;/b&#62;" in rendered
    assert 'rel="nofollow"' in rendered
    assert_fragments_present(rendered, COMMON_TEXT_FRAGMENTS, context="textile_restricted")
    assert_no_replacement_chars(rendered, context="textile_restricted")


def test_textile_head_offset_preserves_unicode_heading_text() -> None:
    """
    Perform the test textile head offset preserves unicode heading text operation under explicit file-format and conversion rules.

    Example:
        Exercise test textile head offset preserves unicode heading text through a consuming regression::

            python -m pytest -q tests/file_formats/textile/test_textile_unicode_framework.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats.textile.functions import textile

    rendered = textile("h1. Καλημέρα 世界 👩🏽‍💻", head_offset=2)

    assert "<h3>Καλημέρα 世界 👩🏽‍💻</h3>" in rendered
    assert_no_replacement_chars(rendered, context="textile head_offset")


def test_textile_is_stable_under_shared_unicode_fuzz() -> None:
    """
    Perform the test textile is stable under shared unicode fuzz operation under explicit file-format and conversion rules.

    Example:
        Exercise test textile is stable under shared unicode fuzz through a consuming regression::

            python -m pytest -q tests/file_formats/textile/test_textile_unicode_framework.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats.textile.functions import textile

    source = "h2. Fuzz\n\n" + deterministic_unicode_fuzz(seed=6804, length=600)

    rendered = assert_output_deterministic(textile, source, context="textile fuzz")

    assert rendered
    assert_no_replacement_chars(rendered, context="textile fuzz")
