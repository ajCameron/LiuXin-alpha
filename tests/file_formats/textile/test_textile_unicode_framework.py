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
    from LiuXin_alpha.file_formats.textile.functions import textile_restricted

    source = '<b>Unsafe</b>\n\n"参照":https://example.com/路径?鍵=值\n\n' + MULTISCRIPT_TEXT

    rendered = textile_restricted(source)

    assert "&#60;b&#62;Unsafe&#60;/b&#62;" in rendered
    assert 'rel="nofollow"' in rendered
    assert_fragments_present(rendered, COMMON_TEXT_FRAGMENTS, context="textile_restricted")
    assert_no_replacement_chars(rendered, context="textile_restricted")


def test_textile_head_offset_preserves_unicode_heading_text() -> None:
    from LiuXin_alpha.file_formats.textile.functions import textile

    rendered = textile("h1. Καλημέρα 世界 👩🏽‍💻", head_offset=2)

    assert "<h3>Καλημέρα 世界 👩🏽‍💻</h3>" in rendered
    assert_no_replacement_chars(rendered, context="textile head_offset")


def test_textile_is_stable_under_shared_unicode_fuzz() -> None:
    from LiuXin_alpha.file_formats.textile.functions import textile

    source = "h2. Fuzz\n\n" + deterministic_unicode_fuzz(seed=6804, length=600)

    rendered = assert_output_deterministic(textile, source, context="textile fuzz")

    assert rendered
    assert_no_replacement_chars(rendered, context="textile fuzz")
