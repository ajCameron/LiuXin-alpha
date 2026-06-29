from __future__ import annotations

import pytest

from tests.support.file_format_markup import (
    TEXTILE_HOSTILE_CASES,
    assert_markup_renderer_deterministic,
    assert_markup_survives,
    repeated_delimiter_payload,
)
from tests.support.file_format_unicode import assert_no_replacement_chars


@pytest.mark.parametrize("case", TEXTILE_HOSTILE_CASES, ids=lambda case: case.case_id)
def test_textile_preserves_multilingual_text_around_malformed_markup(case) -> None:
    from LiuXin_alpha.file_formats.textile.functions import textile

    rendered = assert_markup_renderer_deterministic(textile, case.source, context=case.case_id)

    assert_markup_survives(rendered, case.fragments, context=case.case_id)
    assert_no_replacement_chars(rendered, context=case.case_id)


def test_textile_restricted_escapes_raw_html_without_losing_foreign_text() -> None:
    from LiuXin_alpha.file_formats.textile.functions import textile_restricted

    source = '<script>Καλημέρα()</script> "مرحبا":https://example.com/שלום नमस्ते 你好 cafe\u0301'
    rendered = textile_restricted(source)

    assert "&#60;script&#62;" in rendered
    assert 'rel="nofollow"' in rendered
    assert_markup_survives(rendered, context="textile restricted raw html")
    assert_no_replacement_chars(rendered, context="textile restricted raw html")


def test_textile_repeated_delimiters_are_deterministic_and_preserve_foreign_text() -> None:
    from LiuXin_alpha.file_formats.textile.functions import textile

    rendered = assert_markup_renderer_deterministic(
        textile,
        repeated_delimiter_payload(),
        context="textile delimiter stress",
    )

    assert_markup_survives(rendered, context="textile delimiter stress")
    assert_no_replacement_chars(rendered, context="textile delimiter stress")
