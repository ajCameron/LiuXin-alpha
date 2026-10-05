"""
Provide test markdown malformed hostile utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test markdown malformed hostile through a consuming regression::

        python -m pytest -q tests/file_formats/markdown/test_markdown_malformed_hostile.py
"""
from __future__ import annotations

import pytest

from tests.support.file_format_markup import (
    MARKDOWN_HOSTILE_CASES,
    assert_markup_renderer_deterministic,
    assert_markup_survives,
    repeated_delimiter_payload,
)
from tests.support.file_format_unicode import assert_no_replacement_chars


@pytest.mark.parametrize("case", MARKDOWN_HOSTILE_CASES, ids=lambda case: case.case_id)
def test_markdown_preserves_multilingual_text_around_malformed_markup(case) -> None:
    """
    Perform the test markdown preserves multilingual text around malformed markup operation under explicit file-format and conversion rules.

    Example:
        Exercise test markdown preserves multilingual text around malformed markup through a consuming regression::

            python -m pytest -q tests/file_formats/markdown/test_markdown_malformed_hostile.py


    :param case: Value supplied for case under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats import markdown

    rendered = assert_markup_renderer_deterministic(
        lambda source: markdown.markdown(source, extensions=["footnotes", "tables", "toc", "headerid"]),
        case.source,
        context=case.case_id,
    )

    assert_markup_survives(rendered, case.fragments, context=case.case_id)
    assert_no_replacement_chars(rendered, context=case.case_id)


def test_markdown_repeated_delimiters_are_deterministic_and_preserve_foreign_text() -> None:
    """
    Perform the test markdown repeated delimiters are deterministic and preserve foreign text operation under explicit file-format and conversion rules.

    Example:
        Exercise test markdown repeated delimiters are deterministic and preserve foreign text through a consuming regression::

            python -m pytest -q tests/file_formats/markdown/test_markdown_malformed_hostile.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats import markdown

    rendered = assert_markup_renderer_deterministic(
        lambda source: markdown.markdown(source, extensions=["tables"]),
        repeated_delimiter_payload(),
        context="markdown delimiter stress",
    )

    assert_markup_survives(rendered, context="markdown delimiter stress")
    assert_no_replacement_chars(rendered, context="markdown delimiter stress")


def test_markdown_safe_mode_escape_preserves_text_around_raw_html() -> None:
    """
    Perform the test markdown safe mode escape preserves text around raw html operation under explicit file-format and conversion rules.

    Example:
        Exercise test markdown safe mode escape preserves text around raw html through a consuming regression::

            python -m pytest -q tests/file_formats/markdown/test_markdown_malformed_hostile.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats import markdown

    source = "<script>Καλημέρα()</script>\n\nمرحبا שלום नमस्ते 你好 cafe\u0301"
    rendered = markdown.markdown(source, safe_mode="escape")

    assert "&lt;script&gt;" in rendered
    assert_markup_survives(rendered, context="markdown safe_mode escape")
    assert_no_replacement_chars(rendered, context="markdown safe_mode escape")
