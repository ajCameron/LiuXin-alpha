"""
Build deterministic MARKUP fixtures and test doubles.

The module keeps generated data, ordering and failure modes explicit so consumers
can assert stable behavior.

Example:
    Exercise file format markup through a consuming regression::

        python -m pytest -q tests/metadata/file_sources/test_html_metadata_source.py
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Sequence


HOSTILE_MARKUP_FRAGMENTS = (
    "Καλημέρα",
    "مرحبا",
    "שלום",
    "नमस्ते",
    "你好",
    "cafe\u0301",
)


@dataclass(frozen=True)
class HostileMarkupCase:
    """
    Carry the deterministic HostileMarkupCase inputs and expected values used by format tests.

    Example:
        Exercise HostileMarkupCase through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_html_metadata_source.py
    """
    case_id: str
    source: str
    fragments: tuple[str, ...] = HOSTILE_MARKUP_FRAGMENTS
    description: str = ""


MARKDOWN_HOSTILE_CASES: tuple[HostileMarkupCase, ...] = (
    HostileMarkupCase(
        "broken_links_images_and_tables",
        "\n".join(
            (
                "# Καλημέρα [broken heading",
                "",
                "Body مرحبا with **unterminated strong and _nested emphasis",
                "[שלום link](https://example.com/路径?x=[unterminated",
                "![你好 alt](../../../../evil.png \"unterminated title",
                "",
                "| key | value |",
                "| --- | --- |",
                "| नमस्ते | cafe\u0301 |",
                "",
                "<script>alert('not-sanitized-here')</script>",
            )
        ),
        description="malformed inline delimiters around a table, image, link, and raw HTML",
    ),
    HostileMarkupCase(
        "reference_and_footnote_edges",
        "\n".join(
            (
                "Καλημέρα [missing-ref][שלום",
                "",
                "[سلام]: https://example.com/مرحبا \"unterminated",
                "[^bad]: Footnote नमस्ते with [broken",
                "",
                "> 你好 quote with `unterminated code and cafe\u0301",
            )
        ),
        fragments=("Καλημέρα", "مرحبا", "नमस्ते", "你好", "cafe\u0301"),
        description="broken reference-style links, footnotes, quote, and inline code",
    ),
)


TEXTILE_HOSTILE_CASES: tuple[HostileMarkupCase, ...] = (
    HostileMarkupCase(
        "broken_links_images_and_tables",
        "\n".join(
            (
                "h1. Καλημέρα مرحبا",
                "",
                '"שלום link":https://example.com/路径?x=[unterminated',
                "!../../../../evil.png(你好!",
                "",
                "|_. key |_. value |",
                "| नमस्ते | cafe\u0301 |",
                "",
                "* item with _unterminated emphasis and <script>alert(1)</script>",
            )
        ),
        description="malformed Textile links, image syntax, table, emphasis, and raw HTML",
    ),
    HostileMarkupCase(
        "notextile_and_reference_edges",
        "\n".join(
            (
                "[שלום]https://example.com/مرحبا?x=[bad",
                "",
                "Visible שלום before reference use",
                "",
                '"Καλημέρα":שלום',
                "",
                "<notextile>*नमस्ते* [你好](broken cafe\u0301</notextile>",
            )
        ),
        fragments=("Καλημέρα", "مرحبا", "שלום", "नमस्ते", "你好", "cafe\u0301"),
        description="reference-style links and no-textile regions with malformed markup",
    ),
)


def repeated_delimiter_payload(*, seed_text: str = "Καλημέρα مرحبا שלום नमस्ते 你好 cafe\u0301") -> str:
    """
    Perform the repeated delimiter payload step with deterministic fixture inputs.

    Example:
        Exercise repeated delimiter payload through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_html_metadata_source.py


    :param seed_text: Value supplied for seed text under the deterministic fixture
        contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    delimiter_runs = (
        "*_" * 120,
        "[]()" * 90,
        "![" * 80,
        "| " * 120,
        "`" * 160,
    )
    return "\n\n".join((*delimiter_runs, seed_text, *reversed(delimiter_runs)))


def assert_markup_survives(
    rendered: str,
    fragments: Sequence[str] = HOSTILE_MARKUP_FRAGMENTS,
    *,
    context: str = "",
) -> None:
    """
    Assert markup survives under the fixture contract.

    Example:
        Exercise assert markup survives through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_html_metadata_source.py


    :param rendered: Value supplied for rendered under the deterministic fixture
        contract.
    :param fragments: Value supplied for fragments under the deterministic fixture
        contract.
    :param context: Value supplied for context under the deterministic fixture contract.
    :return: None; fixture state or the supplied destination is updated in place.
    """
    missing = [fragment for fragment in fragments if fragment not in rendered]
    if missing:
        detail = f" for {context}" if context else ""
        raise AssertionError(f"missing hostile-markup fragments{detail}: {missing!r}")
    if "\x00" in rendered:
        detail = f" for {context}" if context else ""
        raise AssertionError(f"unexpected NUL in rendered markup{detail}")


def assert_markup_renderer_deterministic(
    renderer: Callable[[str], str],
    source: str,
    *,
    context: str = "",
) -> str:
    """
    Assert markup renderer deterministic under the fixture contract.

    Example:
        Exercise assert markup renderer deterministic through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_html_metadata_source.py


    :param renderer: Value supplied for renderer under the deterministic fixture
        contract.
    :param source: Value supplied for source under the deterministic fixture contract.
    :param context: Value supplied for context under the deterministic fixture contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    first = renderer(source)
    second = renderer(source)
    if first != second:
        detail = f" for {context}" if context else ""
        raise AssertionError(f"markup renderer output changed between runs{detail}")
    return first
