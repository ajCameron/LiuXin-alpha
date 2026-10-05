"""
Provide test markdown unicode framework utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test markdown unicode framework through a consuming regression::

        python -m pytest -q tests/file_formats/markdown/test_markdown_unicode_framework.py
"""
from __future__ import annotations

from pathlib import Path

import pytest

from tests.support.file_format_unicode import (
    COMMON_TEXT_FRAGMENTS,
    MULTISCRIPT_TEXT,
    assert_fragments_present,
    assert_no_replacement_chars,
    assert_output_deterministic,
    encoded_unicode_cases,
)


def test_markdown_preserves_shared_multiscript_corpus() -> None:
    """
    Perform the test markdown preserves shared multiscript corpus operation under explicit file-format and conversion rules.

    Example:
        Exercise test markdown preserves shared multiscript corpus through a consuming regression::

            python -m pytest -q tests/file_formats/markdown/test_markdown_unicode_framework.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats import markdown

    source = "# Shared Corpus\n\n" + MULTISCRIPT_TEXT

    rendered = assert_output_deterministic(
        lambda text: markdown.markdown(text, extensions=["toc", "headerid"]),
        source,
        context="markdown.markdown",
    )

    assert '<h1 id="shared-corpus">Shared Corpus</h1>' in rendered
    assert_fragments_present(rendered, COMMON_TEXT_FRAGMENTS, context="markdown.markdown")
    assert_no_replacement_chars(rendered, context="markdown.markdown")


@pytest.mark.parametrize(
    "payload",
    [
        MULTISCRIPT_TEXT.encode("utf-8"),
        bytearray(MULTISCRIPT_TEXT.encode("utf-8")),
        memoryview(MULTISCRIPT_TEXT.encode("utf-8")),
    ],
    ids=("bytes", "bytearray", "memoryview"),
)
def test_markdown_convert_preserves_shared_corpus_from_bytes_like_inputs(payload) -> None:
    """
    Perform the test markdown convert preserves shared corpus from bytes like inputs operation under explicit file-format and conversion rules.

    Example:
        Exercise test markdown convert preserves shared corpus from bytes like inputs through a consuming regression::

            python -m pytest -q tests/file_formats/markdown/test_markdown_unicode_framework.py


    :param payload: Value supplied for payload under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats.markdown import Markdown

    rendered = Markdown().convert(payload)

    assert_fragments_present(rendered, COMMON_TEXT_FRAGMENTS, context="Markdown.convert bytes-like")
    assert_no_replacement_chars(rendered, context="Markdown.convert bytes-like")


@pytest.mark.parametrize("case", encoded_unicode_cases("# Shared Corpus\n\n" + MULTISCRIPT_TEXT), ids=lambda case: case.case_id)
def test_markdown_from_file_handles_shared_encoded_unicode_cases(tmp_path: Path, case) -> None:
    """
    Perform the test markdown from file handles shared encoded unicode cases operation under explicit file-format and conversion rules.

    Example:
        Exercise test markdown from file handles shared encoded unicode cases through a consuming regression::

            python -m pytest -q tests/file_formats/markdown/test_markdown_unicode_framework.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :param case: Value supplied for case under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats.markdown import markdownFromFile

    source = tmp_path / f"{case.case_id}.md"
    output = tmp_path / f"{case.case_id}.html"
    source.write_bytes(case.payload)

    markdownFromFile(input=str(source), output=str(output), encoding=case.encoding)

    rendered = output.read_bytes().decode(case.encoding, "strict")
    assert "Shared Corpus" in rendered
    assert_fragments_present(rendered, case.fragments, context=case.case_id)
    assert_no_replacement_chars(rendered, context=case.case_id)
