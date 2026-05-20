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
    from LiuXin_alpha.file_formats.markdown import Markdown

    rendered = Markdown().convert(payload)

    assert_fragments_present(rendered, COMMON_TEXT_FRAGMENTS, context="Markdown.convert bytes-like")
    assert_no_replacement_chars(rendered, context="Markdown.convert bytes-like")


@pytest.mark.parametrize("case", encoded_unicode_cases("# Shared Corpus\n\n" + MULTISCRIPT_TEXT), ids=lambda case: case.case_id)
def test_markdown_from_file_handles_shared_encoded_unicode_cases(tmp_path: Path, case) -> None:
    from LiuXin_alpha.file_formats.markdown import markdownFromFile

    source = tmp_path / f"{case.case_id}.md"
    output = tmp_path / f"{case.case_id}.html"
    source.write_bytes(case.payload)

    markdownFromFile(input=str(source), output=str(output), encoding=case.encoding)

    rendered = output.read_bytes().decode(case.encoding, "strict")
    assert "Shared Corpus" in rendered
    assert_fragments_present(rendered, case.fragments, context=case.case_id)
    assert_no_replacement_chars(rendered, context=case.case_id)
