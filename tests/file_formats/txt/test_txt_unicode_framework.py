from __future__ import annotations

import importlib
import io
import sys
import types
from pathlib import Path

import pytest

from tests.support.deterministic_conversion import assert_bytes_deterministic
from tests.support.file_format_conversion import (
    TEXT_OUTPUT_MATRIX_CASES,
    assert_text_output_matrix_case,
)
from tests.support.file_format_unicode import (
    COMMON_TEXT_FRAGMENTS,
    MULTISCRIPT_TEXT,
    assert_fragments_present,
    assert_no_replacement_chars,
    assert_output_deterministic,
    deterministic_unicode_fuzz,
    encoded_unicode_cases,
)


class _Opt:
    def __init__(self, name: str, val: object) -> None:
        self.option = types.SimpleNamespace(name=name)
        self.recommended_value = val


class _CapturingHTMLInput:
    options = (_Opt("breadth_first", False), _Opt("dont_package", False))

    def __init__(self) -> None:
        self.last_html = b""

    def convert(self, stream, options, file_ext, log, accelerators):
        self.last_html = stream.read()
        return types.SimpleNamespace(metadata=types.SimpleNamespace())


@pytest.fixture()
def html_input(monkeypatch) -> _CapturingHTMLInput:
    captured = _CapturingHTMLInput()
    fake_ui = types.ModuleType("LiuXin_alpha.customize.ui")
    fake_ui.plugin_for_input_format = lambda fmt: captured if fmt == "html" else None
    fake_ui.get_file_type_metadata = (
        lambda stream, file_ext, calibre=True: types.SimpleNamespace(title="Unicode", authors=["Tester"])
    )
    fake_meta = types.ModuleType("LiuXin_alpha.file_formats.oeb.transforms.metadata")
    fake_meta.meta_info_to_oeb_metadata = lambda mi, metadata, log: None
    monkeypatch.setitem(sys.modules, "LiuXin_alpha.customize.ui", fake_ui)
    monkeypatch.setitem(sys.modules, "LiuXin_alpha.file_formats.oeb.transforms.metadata", fake_meta)
    return captured


def _txt_options(**overrides):
    values = {
        "input_encoding": "utf-8",
        "paragraph_type": "block",
        "formatting_type": "plain",
        "preserve_spaces": False,
        "txt_in_remove_indents": False,
        "markdown_extensions": "footnotes, tables, toc",
        "debug_pipeline": None,
        "verbose": 0,
        "enable_heuristics": False,
        "dehyphenate": False,
        "flow_size": 0,
    }
    values.update(overrides)
    return types.SimpleNamespace(**values)


def _txt_output_options(**overrides):
    values = {
        "txt_output_formatting": "plain",
        "newline": "unix",
        "txt_output_encoding": "utf-8",
        "remove_paragraph_spacing": False,
        "max_line_length": 0,
        "force_max_line_length": False,
        "inline_toc": False,
    }
    values.update(overrides)
    return types.SimpleNamespace(**values)


def _null_log():
    return types.SimpleNamespace(debug=lambda *a, **k: None, info=lambda *a, **k: None)


@pytest.fixture()
def fake_txtmlizer(monkeypatch) -> None:
    fake_txtml_mod = types.ModuleType("LiuXin_alpha.file_formats.txt.txtml")

    class _TXTMLizer:
        def __init__(self, _log):
            pass

        def extract_content(self, _oeb, _opts):
            return MULTISCRIPT_TEXT

    fake_txtml_mod.TXTMLizer = _TXTMLizer
    monkeypatch.setitem(sys.modules, "LiuXin_alpha.file_formats.txt.txtml", fake_txtml_mod)


def test_convert_basic_preserves_shared_multiscript_corpus() -> None:
    processor = importlib.import_module("LiuXin_alpha.file_formats.txt.processor")

    rendered = assert_output_deterministic(
        lambda source: processor.convert_basic(source, title="Ω 世界", epub_split_size_kb=8),
        MULTISCRIPT_TEXT,
        context="txt.convert_basic",
    )

    assert "<title>Ω 世界 " in rendered
    assert_fragments_present(rendered, COMMON_TEXT_FRAGMENTS, context="txt.convert_basic")
    assert_no_replacement_chars(rendered, context="txt.convert_basic")


def test_convert_markdown_and_textile_preserve_shared_multiscript_fragments() -> None:
    processor = importlib.import_module("LiuXin_alpha.file_formats.txt.processor")

    markdown_html = processor.convert_markdown(
        "# 多言語\n\n" + MULTISCRIPT_TEXT,
        title="Markdown",
        extensions=("footnotes", "tables", "toc"),
    )
    textile_html = processor.convert_textile(
        "h1. 多言語\n\n" + MULTISCRIPT_TEXT,
        title="Textile",
    )

    assert "<title>Markdown " in markdown_html
    assert "<title>Textile " in textile_html
    assert_fragments_present(markdown_html, COMMON_TEXT_FRAGMENTS, context="txt.convert_markdown")
    assert_fragments_present(textile_html, COMMON_TEXT_FRAGMENTS, context="txt.convert_textile")
    assert_no_replacement_chars(markdown_html, context="txt.convert_markdown")
    assert_no_replacement_chars(textile_html, context="txt.convert_textile")


def test_clean_txt_handles_shared_corpus_bytes_and_invalid_sequences() -> None:
    processor = importlib.import_module("LiuXin_alpha.file_formats.txt.processor")

    cleaned = processor.clean_txt(MULTISCRIPT_TEXT.encode("utf-8") + b"\xff\xfe")

    assert_fragments_present(cleaned, COMMON_TEXT_FRAGMENTS, context="txt.clean_txt")
    assert "\ufffd" in cleaned


def test_detect_formatting_type_is_stable_under_shared_unicode_fuzz() -> None:
    processor = importlib.import_module("LiuXin_alpha.file_formats.txt.processor")
    fuzz = deterministic_unicode_fuzz(seed=6803, length=1200)

    detected = assert_output_deterministic(
        processor.detect_formatting_type,
        fuzz,
        context="txt.detect_formatting_type",
    )

    assert detected in {"markdown", "textile", "heuristic"}


@pytest.mark.parametrize("case", encoded_unicode_cases(), ids=lambda case: case.case_id)
def test_txt_input_decodes_shared_encoded_unicode_cases(tmp_path: Path, html_input: _CapturingHTMLInput, case) -> None:
    txt_input_mod = importlib.import_module("LiuXin_alpha.file_formats.conversion.plugins.txt_input")
    source = tmp_path / f"{case.case_id}.txt"
    source.write_bytes(case.payload)
    options = _txt_options(input_encoding=case.encoding)

    with source.open("rb") as stream:
        txt_input_mod.TXTInput(None).convert(stream, options, "txt", _null_log(), {})

    decoded_html = html_input.last_html.decode("utf-8", "replace")
    assert_fragments_present(decoded_html, case.fragments, context=case.case_id)
    assert_no_replacement_chars(decoded_html, context=case.case_id)


@pytest.mark.parametrize(
    ("file_ext", "source_text", "expected_formatting", "expected_fragment"),
    (
        ("md", "# 多言語\n\nBody café 世界 👩🏽‍💻", "markdown", "<h1"),
        ("markdown", "# 多言語\n\nBody café 世界 👩🏽‍💻", "markdown", "<h1"),
        ("textile", "h1. 多言語\n\nBody café 世界 👩🏽‍💻", "textile", "<h1"),
    ),
)
def test_txt_input_extensions_force_formatting_without_losing_unicode(
    tmp_path: Path,
    html_input: _CapturingHTMLInput,
    file_ext: str,
    source_text: str,
    expected_formatting: str,
    expected_fragment: str,
) -> None:
    txt_input_mod = importlib.import_module("LiuXin_alpha.file_formats.conversion.plugins.txt_input")
    source = tmp_path / f"input.{file_ext}"
    source.write_text(source_text, encoding="utf-8")
    options = _txt_options(paragraph_type="auto", formatting_type="auto")

    with source.open("rb") as stream:
        txt_input_mod.TXTInput(None).convert(stream, options, file_ext, _null_log(), {})

    decoded_html = html_input.last_html.decode("utf-8", "replace")
    assert options.formatting_type == expected_formatting
    assert options.paragraph_type == "off"
    assert expected_fragment in decoded_html
    assert "café" in decoded_html
    assert "世界" in decoded_html
    assert "👩🏽‍💻" in decoded_html


@pytest.mark.parametrize("case", TEXT_OUTPUT_MATRIX_CASES, ids=lambda case: case.case_id)
def test_txt_output_matrix_preserves_unicode_across_encodings_and_newlines(fake_txtmlizer, case) -> None:
    txt_output_mod = importlib.import_module("LiuXin_alpha.file_formats.conversion.plugins.txt_output")

    def render_once(_run_name: str) -> bytes:
        out = io.BytesIO()
        options = _txt_output_options(
            newline=case.newline_option,
            txt_output_encoding=case.encoding,
        )
        txt_output_mod.TXTOutput(None).convert(object(), out, None, options, _null_log())
        return out.getvalue()

    payload = assert_bytes_deterministic(render_once)

    assert_text_output_matrix_case(payload, case, COMMON_TEXT_FRAGMENTS)
