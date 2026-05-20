from __future__ import annotations

import importlib
import sys
import types

import pytest

from tests.support.file_format_markup import (
    MARKDOWN_HOSTILE_CASES,
    TEXTILE_HOSTILE_CASES,
    assert_markup_survives,
    repeated_delimiter_payload,
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


class _Log:
    def __init__(self) -> None:
        self.messages: list[str] = []

    def debug(self, message: str, *args) -> None:
        self.messages.append(message % args if args else message)

    def info(self, message: str, *args) -> None:
        self.messages.append(message % args if args else message)

    def warning(self, message: str, *args) -> None:
        self.messages.append(message % args if args else message)

    warn = warning


@pytest.fixture()
def html_input(monkeypatch) -> _CapturingHTMLInput:
    captured = _CapturingHTMLInput()
    fake_ui = types.ModuleType("LiuXin_alpha.customize.ui")
    fake_ui.plugin_for_input_format = lambda fmt: captured if fmt == "html" else None
    fake_ui.get_file_type_metadata = (
        lambda stream, file_ext, calibre=True: types.SimpleNamespace(title="Hostile Markup", authors=["Tester"])
    )
    fake_meta = types.ModuleType("LiuXin_alpha.file_formats.oeb.transforms.metadata")
    fake_meta.meta_info_to_oeb_metadata = lambda mi, metadata, log: None
    monkeypatch.setitem(sys.modules, "LiuXin_alpha.customize.ui", fake_ui)
    monkeypatch.setitem(sys.modules, "LiuXin_alpha.file_formats.oeb.transforms.metadata", fake_meta)
    return captured


def _txt_options(**overrides):
    values = {
        "input_encoding": "utf-8",
        "paragraph_type": "auto",
        "formatting_type": "auto",
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


@pytest.mark.parametrize(
    ("file_ext", "source", "expected_formatting"),
    (
        ("md", MARKDOWN_HOSTILE_CASES[0].source, "markdown"),
        ("markdown", MARKDOWN_HOSTILE_CASES[1].source, "markdown"),
        ("textile", TEXTILE_HOSTILE_CASES[0].source, "textile"),
    ),
)
def test_txt_input_markup_extensions_preserve_foreign_text_in_hostile_markup(
    html_input: _CapturingHTMLInput,
    file_ext: str,
    source: str,
    expected_formatting: str,
) -> None:
    txt_input_mod = importlib.import_module("LiuXin_alpha.file_formats.conversion.plugins.txt_input")
    payload = source.encode("utf-8") + b"\xff\xfe"
    options = _txt_options()
    log = _Log()

    txt_input_mod.TXTInput(None).convert(types.SimpleNamespace(read=lambda: payload, seek=lambda _pos: None), options, file_ext, log, {})

    decoded_html = html_input.last_html.decode("utf-8", "replace")
    assert_markup_survives(decoded_html, context=f"TXTInput {file_ext}")
    assert "\ufffd" in decoded_html
    assert options.paragraph_type == "off"
    assert options.formatting_type == expected_formatting


def test_txt_input_auto_detection_stays_deterministic_on_markup_delimiter_stress(
    html_input: _CapturingHTMLInput,
) -> None:
    txt_input_mod = importlib.import_module("LiuXin_alpha.file_formats.conversion.plugins.txt_input")
    payload = repeated_delimiter_payload().encode("utf-8")
    rendered_outputs: list[str] = []
    detected_formats: list[str] = []

    for _ in range(2):
        options = _txt_options(paragraph_type="auto", formatting_type="auto")
        txt_input_mod.TXTInput(None).convert(
            types.SimpleNamespace(read=lambda payload=payload: payload, seek=lambda _pos: None),
            options,
            "txt",
            _Log(),
            {},
        )
        decoded_html = html_input.last_html.decode("utf-8", "replace")
        rendered_outputs.append(decoded_html)
        detected_formats.append(options.formatting_type)
        assert_markup_survives(decoded_html, context="TXTInput delimiter stress")
        assert options.formatting_type in {"markdown", "textile", "heuristic"}

    assert rendered_outputs[0] == rendered_outputs[1]
    assert detected_formats[0] == detected_formats[1]
