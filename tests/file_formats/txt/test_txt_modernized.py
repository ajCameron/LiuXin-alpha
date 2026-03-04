from __future__ import annotations

import importlib
import io
import sys
import types
import zipfile
from pathlib import Path

import pytest


class _Log:
    def __init__(self) -> None:
        self.messages: list[tuple[str, str]] = []

    def _record(self, level: str, *parts) -> None:
        self.messages.append((level, " ".join(str(x) for x in parts)))

    def __call__(self, *parts) -> None:
        self._record("call", *parts)

    def debug(self, *parts) -> None:
        self._record("debug", *parts)

    def info(self, *parts) -> None:
        self._record("info", *parts)

    def warn(self, *parts) -> None:
        self._record("warn", *parts)

    def warning(self, *parts) -> None:
        self._record("warning", *parts)

    def error(self, *parts) -> None:
        self._record("error", *parts)


def test_txt_modules_import_smoke() -> None:
    modules = (
        "LiuXin_alpha.file_formats.txt",
        "LiuXin_alpha.file_formats.txt.processor",
        "LiuXin_alpha.file_formats.txt.txtml",
        "LiuXin_alpha.file_formats.txt.markdownml",
        "LiuXin_alpha.file_formats.txt.textileml",
        "LiuXin_alpha.file_formats.conversion.plugins.txt_input",
        "LiuXin_alpha.file_formats.conversion.plugins.txt_output",
    )
    for module_name in modules:
        importlib.import_module(module_name)


def test_txt_processor_clean_and_detect_basics() -> None:
    mod = importlib.import_module("LiuXin_alpha.file_formats.txt.processor")
    cleaned = mod.clean_txt(b"  alpha\r\n\r\n\x01beta")
    assert isinstance(cleaned, str)
    assert "\r" not in cleaned
    assert "\x01" not in cleaned

    assert mod.detect_paragraph_type("") == "single"
    assert mod.detect_formatting_type("plain text only\nno markup") == "heuristic"


def test_txt_processor_split_and_format_detection() -> None:
    mod = importlib.import_module("LiuXin_alpha.file_formats.txt.processor")
    src = ("Sentence one. " * 3000).strip()
    out = mod.split_txt(src, epub_split_size_kb=2)
    assert isinstance(out, str)
    assert len(out) > 0

    markdownish = "\n".join(
        [
            "# H1",
            "## H2",
            "### H3",
            "![img](a.png)",
            "[x](https://example.com)",
            "----",
            "====",
        ]
    )
    textileish = "\n".join(
        [
            "h1. One",
            "h2. Two",
            "h3. Three",
            '\"link\":https://example.com',
            "bq. quote",
            "p. para",
            "!img.png!",
        ]
    )
    assert mod.detect_formatting_type(markdownish) == "markdown"
    assert mod.detect_formatting_type(textileish) == "textile"


def test_txt_input_convert_plain_and_txtz_smoke(tmp_path: Path, monkeypatch) -> None:
    txt_input_mod = importlib.import_module("LiuXin_alpha.file_formats.conversion.plugins.txt_input")

    class _Opt:
        def __init__(self, name, val):
            self.option = types.SimpleNamespace(name=name)
            self.recommended_value = val

    class _HTMLInput:
        options = (_Opt("breadth_first", False), _Opt("dont_package", False))

        def __init__(self):
            self.last_html = b""

        def convert(self, stream, options, file_ext, log, accelerators):
            self.last_html = stream.read()
            return types.SimpleNamespace(metadata=types.SimpleNamespace())

    html_input = _HTMLInput()
    fake_ui = types.ModuleType("LiuXin_alpha.customize.ui")
    fake_ui.plugin_for_input_format = lambda fmt: html_input if fmt == "html" else None
    fake_ui.get_file_type_metadata = (
        lambda stream, file_ext, calibre=True: types.SimpleNamespace(title="TXT Smoke", authors=["A"])
    )
    fake_meta = types.ModuleType("LiuXin_alpha.file_formats.oeb.transforms.metadata")
    fake_meta.meta_info_to_oeb_metadata = lambda mi, metadata, log: None

    monkeypatch.setitem(sys.modules, "LiuXin_alpha.customize.ui", fake_ui)
    monkeypatch.setitem(sys.modules, "LiuXin_alpha.file_formats.oeb.transforms.metadata", fake_meta)

    plugin = txt_input_mod.TXTInput(None)
    options = types.SimpleNamespace(
        input_encoding="utf-8",
        paragraph_type="block",
        formatting_type="plain",
        preserve_spaces=False,
        txt_in_remove_indents=False,
        markdown_extensions="footnotes, tables, toc",
        debug_pipeline=None,
        verbose=0,
        enable_heuristics=False,
        dehyphenate=False,
        flow_size=0,
    )

    plain = tmp_path / "book.txt"
    plain.write_bytes("Hello Ω 世界".encode("utf-8"))
    with plain.open("rb") as stream:
        out = plugin.convert(stream, options, "txt", _Log(), {})
    assert out is not None
    assert b"Hello \xce\xa9" in html_input.last_html

    txtz = tmp_path / "book.txtz"
    with zipfile.ZipFile(txtz, "w") as zf:
        zf.writestr("x.txt", "Line A\n\nLine B")
    with txtz.open("rb") as stream:
        out2 = plugin.convert(stream, options, "txtz", _Log(), {})
    assert out2 is not None
    assert b"Line A" in html_input.last_html


def test_txt_output_convert_handles_write_only_stream(monkeypatch) -> None:
    txt_output_mod = importlib.import_module("LiuXin_alpha.file_formats.conversion.plugins.txt_output")

    fake_txtml_mod = types.ModuleType("LiuXin_alpha.file_formats.txt.txtml")

    class _TXTMLizer:
        def __init__(self, _log):
            pass

        def extract_content(self, _oeb, _opts):
            return "Unicode Ω 世界 👩🏽‍💻"

    fake_txtml_mod.TXTMLizer = _TXTMLizer
    monkeypatch.setitem(sys.modules, "LiuXin_alpha.file_formats.txt.txtml", fake_txtml_mod)

    class _WriteOnly:
        def __init__(self):
            self.data = b""

        def write(self, payload: bytes):
            self.data += payload

    sink = _WriteOnly()
    opts = types.SimpleNamespace(
        txt_output_formatting="plain",
        newline="unix",
        txt_output_encoding="utf-8",
        remove_paragraph_spacing=False,
        max_line_length=0,
        force_max_line_length=False,
        inline_toc=False,
    )
    txt_output_mod.TXTOutput(None).convert(object(), sink, None, opts, _Log())
    assert b"Unicode \xce\xa9" in sink.data


def test_txt_input_does_not_leave_root_index_files_on_failure(
    monkeypatch,
    project_root: Path,
) -> None:
    txt_input_mod = importlib.import_module("LiuXin_alpha.file_formats.conversion.plugins.txt_input")

    class _Opt:
        def __init__(self, name, val):
            self.option = types.SimpleNamespace(name=name)
            self.recommended_value = val

    fake_ui = types.ModuleType("LiuXin_alpha.customize.ui")

    def _raise_convert(stream, options, file_ext, log, accelerators):
        raise RuntimeError("forced txt->html failure")

    fake_ui.plugin_for_input_format = lambda fmt: types.SimpleNamespace(
        options=(_Opt("breadth_first", False), _Opt("dont_package", False)),
        convert=_raise_convert,
    )
    fake_ui.get_file_type_metadata = (
        lambda stream, file_ext, calibre=True: types.SimpleNamespace(title="TXT Smoke", authors=["A"])
    )

    fake_meta = types.ModuleType("LiuXin_alpha.file_formats.oeb.transforms.metadata")
    fake_meta.meta_info_to_oeb_metadata = lambda mi, metadata, log: None

    monkeypatch.setitem(sys.modules, "LiuXin_alpha.customize.ui", fake_ui)
    monkeypatch.setitem(sys.modules, "LiuXin_alpha.file_formats.oeb.transforms.metadata", fake_meta)
    monkeypatch.chdir(project_root)

    plugin = txt_input_mod.TXTInput(None)
    options = types.SimpleNamespace(
        input_encoding="utf-8",
        paragraph_type="block",
        formatting_type="plain",
        preserve_spaces=False,
        txt_in_remove_indents=False,
        markdown_extensions="footnotes, tables, toc",
        debug_pipeline=None,
        verbose=0,
        enable_heuristics=False,
        dehyphenate=False,
        flow_size=0,
    )

    before = {p.name for p in project_root.glob("index*.html")}
    with io.BytesIO(b"hello from memory stream") as stream:
        with pytest.raises(RuntimeError, match="forced txt->html failure"):
            plugin.convert(stream, options, "txt", _Log(), {})
    after = {p.name for p in project_root.glob("index*.html")}

    assert after == before
