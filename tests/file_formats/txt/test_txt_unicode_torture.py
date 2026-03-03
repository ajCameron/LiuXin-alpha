from __future__ import annotations

import importlib
import io
import random
import sys
import types
from pathlib import Path


UNICODE_TORTURE = (
    "Latin café naïve coöperate façade déjà vu\n"
    "Greek Καλημέρα κόσμε\n"
    "Cyrillic Здравствуйте, мир\n"
    "Arabic مرحبا بالعالم\n"
    "Hebrew שלום עולם\n"
    "Hindi नमस्ते दुनिया\n"
    "Thai สวัสดีโลก\n"
    "CJK 你好，世界 / こんにちは世界 / 안녕하세요 세계\n"
    "Emoji 👩🏽‍💻🧪📚🧬\n"
    "Combining cafe\u0301 co\u0308operate A\u030A\n"
    "Bidi \u200fمرحبا\u200f and ZWJ A\u200dB\n"
)


def test_convert_basic_unicode_torture_deterministic() -> None:
    mod = importlib.import_module("LiuXin_alpha.file_formats.txt.processor")
    a = mod.convert_basic(UNICODE_TORTURE, title="Ω 世界", epub_split_size_kb=8)
    b = mod.convert_basic(UNICODE_TORTURE, title="Ω 世界", epub_split_size_kb=8)
    assert a == b
    assert "<title>Ω 世界 " in a
    assert "👩🏽‍💻🧪📚🧬" in a
    assert "cafe\u0301" in a


def test_convert_markdown_and_textile_unicode_paths() -> None:
    mod = importlib.import_module("LiuXin_alpha.file_formats.txt.processor")
    md = mod.convert_markdown("# 多言語\n\n- Ω\n- 世界\n\n`👩🏽‍💻`", title="MD")
    tx = mod.convert_textile("h1. 多言語\n\n\"参照\":https://example.com/路径", title="TX")
    assert "<title>MD " in md and "多言語</h1>" in md
    assert "<title>TX " in tx and '<a href="https://example.com/路径">参照</a>' in tx


def test_txt_input_broken_encoding_falls_back_to_replacement(tmp_path: Path, monkeypatch) -> None:
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
        lambda stream, file_ext, calibre=True: types.SimpleNamespace(title="Broken", authors=["A"])
    )
    fake_meta = types.ModuleType("LiuXin_alpha.file_formats.oeb.transforms.metadata")
    fake_meta.meta_info_to_oeb_metadata = lambda mi, metadata, log: None
    monkeypatch.setitem(sys.modules, "LiuXin_alpha.customize.ui", fake_ui)
    monkeypatch.setitem(sys.modules, "LiuXin_alpha.file_formats.oeb.transforms.metadata", fake_meta)

    payload = UNICODE_TORTURE.encode("utf-8") + bytes([0x81, 0x8D, 0x90, 0xFF])
    source = tmp_path / "broken.txt"
    source.write_bytes(payload)

    plugin = txt_input_mod.TXTInput(None)
    opts = types.SimpleNamespace(
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

    with source.open("rb") as stream:
        plugin.convert(stream, opts, "txt", types.SimpleNamespace(debug=lambda *a, **k: None, info=lambda *a, **k: None), {})

    decoded_html = html_input.last_html.decode("utf-8", "replace")
    assert "Latin café" in decoded_html
    assert "\ufffd" in decoded_html


def test_detect_formatting_type_unicode_fuzz_stable() -> None:
    mod = importlib.import_module("LiuXin_alpha.file_formats.txt.processor")
    rng = random.Random(20260303)
    alphabet = "abcXYZΩЖשלוםمرحباनमस्ते世界👩🏽‍💻_*#[]()!\":./-\n "
    fuzz = "".join(rng.choice(alphabet) for _ in range(3000))
    a = mod.detect_formatting_type(fuzz)
    b = mod.detect_formatting_type(fuzz)
    assert a == b
    assert a in {"markdown", "textile", "heuristic"}


def test_txt_output_roundtrip_bytes_with_unicode_writer(monkeypatch) -> None:
    txt_output_mod = importlib.import_module("LiuXin_alpha.file_formats.conversion.plugins.txt_output")

    fake_txtml_mod = types.ModuleType("LiuXin_alpha.file_formats.txt.txtml")

    class _TXTMLizer:
        def __init__(self, _log):
            pass

        def extract_content(self, _oeb, _opts):
            return UNICODE_TORTURE

    fake_txtml_mod.TXTMLizer = _TXTMLizer
    monkeypatch.setitem(sys.modules, "LiuXin_alpha.file_formats.txt.txtml", fake_txtml_mod)

    out = io.BytesIO()
    opts = types.SimpleNamespace(
        txt_output_formatting="plain",
        newline="unix",
        txt_output_encoding="utf-8",
        remove_paragraph_spacing=False,
        max_line_length=0,
        force_max_line_length=False,
        inline_toc=False,
    )
    log = types.SimpleNamespace(debug=lambda *a, **k: None, info=lambda *a, **k: None)
    txt_output_mod.TXTOutput(None).convert(object(), out, None, opts, log)
    out.seek(0)
    data = out.read().decode("utf-8", "replace")
    assert "Greek Καλημέρα κόσμε" in data
    assert "Emoji 👩🏽‍💻🧪📚🧬" in data
