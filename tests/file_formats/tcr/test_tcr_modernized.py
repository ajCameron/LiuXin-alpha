from __future__ import annotations

import importlib
import io
import sys
import types

import pytest


class _Log:
    def info(self, *_args, **_kwargs):
        return None

    def debug(self, *_args, **_kwargs):
        return None

    def warn(self, *_args, **_kwargs):
        return None

    def warning(self, *_args, **_kwargs):
        return None

    def error(self, *_args, **_kwargs):
        return None


def test_tcr_modules_import_smoke() -> None:
    modules = (
        "LiuXin_alpha.file_formats.tcr",
        "LiuXin_alpha.file_formats.compression.tcr",
        "LiuXin_alpha.file_formats.conversion.plugins.tcr_input",
        "LiuXin_alpha.file_formats.conversion.plugins.tcr_output",
    )
    for module_name in modules:
        importlib.import_module(module_name)


def test_tcr_roundtrip_unicode_torture_and_deterministic_encoding() -> None:
    mod = importlib.import_module("LiuXin_alpha.file_formats.compression.tcr")
    unicode_payload = (
        "Latin: café naïve\n"
        "Greek: Καλημέρα κόσμε\n"
        "Cyrillic: Здравствуйте\n"
        "Arabic: مرحبا بالعالم\n"
        "Hebrew: שלום עולם\n"
        "Hindi: नमस्ते दुनिया\n"
        "CJK: 你好，世界\n"
        "Emoji: 👩🏽‍💻🧪📚\n"
        "Combining: cafe\u0301 co\u0308operate A\u030A\n"
    ).encode("utf-8")
    raw = unicode_payload + bytes([0x00, 0x81, 0x90, 0xFF]) + unicode_payload

    encoded_a = mod.compress(raw)
    encoded_b = mod.compress(raw)

    assert encoded_a == encoded_b
    assert mod.decompress(io.BytesIO(encoded_a)) == raw


def test_tcr_decompress_reports_truncated_dictionary() -> None:
    mod = importlib.import_module("LiuXin_alpha.file_formats.compression.tcr")
    with pytest.raises(ValueError, match="truncated"):
        mod.decompress(io.BytesIO(b"!!8-Bit!!"))
    with pytest.raises(ValueError, match="truncated"):
        mod.decompress(io.BytesIO(b"!!8-Bit!!" + bytes([2]) + b"a"))


def test_tcr_input_end_to_end_with_unicode_and_broken_bytes(monkeypatch: pytest.MonkeyPatch) -> None:
    tcr_mod = importlib.import_module("LiuXin_alpha.file_formats.compression.tcr")
    tcr_input_mod = importlib.import_module("LiuXin_alpha.file_formats.conversion.plugins.tcr_input")

    class _Opt:
        option = types.SimpleNamespace(name="max_line_length")
        recommended_value = 80

    class _TxtPlugin:
        options = (_Opt(),)

        def __init__(self):
            self.seen_text = None

        def convert(self, stream, options, file_ext, log, accelerators):
            self.seen_text = stream.read()
            return "converted.oeb"

    fake_txt = _TxtPlugin()
    fake_ui = types.ModuleType("LiuXin_alpha.customize.ui")
    fake_ui.plugin_for_input_format = lambda fmt: fake_txt
    monkeypatch.setitem(sys.modules, "LiuXin_alpha.customize.ui", fake_ui)

    raw = "Hello Ω 世界".encode("utf-8") + b"\x81\x90\xff"
    tcr_stream = io.BytesIO(tcr_mod.compress(raw))

    plugin = tcr_input_mod.TCRInput(None)
    options = types.SimpleNamespace(input_encoding="utf-8")
    result = plugin.convert(tcr_stream, options, "tcr", _Log(), {})

    assert result == "converted.oeb"
    assert "Hello Ω 世界" in fake_txt.seen_text
    assert "\ufffd" in fake_txt.seen_text
    assert options.max_line_length == 80


def test_tcr_output_roundtrip_uses_default_utf8_encoding(monkeypatch: pytest.MonkeyPatch) -> None:
    tcr_mod = importlib.import_module("LiuXin_alpha.file_formats.compression.tcr")
    tcr_output_mod = importlib.import_module("LiuXin_alpha.file_formats.conversion.plugins.tcr_output")

    fake_txtml_mod = types.ModuleType("LiuXin_alpha.file_formats.txt.txtml")

    class _TXTMLizer:
        def __init__(self, _log):
            pass

        def extract_content(self, _oeb, _opts):
            return "Unicode Ω 東京 👩🏽‍💻"

    fake_txtml_mod.TXTMLizer = _TXTMLizer
    monkeypatch.setitem(sys.modules, "LiuXin_alpha.file_formats.txt.txtml", fake_txtml_mod)

    out_stream = io.BytesIO()
    opts = types.SimpleNamespace()
    tcr_output_mod.TCROutput(None).convert(object(), out_stream, None, opts, _Log())

    out_stream.seek(0)
    decoded = tcr_mod.decompress(out_stream).decode("utf-8", "replace")
    assert "Unicode Ω 東京 👩🏽‍💻" in decoded


def test_tcr_output_accepts_write_only_stream(monkeypatch: pytest.MonkeyPatch) -> None:
    tcr_mod = importlib.import_module("LiuXin_alpha.file_formats.compression.tcr")
    tcr_output_mod = importlib.import_module("LiuXin_alpha.file_formats.conversion.plugins.tcr_output")

    fake_txtml_mod = types.ModuleType("LiuXin_alpha.file_formats.txt.txtml")

    class _TXTMLizer:
        def __init__(self, _log):
            pass

        def extract_content(self, _oeb, _opts):
            return b"binary payload"

    fake_txtml_mod.TXTMLizer = _TXTMLizer
    monkeypatch.setitem(sys.modules, "LiuXin_alpha.file_formats.txt.txtml", fake_txtml_mod)

    class _WriteOnly:
        def __init__(self):
            self.data = b""

        def write(self, payload: bytes):
            self.data += payload

    sink = _WriteOnly()
    opts = types.SimpleNamespace(tcr_output_encoding="cp1252")
    tcr_output_mod.TCROutput(None).convert(object(), sink, None, opts, _Log())

    roundtrip = tcr_mod.decompress(io.BytesIO(sink.data))
    assert roundtrip == b"binary payload"
