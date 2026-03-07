from __future__ import annotations

import importlib
import io
import sys
import types
from pathlib import Path

from lxml import etree


class _Log:
    def __init__(self) -> None:
        self.messages: list[tuple[str, str]] = []

    def __call__(self, *parts) -> None:
        self.messages.append(("call", " ".join(str(x) for x in parts)))

    def debug(self, *parts) -> None:
        self.messages.append(("debug", " ".join(str(x) for x in parts)))

    def info(self, *parts) -> None:
        self.messages.append(("info", " ".join(str(x) for x in parts)))

    def warn(self, *parts) -> None:
        self.messages.append(("warn", " ".join(str(x) for x in parts)))

    def warning(self, *parts) -> None:
        self.messages.append(("warning", " ".join(str(x) for x in parts)))

    def error(self, *parts) -> None:
        self.messages.append(("error", " ".join(str(x) for x in parts)))

    def exception(self, *parts) -> None:
        self.messages.append(("exception", " ".join(str(x) for x in parts)))


def test_rtf_modules_import_smoke() -> None:
    modules = (
        "LiuXin_alpha.file_formats.rtf",
        "LiuXin_alpha.file_formats.rtf.input",
        "LiuXin_alpha.file_formats.rtf.preprocess",
        "LiuXin_alpha.file_formats.rtf.rtfml",
        "LiuXin_alpha.file_formats.rtf2xml.ParseRtf",
        "LiuXin_alpha.file_formats.conversion.plugins.rtf_input",
        "LiuXin_alpha.file_formats.conversion.plugins.rtf_output",
    )
    for module_name in modules:
        importlib.import_module(module_name)


def test_txt2rtf_escapes_control_chars_and_unicode() -> None:
    mod = importlib.import_module("LiuXin_alpha.file_formats.rtf.rtfml")
    out = mod.txt2rtf(r"{\} café Ω 😀")
    assert r"\'7b" in out and r"\'7d" in out and r"\'5c" in out
    assert r"\u233?" in out  # é
    assert r"\u937?" in out  # Ω


def test_rtfmlizer_image_to_hexstring_handles_binary_bytes(monkeypatch) -> None:
    mod = importlib.import_module("LiuXin_alpha.file_formats.rtf.rtfml")
    monkeypatch.setattr(mod, "_convert_image_to_jpeg_bytes", lambda data: b"\x01\xAB\xFE\x10")
    monkeypatch.setattr(mod, "_identify_data", lambda data: (320, 240, "jpeg"))

    out, width, height = mod.RTFMLizer(_Log()).image_to_hexstring(b"raw")
    assert width == 320 and height == 240
    assert out == "01abfe10"


def test_rtf_output_convert_writes_binary_stream(tmp_path: Path, monkeypatch) -> None:
    mod = importlib.import_module("LiuXin_alpha.file_formats.conversion.plugins.rtf_output")
    fake_rtfml_mod = types.ModuleType("LiuXin_alpha.file_formats.rtf.rtfml")

    class _RTFMLizer:
        def __init__(self, _log):
            pass

        def extract_content(self, _oeb, _opts):
            return "{\\rtf1\\ansi hello}"

    fake_rtfml_mod.RTFMLizer = _RTFMLizer
    monkeypatch.setitem(sys.modules, "LiuXin_alpha.file_formats.rtf.rtfml", fake_rtfml_mod)

    out_path = tmp_path / "out.rtf"
    plugin = mod.RTFOutput(None)
    plugin.convert(types.SimpleNamespace(), str(out_path), None, types.SimpleNamespace(), _Log())

    raw = out_path.read_bytes()
    assert raw.startswith(b"{\\rtf1")
    assert b"hello" in raw


def test_rtf_preprocess_unicode_partial_data_fix() -> None:
    mod = importlib.import_module("LiuXin_alpha.file_formats.rtf.preprocess")
    tokens = [
        mod.tokenControlWordWithNumericArgument("\\u", 945, " "),  # alpha
        mod.tokenData("xyz"),  # replacement chars for uc handling
        mod.tokenData("tail"),
    ]
    parser = mod.RtfTokenParser(tokens)
    out = parser.toRTF()
    assert "\\u945" in out
    assert "tail" in out


def test_rtf_preprocess_tokenizer_accepts_bytes_input() -> None:
    mod = importlib.import_module("LiuXin_alpha.file_formats.rtf.preprocess")
    tok = mod.RtfTokenizer(b"{\\rtf1 test}")
    assert tok.tokens
    rebuilt = tok.toRTF()
    assert isinstance(rebuilt, str)
    assert "\\rtf1" in rebuilt


def test_rtf_input_convert_borders_assigns_classes() -> None:
    mod = importlib.import_module("LiuXin_alpha.file_formats.conversion.plugins.rtf_input")
    plugin = mod.RTFInput(None)
    root = etree.fromstring(
        b"""
<doc>
  <cell border-cell-top-style="double-border" border-cell-top-line-width="2" border-cell-top-color="#123456" />
  <cell border-cell-top-style="double-border" border-cell-top-line-width="2" border-cell-top-color="#123456" />
</doc>
"""
    )
    style_map = plugin.convert_borders(root)
    assert style_map
    classes = [c.get("class") for c in root.xpath("//*[local-name()='cell']")]
    assert classes[0] == classes[1]


def test_rtf_input_postprocess_book_removes_remove_me_images() -> None:
    mod = importlib.import_module("LiuXin_alpha.file_formats.conversion.plugins.rtf_input")
    plugin = mod.RTFInput(None)

    data = etree.fromstring(
        b"""
<html xmlns="http://www.w3.org/1999/xhtml">
  <body><p>Hello<img src="__REMOVE_ME__"/>Tail</p></body>
</html>
"""
    )
    item = types.SimpleNamespace(data=data)
    oeb = types.SimpleNamespace(spine=[item])

    plugin.postprocess_book(oeb, types.SimpleNamespace(), _Log())
    imgs = data.xpath('//*[local-name()="img"]')
    assert imgs == []
    assert "Tail" in "".join(data.xpath("string()"))


def test_rtf_input_convert_glue_smoke(tmp_path: Path, monkeypatch) -> None:
    mod = importlib.import_module("LiuXin_alpha.file_formats.conversion.plugins.rtf_input")

    fake_parse_rtf = types.ModuleType("LiuXin_alpha.file_formats.rtf2xml.ParseRtf")

    class _RtfInvalidCodeException(Exception):
        pass

    fake_parse_rtf.RtfInvalidCodeException = _RtfInvalidCodeException
    monkeypatch.setitem(sys.modules, "LiuXin_alpha.file_formats.rtf2xml.ParseRtf", fake_parse_rtf)

    # Fake metadata module import used inside convert()
    fake_meta = types.ModuleType("LiuXin_alpha.metadata.file_sources")
    fake_meta.get_metadata = lambda stream, ext=None: types.SimpleNamespace(title="T", authors=["A"])
    monkeypatch.setitem(sys.modules, "LiuXin_alpha.metadata.file_sources", fake_meta)

    # Fake OPF creator import used inside convert()
    fake_opf2 = types.ModuleType("LiuXin_alpha.file_formats.opf.opf2")

    class _OPFCreator:
        def __init__(self, _cwd, _mi):
            self.manifest = []
            self.spine = []

        def create_manifest(self, manifest):
            self.manifest = list(manifest)

        def create_spine(self, spine):
            self.spine = list(spine)

        def render(self, f):
            f.write(b"<package/>")

    fake_opf2.OPFCreator = _OPFCreator
    monkeypatch.setitem(sys.modules, "LiuXin_alpha.file_formats.opf.opf2", fake_opf2)

    class _FakeTransform:
        def __call__(self, _doc):
            return object()

        def tostring(self, _result):
            return "<html><body><p>RTF converted</p></body></html>"

    monkeypatch.setattr(etree, "XSLT", lambda *a, **k: _FakeTransform())
    monkeypatch.setattr(mod, "P", lambda *a, **k: b"<xsl:stylesheet xmlns:xsl='http://www.w3.org/1999/XSL/Transform' version='1.0'/>")

    plugin = mod.RTFInput(None)
    plugin.generate_xml = lambda _path: (
        b"<root xmlns:rtf='http://rtf2xml.sourceforge.net/'><rtf:pict num='1'/><cell/></root>"
    )
    plugin.extract_images = lambda _p: {1: "0001.png"}

    in_stream = io.BytesIO(b"{\\rtf1 sample}")
    in_stream.name = str(tmp_path / "in.rtf")
    Path(in_stream.name).write_bytes(b"{\\rtf1 sample}")
    monkeypatch.chdir(tmp_path)

    out = plugin.convert(in_stream, types.SimpleNamespace(input_encoding=None, debug_pipeline=None), "rtf", _Log(), {})

    assert out.endswith("metadata.opf")
    assert (tmp_path / "metadata.opf").exists()
    assert (tmp_path / "index.xhtml").exists()
    assert b"RTF converted" in (tmp_path / "index.xhtml").read_bytes()
