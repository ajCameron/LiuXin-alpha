from __future__ import annotations

import unicodedata
from pathlib import Path
from types import SimpleNamespace
from xml.etree import ElementTree as ET


UNICODE_TORTURE_LINES = [
    "Latin accents: naïve coöperate façade déjà vu.",
    "Greek: Καλημέρα κόσμε.",
    "Cyrillic: Здравствуйте, мир.",
    "Arabic RTL: مرحبا بالعالم.",
    "Hebrew RTL: שלום עולם.",
    "Devanagari: नमस्ते दुनिया।",
    "CJK: 你好，世界。こんにちは世界。안녕하세요 세계.",
    "Combining marks: cafe\u0301 co\u0308perate A\u030A.",
    "Emoji and ZWJ: 👩🏽\u200d🔬 👨\u200d👩\u200d👧\u200d👦 🏳️\u200d🌈 🙂.",
    "Directionality: \u202bRTL block\u202c and \u200fmarks\u200f.",
]


class _Log:
    def __call__(self, *args, **kwargs):
        return None

    def debug(self, *args, **kwargs):
        return None

    def warning(self, *args, **kwargs):
        return None

    warn = warning

    def exception(self, *args, **kwargs):
        return None


def _build_odt(path: Path) -> None:
    from LiuXin_alpha.file_formats.odf.opendocument import OpenDocumentText
    from LiuXin_alpha.file_formats.odf.teletype import addTextToElement
    from LiuXin_alpha.file_formats.odf.text import P

    doc = OpenDocumentText()
    for line in UNICODE_TORTURE_LINES:
        p = P()
        addTextToElement(p, line)
        doc.text.addElement(p)
    doc.save(path)


def test_odt_modules_import_smoke() -> None:
    import importlib

    importlib.import_module("LiuXin_alpha.file_formats.odt.input")
    importlib.import_module("LiuXin_alpha.file_formats.conversion.plugins.odt_input")


def test_odt_extract_full_stack_unicode_torture(tmp_path: Path) -> None:
    from LiuXin_alpha.file_formats.odt.input import Extract

    src = tmp_path / "unicode_torture_📚.odt"
    _build_odt(src)
    out_dir = tmp_path / "extract_out"

    with src.open("rb") as stream:
        opf_path = Path(Extract()(stream, str(out_dir), _Log()))

    assert opf_path.exists()
    assert opf_path.name == "metadata.opf"
    assert opf_path.parent == out_dir
    assert (out_dir / "index.xhtml").exists()

    root = ET.parse(opf_path).getroot()
    assert root.tag.endswith("package")

    html = unicodedata.normalize("NFC", (out_dir / "index.xhtml").read_text(encoding="utf-8", errors="replace"))
    probes = ["naïve", "Καλημέρα", "Здравствуйте", "مرحبا", "שלום", "नमस्ते", "こんにちは", "🙂"]
    hits = sum(1 for p in probes if unicodedata.normalize("NFC", p) in html)
    assert hits >= 6


def test_odt_plugin_convert_smoke_unicode(tmp_path: Path, monkeypatch) -> None:
    from LiuXin_alpha.file_formats.conversion.plugins.odt_input import ODTInput

    src = tmp_path / "plugin_unicode.odt"
    _build_odt(src)

    workdir = tmp_path / "plugin_work"
    workdir.mkdir()
    monkeypatch.chdir(workdir)

    with src.open("rb") as stream:
        out = ODTInput(None).convert(stream, SimpleNamespace(), "odt", _Log(), {})

    opf_path = Path(out)
    assert opf_path.is_absolute()
    assert opf_path.exists()
    assert opf_path.name == "metadata.opf"
    assert (opf_path.parent / "index.xhtml").exists()

    html = (opf_path.parent / "index.xhtml").read_text(encoding="utf-8", errors="replace")
    assert "こんにちは" in html
