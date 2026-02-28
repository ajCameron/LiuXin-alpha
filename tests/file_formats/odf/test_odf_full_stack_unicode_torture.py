from __future__ import annotations

import unicodedata
import zipfile
from pathlib import Path

import pytest


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
    "Math symbols: ∑ ∫ √ π ≈ ≤ ≥.",
    "Supplementary planes: 𐍈 𑀓 𞸀.",
]


def _build_unicode_doc(lines: list[str]):
    from LiuXin_alpha.file_formats.odf.opendocument import OpenDocumentText
    from LiuXin_alpha.file_formats.odf.teletype import addTextToElement
    from LiuXin_alpha.file_formats.odf.text import P

    doc = OpenDocumentText()
    for line in lines:
        para = P()
        addTextToElement(para, line)
        doc.text.addElement(para)
    return doc


def _rewrite_content_xml_with_invalid_utf8(src: Path, dst: Path) -> None:
    with zipfile.ZipFile(src, "r") as zin, zipfile.ZipFile(dst, "w") as zout:
        for info in zin.infolist():
            data = zin.read(info.filename)
            if info.filename == "content.xml":
                # Inject invalid UTF-8 near the end of text content.
                data = data.replace(b"</office:text>", b"\xff\xfe</office:text>", 1)
            zout.writestr(info, data)


def test_odf_full_stack_unicode_torture_roundtrip_and_xhtml(tmp_path: Path) -> None:
    from LiuXin_alpha.file_formats.odf.odf2xhtml import ODF2XHTML
    from LiuXin_alpha.file_formats.odf.opendocument import load
    from LiuXin_alpha.file_formats.odf.teletype import extractText
    from LiuXin_alpha.file_formats.odf.text import P

    path = tmp_path / "ユニコード_📚.odt"
    _build_unicode_doc(UNICODE_TORTURE_LINES).save(path)

    loaded = load(path)
    text_paragraphs = [extractText(p) for p in loaded.getElementsByType(P)]
    for line in UNICODE_TORTURE_LINES:
        assert line in text_paragraphs

    html = ODF2XHTML(generate_css=True).odf2xhtml(loaded)
    html_nfc = unicodedata.normalize("NFC", html)
    probes = [
        "naïve",
        "Καλημέρα",
        "Здравствуйте",
        "مرحبا",
        "שלום",
        "नमस्ते",
        "こんにちは",
        "🙂",
        "∑",
    ]
    hits = sum(1 for probe in probes if unicodedata.normalize("NFC", probe) in html_nfc)
    assert hits >= 7


def test_odf_full_stack_load_supports_stream_and_pathlike(tmp_path: Path) -> None:
    from LiuXin_alpha.file_formats.odf.odf2xhtml import ODF2XHTML
    from LiuXin_alpha.file_formats.odf.opendocument import load
    from LiuXin_alpha.file_formats.odf.teletype import extractText
    from LiuXin_alpha.file_formats.odf.text import P

    src = "Pathlike + stream smoke: こんにちは\tمرحبا\nline 2"
    path = tmp_path / "stream_pathlike.odt"
    _build_unicode_doc([src]).save(path)

    with path.open("rb") as stream:
        loaded_from_stream = load(stream)
    paragraphs = [extractText(p) for p in loaded_from_stream.getElementsByType(P)]
    assert src in paragraphs

    html = ODF2XHTML(generate_css=False).odf2xhtml(path)
    assert "<html" in html
    assert "こんにちは" in html


def test_odf_load_invalid_utf8_content_xml_raises(tmp_path: Path) -> None:
    from LiuXin_alpha.file_formats.odf.opendocument import load

    good = tmp_path / "good.odt"
    bad = tmp_path / "bad_invalid_utf8.odt"
    _build_unicode_doc(["safe"]).save(good)
    _rewrite_content_xml_with_invalid_utf8(good, bad)

    with pytest.raises(Exception):
        load(bad)
